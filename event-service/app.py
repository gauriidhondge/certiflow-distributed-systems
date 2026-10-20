"""
CertiFlow - Event Service
-------------------------
Accepts certificate batches from organisers, stores event metadata in Redis
and pushes one job per participant onto a shared work queue. Worker pods
consume that queue in parallel.
"""
import json
import os
import socket
import time
import uuid

import redis
from flask import Flask, jsonify, request

app = Flask(__name__)

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    decode_responses=True,
)

QUEUE = "certiflow:jobs"
PROCESSING = "certiflow:processing"
POD = socket.gethostname()
MAX_PARTICIPANTS = int(os.getenv("MAX_PARTICIPANTS", "5000"))
CERT_FIELDS = ["cert_id", "name", "email", "role", "status", "worker", "generated_at", "error"]


@app.get("/health")
def health():
    try:
        r.ping()
        return jsonify(service="event-service", pod=POD, redis="up"), 200
    except redis.RedisError:
        return jsonify(service="event-service", pod=POD, redis="down"), 503


@app.post("/events")
def create_event():
    data = request.get_json(silent=True) or {}
    name = str(data.get("name", "")).strip()
    organizer = str(data.get("organizer", "")).strip()
    date = str(data.get("date", "")).strip()
    participants = data.get("participants") or []

    if not name or not organizer or not date:
        return jsonify(error="name, organizer and date are required"), 400
    if not isinstance(participants, list) or not participants:
        return jsonify(error="participants must be a non-empty list"), 400
    if len(participants) > MAX_PARTICIPANTS:
        return jsonify(error=f"max {MAX_PARTICIPANTS} participants per batch"), 400

    cleaned = []
    for p in participants:
        pname = str((p or {}).get("name", "")).strip()
        if not pname:
            continue
        cleaned.append({
            "name": pname[:80],
            "email": str(p.get("email", "")).strip()[:120],
            "role": str(p.get("role", "Participant")).strip()[:40] or "Participant",
        })
    if not cleaned:
        return jsonify(error="no valid participant names found"), 400

    event_id = "EV" + uuid.uuid4().hex[:6].upper()
    now = time.time()
    pipe = r.pipeline()
    pipe.hset(f"event:{event_id}", mapping={
        "event_id": event_id,
        "name": name[:120],
        "organizer": organizer[:120],
        "date": date[:40],
        "total": len(cleaned),
        "done": 0,
        "failed": 0,
        "created_at": now,
        "accepted_by": POD,
    })
    pipe.zadd("events", {event_id: now})

    for p in cleaned:
        cert_id = f"CF-{event_id}-{uuid.uuid4().hex[:8].upper()}"
        pipe.hset(f"cert:{cert_id}", mapping={
            "cert_id": cert_id,
            "event_id": event_id,
            "event_name": name[:120],
            "organizer": organizer[:120],
            "date": date[:40],
            "name": p["name"],
            "email": p["email"],
            "role": p["role"],
            "status": "queued",
        })
        pipe.rpush(f"event:{event_id}:certs", cert_id)
        # LPUSH here + worker pops from the RIGHT => FIFO order
        pipe.lpush(QUEUE, json.dumps({"cert_id": cert_id, "attempt": 1}))
    pipe.execute()

    return jsonify(event_id=event_id, queued=len(cleaned), accepted_by=POD), 201


@app.get("/events")
def list_events():
    ids = r.zrevrange("events", 0, 49)
    pipe = r.pipeline()
    for eid in ids:
        pipe.hgetall(f"event:{eid}")
    events = [e for e in pipe.execute() if e]
    return jsonify(events=events, served_by=POD)


@app.get("/events/<event_id>")
def get_event(event_id):
    event = r.hgetall(f"event:{event_id}")
    if not event:
        return jsonify(error="event not found"), 404
    cert_ids = r.lrange(f"event:{event_id}:certs", 0, -1)
    pipe = r.pipeline()
    for cid in cert_ids:
        pipe.hmget(f"cert:{cid}", CERT_FIELDS)
    certs = [dict(zip(CERT_FIELDS, row)) for row in pipe.execute()]
    return jsonify(event=event, certificates=certs, served_by=POD)


@app.get("/stats")
def stats():
    now = time.time()
    alive = r.zrangebyscore("workers:heartbeat", now - 15, "+inf", withscores=True)
    counts = r.hgetall("workers:processed")
    workers = [
        {"pod": pod, "processed": int(counts.get(pod, 0)), "last_seen": round(now - ts, 1)}
        for pod, ts in alive
    ]
    workers.sort(key=lambda w: w["pod"])
    return jsonify(
        queue_length=r.llen(QUEUE),
        in_progress=r.llen(PROCESSING),
        total_generated=int(r.get("stats:generated") or 0),
        workers=workers,
        served_by=POD,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5001")))

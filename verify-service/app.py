"""
CertiFlow - Verification Service
--------------------------------
Public, read-heavy service. Anyone (recruiter, college office) can check a
certificate ID. The HMAC signature is recomputed with the shared secret, so
any edit to the stored name/event makes the certificate show as TAMPERED.
"""
import base64
import hashlib
import hmac
import io
import os
import socket

import redis
from flask import Flask, jsonify, send_file

app = Flask(__name__)

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    decode_responses=True,
)
POD = socket.gethostname()
SECRET = os.getenv("SIGNING_SECRET", "change-me").encode()


def expected_signature(cert):
    payload = f"{cert['cert_id']}|{cert['name']}|{cert['event_name']}|{cert['date']}"
    return hmac.new(SECRET, payload.encode(), hashlib.sha256).hexdigest()[:24]


@app.get("/health")
def health():
    try:
        r.ping()
        return jsonify(service="verify-service", pod=POD, redis="up"), 200
    except redis.RedisError:
        return jsonify(service="verify-service", pod=POD, redis="down"), 503


@app.get("/verify/<cert_id>")
def verify(cert_id):
    cert_id = cert_id.strip().upper()
    cert = r.hgetall(f"cert:{cert_id}")
    if not cert:
        return jsonify(valid=False, reason="No certificate with this ID exists", verified_by=POD), 404

    if cert.get("status") != "generated":
        return jsonify(valid=False, reason=f"Certificate is still {cert.get('status')}",
                       status=cert.get("status"), verified_by=POD), 200

    ok = hmac.compare_digest(cert.get("signature", ""), expected_signature(cert))
    details = {k: cert.get(k) for k in
               ("cert_id", "name", "role", "event_name", "organizer", "date", "generated_at", "worker")}
    if not ok:
        return jsonify(valid=False, reason="Signature mismatch - record has been TAMPERED",
                       certificate=details, verified_by=POD), 200
    return jsonify(valid=True, certificate=details, verified_by=POD), 200


@app.get("/certificate/<cert_id>/pdf")
def download(cert_id):
    cert_id = cert_id.strip().upper()
    pdf_b64 = r.hget(f"cert:{cert_id}", "pdf")
    if not pdf_b64:
        return jsonify(error="PDF not available (not generated yet or unknown ID)"), 404
    return send_file(io.BytesIO(base64.b64decode(pdf_b64)), mimetype="application/pdf",
                     as_attachment=False, download_name=f"{cert_id}.pdf")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5002")))

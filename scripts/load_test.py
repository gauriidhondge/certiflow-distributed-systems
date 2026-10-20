"""
CertiFlow load test - submits a large batch and measures throughput.
Uses only the Python standard library (no pip install needed).

Usage:
  python scripts/load_test.py --url http://localhost:30080 --count 300
  python scripts/load_test.py --url http://localhost:8080  --count 300   (docker compose)
"""
import argparse
import json
import time
import urllib.request


def call(url, data=None):
    req = urllib.request.Request(url, method="POST" if data else "GET",
                                 data=json.dumps(data).encode() if data else None,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:30080")
    ap.add_argument("--count", type=int, default=300)
    a = ap.parse_args()
    base = a.url.rstrip("/")

    participants = [{"name": f"Student {i:04d}", "email": f"s{i}@example.com", "role": "Participant"}
                    for i in range(a.count)]
    t0 = time.time()
    ev = call(f"{base}/api/events", {"name": f"Load Test {time.strftime('%H:%M:%S')}",
                                     "organizer": "DS Mini Project", "date": time.strftime("%d %b %Y"),
                                     "participants": participants})
    eid = ev["event_id"]
    print(f"Submitted {a.count} certificates as event {eid} (accepted by {ev['accepted_by']})")

    while True:
        d = call(f"{base}/api/events/{eid}")["event"]
        s = call(f"{base}/api/stats")
        done, failed, total = int(d["done"]), int(d["failed"]), int(d["total"])
        el = time.time() - t0
        print(f"[{el:6.1f}s] {done:4d}/{total} done | queue={s['queue_length']:4d} "
              f"| workers={len(s['workers'])} | {done / el:5.1f} certs/s")
        if done + failed >= total:
            break
        time.sleep(2)

    el = time.time() - t0
    print(f"\nFinished {total} certificates in {el:.1f}s  ->  {total / el:.1f} certificates/second")
    print("Per-pod distribution:")
    for w in s["workers"]:
        print(f"  {w['pod']:40s} {w['processed']}")


if __name__ == "__main__":
    main()

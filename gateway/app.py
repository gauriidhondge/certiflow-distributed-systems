"""
CertiFlow - API Gateway
-----------------------
Single entry point for users. Serves the web UI and forwards REST calls to
the internal services over HTTP using service-discovery DNS names
(event-service, verify-service) provided by Docker Compose / Kubernetes.
"""
import os
import socket

import requests
from flask import Flask, Response, jsonify, request, send_from_directory

app = Flask(__name__, static_folder="static")

EVENT_URL = os.getenv("EVENT_SERVICE_URL", "http://event-service:5001")
VERIFY_URL = os.getenv("VERIFY_SERVICE_URL", "http://verify-service:5002")
POD = socket.gethostname()


def forward(base, path):
    try:
        resp = requests.request(
            request.method,
            f"{base}{path}",
            json=request.get_json(silent=True) if request.method == "POST" else None,
            timeout=15,
        )
    except requests.RequestException as e:
        return jsonify(error=f"upstream unavailable: {base}", detail=str(e), gateway=POD), 503
    out = Response(resp.content, status=resp.status_code,
                   content_type=resp.headers.get("Content-Type", "application/json"))
    out.headers["X-Gateway-Pod"] = POD
    return out


@app.get("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/events", methods=["GET", "POST"])
def events():
    return forward(EVENT_URL, "/events")


@app.get("/api/events/<event_id>")
def event_detail(event_id):
    return forward(EVENT_URL, f"/events/{event_id}")


@app.get("/api/stats")
def stats():
    return forward(EVENT_URL, "/stats")


@app.get("/api/verify/<cert_id>")
def verify(cert_id):
    return forward(VERIFY_URL, f"/verify/{cert_id}")


@app.get("/api/certificate/<cert_id>/pdf")
def pdf(cert_id):
    return forward(VERIFY_URL, f"/certificate/{cert_id}/pdf")


@app.get("/health")
def health():
    """Aggregated health: shows inter-service connectivity at a glance."""
    result = {"gateway": {"pod": POD, "status": "up"}}
    healthy = True
    for name, base in (("event-service", EVENT_URL), ("verify-service", VERIFY_URL)):
        try:
            resp = requests.get(f"{base}/health", timeout=3)
            result[name] = resp.json()
            healthy &= resp.ok
        except requests.RequestException as e:
            result[name] = {"status": "down", "error": str(e)}
            healthy = False
    return jsonify(result), (200 if healthy else 503)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))

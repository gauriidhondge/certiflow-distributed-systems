# CertiFlow — Distributed Bulk Certificate Generation & Tamper‑Proof Verification

**Distributed Systems Mini Project** · Docker + Kubernetes · Python microservices + Redis

---

## 1. Problem Statement

College events (hackathons, fests, workshops, NSS drives, webinars) often have to issue **hundreds or thousands of certificates** right after the event. Today this is usually done with a single laptop running a mail-merge or Canva export, which causes three problems:

1. **Slow and doesn't scale.** One machine renders certificates one at a time, and a 2,000-participant fest can take hours.
2. **Single point of failure.** If that one machine or script crashes halfway through, the batch is lost or half-done.
3. **Easy to forge.** A PDF certificate can be edited in minutes, and recruiters or college offices have no way to check whether it is genuine.

**CertiFlow** is a distributed system that:

- takes a batch of participants from an organiser,
- splits it into independent jobs on a **shared work queue**,
- has **horizontally scalable worker pods** that render the PDF certificates in parallel (each with a QR code and an **HMAC-SHA256 digital signature**),
- provides a **public verification service** where anyone can scan the QR code or enter the ID to confirm the certificate is authentic and **untampered**.

If you add more worker pods, certificates are generated proportionally faster (scalability). If a pod dies, its jobs are not lost (fault tolerance).

---

## 2. Architecture

```
                          ┌─────────────────────────────┐
   Browser / QR scan ───► │   gateway  (2–5 pods, HPA)   │  NodePort 30080
                          │   Web UI + REST API routing  │
                          └──────┬────────────────┬──────┘
                       HTTP/REST │                │ HTTP/REST
                                 ▼                ▼
             ┌────────────────────────┐   ┌────────────────────────┐
             │ event-service (2 pods) │   │ verify-service (2 pods)│
             │ accept batch, progress │   │ HMAC check, serve PDF  │
             └───────────┬────────────┘   └───────────┬────────────┘
                 LPUSH jobs                     read cert records
                         ▼                            │
             ┌──────────────────────────────────────────────────────┐
             │              Redis  (queue + data store)             │
             │  certiflow:jobs → BLMOVE → certiflow:processing      │
             └──────────────────────────┬───────────────────────────┘
                                        │ BLMOVE (atomic, competing consumers)
             ┌──────────┬──────────┬────┴─────┬──────────┐
             │ worker-1 │ worker-2 │ worker-3 │ worker-N │  2–8 pods (HPA on CPU)
             └──────────┴──────────┴──────────┴──────────┘
               render PDF + QR, sign with HMAC, store result
```

| Service | Tech | Role | Scales? |
|---|---|---|---|
| `gateway` | Flask + Gunicorn | Single entry point; serves the UI; routes `/api/*` to internal services | Yes (HPA) |
| `event-service` | Flask + Gunicorn | Validates the batch, creates the event, enqueues one job per participant, reports progress and cluster stats | Yes (replicas) |
| `worker` | Python + ReportLab | Competing consumers: pop a job, render the PDF, sign it, store it, acknowledge | **Yes (HPA, main scaling point)** |
| `verify-service` | Flask + Gunicorn | Recomputes the HMAC to detect forgery or tampering; serves PDFs | Yes (replicas) |
| `redis` | Redis 7 | Message queue + shared state store | 1 pod |

### Distributed-systems concepts demonstrated

| Concept | Where in CertiFlow |
|---|---|
| **Inter-service communication (synchronous)** | gateway → event-service / verify-service over REST, using Kubernetes DNS names (`http://event-service:5001`) |
| **Inter-service communication (asynchronous)** | event-service → Redis queue → workers (producer–consumer message passing) |
| **Service discovery** | Kubernetes `Service` objects / Docker Compose network DNS |
| **Load balancing** | A K8s Service spreads requests across gateway, event and verify pods. Workers load-balance themselves through the queue (competing consumers). |
| **Horizontal scalability** | `kubectl scale` and HorizontalPodAutoscaler. Throughput grows with the number of worker pods. |
| **Fault tolerance** | `BLMOVE` hands a job off atomically into a processing list; failed jobs are retried up to 3 times; K8s restarts crashed pods; liveness and readiness probes |
| **Statelessness** | All app pods are stateless, and state lives in Redis, so any pod can serve any request |
| **Security / integrity** | HMAC-SHA256 signature over (ID, name, event, date) with the secret stored in a K8s `Secret`; tampering is detected |
| **Observability** | Live dashboard shows queue depth, alive workers (heartbeats) and per-pod job counts. Every response shows which pod served it. |
| **Configuration management** | `ConfigMap` and `Secret` injected as environment variables |

---

## 3. Folder Structure

```
certiflow/
├── README.md                 ← this file
├── docker-compose.yml        ← run everything locally with Docker
├── gateway/                  ← API gateway + web UI
│   ├── app.py
│   ├── static/index.html
│   ├── requirements.txt
│   └── Dockerfile
├── event-service/            ← batch intake + progress + stats
├── verify-service/           ← signature verification + PDF download
├── worker/                   ← certificate generator (scalable)
├── k8s/                      ← Kubernetes manifests (apply in order)
│   ├── 00-namespace.yaml
│   ├── 01-config.yaml        (ConfigMap + Secret)
│   ├── 02-redis.yaml
│   ├── 03-event-service.yaml
│   ├── 04-verify-service.yaml
│   ├── 05-worker.yaml
│   ├── 06-gateway.yaml       (NodePort 30080)
│   └── 07-hpa.yaml           (autoscalers)
└── scripts/
    ├── build-images.ps1      ← Windows image build
    ├── build-images.sh       ← Linux/macOS image build
    └── load_test.py          ← throughput benchmark (stdlib only)
```

---

## 4. Prerequisites

| Tool | Why | Check with |
|---|---|---|
| **Docker Desktop** (Windows/macOS) or Docker Engine (Linux) | build and run containers | `docker --version` |
| **Kubernetes**: either Docker Desktop's built-in K8s *(easiest on Windows)* or **minikube** | orchestration | `kubectl version --client` |
| **Python 3.8+** *(only for the load-test script)* | benchmark | `python --version` |

**Enabling Kubernetes in Docker Desktop:** open Docker Desktop → ⚙ Settings → **Kubernetes** → tick **Enable Kubernetes** → *Apply & restart*. If you are asked to choose a cluster type, pick **kubeadm** (it can use your locally built images directly). Wait until the bottom-left shows *Kubernetes running* (green). Then run:

```bash
kubectl config use-context docker-desktop
kubectl get nodes          # should show 1 node, STATUS Ready
```

---

## 5. PART A — Run with Docker Compose (quick test, ~5 min)

Open a terminal (PowerShell on Windows) **inside the `certiflow` folder**.

**Step A1: build and start all containers**
```bash
docker compose up --build -d
```
The first build downloads Python and Redis images, so it takes 2–4 minutes.

**Step A2: confirm everything is running**
```bash
docker compose ps
```
You should see `redis`, `event-service`, `verify-service`, `gateway` and **3** `worker` containers, all *running*.

**Step A3: open the app** at **http://localhost:8080**
1. On tab **1. Issue certificates**, click **Fill 200 sample participants**, then **Submit batch to cluster**.
2. Tab **2. Live cluster dashboard** shows the queue draining and the 3 workers sharing the load.
3. Click an event row, then **PDF** to open a certificate, or **verify** to check it.

**Step A4: scale workers up live**
```bash
docker compose up -d --scale worker=6
```
Submit another 200-participant batch. The dashboard now shows 6 workers, and the batch finishes about twice as fast.

**Step A5: check inter-service health**
```bash
curl http://localhost:8080/health
```

**Step A6: stop everything**
```bash
docker compose down
```

---

## 6. PART B — Deploy on Kubernetes (main submission)

All commands are run from the `certiflow` folder.

### Step B1: build the Docker images

**Windows (PowerShell):**
```powershell
powershell -ExecutionPolicy Bypass -File scripts\build-images.ps1
```
**Linux / macOS:**
```bash
bash scripts/build-images.sh
```
Check the images:
```bash
docker images | findstr certiflow      # Windows
docker images | grep certiflow         # Linux/macOS
```
You should see 4 images tagged `1.0`.

> **Using minikube instead of Docker Desktop?** Load the images into minikube after building:
> ```bash
> minikube start --cpus=4 --memory=4096
> minikube image load certiflow/gateway:1.0
> minikube image load certiflow/event-service:1.0
> minikube image load certiflow/verify-service:1.0
> minikube image load certiflow/worker:1.0
> ```

### Step B2: deploy everything
```bash
kubectl apply -f k8s/
```
Expected output: `namespace/certiflow created`, `configmap/... created`, `deployment.apps/... created` and so on.

### Step B3: watch the pods start
```bash
kubectl get pods -n certiflow -w
```
Wait until every pod shows `1/1 Running`, then press **Ctrl+C**. You should have:
`redis ×1, event-service ×2, verify-service ×2, worker ×2, gateway ×2` (9 pods).

```bash
kubectl get deploy,svc -n certiflow
```

### Step B4: open the application
- **Docker Desktop K8s:** http://localhost:30080
- **minikube:** `minikube service gateway -n certiflow --url`, then open the printed URL
- **Any cluster (fallback):** `kubectl port-forward svc/gateway 8080:80 -n certiflow`, then open http://localhost:8080

> On minikube or port-forward, the QR codes point to `http://localhost:30080` by default. To fix this, edit `PUBLIC_URL` in `k8s/01-config.yaml`, then run `kubectl apply -f k8s/01-config.yaml` and `kubectl rollout restart deploy/worker -n certiflow`.

### Step B5: issue certificates
Tab 1 → **Fill 200 sample participants** → **Submit**. On the dashboard, the **"Load distribution across worker pods"** card lists the real pod names (for example `worker-7c9d8f-abcde`), each with its share of the jobs.

---

## 7. Demonstrating Scalability (the key part of the viva)

### Demo 1: manual horizontal scaling
Use two terminals side by side.

**Terminal 1** (benchmark with 2 workers):
```bash
python scripts/load_test.py --url http://localhost:30080 --count 300
```
Note the final **certificates/second**.

**Terminal 2** (scale out):
```bash
kubectl scale deployment worker --replicas=6 -n certiflow
kubectl get pods -n certiflow -l app=worker
```
Run the load test again. Throughput should rise roughly in proportion to the number of workers (limited by your laptop's CPU cores). Record both numbers in your report, for example:

| Worker pods | Time for 300 certs | Certs/second |
|---|---|---|
| 2 | __ s | __ |
| 4 | __ s | __ |
| 6 | __ s | __ |

> If you created the HPA (Demo 2), it may scale your manual replicas back down. For a clean manual test, first run `kubectl delete hpa worker-hpa -n certiflow`.

### Demo 2: automatic scaling with HPA
HPA needs **metrics-server**.

- **minikube:** `minikube addons enable metrics-server`
- **Docker Desktop:**
  ```bash
  kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
  kubectl patch deployment metrics-server -n kube-system --type=json -p "[{\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/args/-\",\"value\":\"--kubelet-insecure-tls\"}]"
  ```
Wait about 1 minute, then confirm with `kubectl top pods -n certiflow` (it should print CPU numbers).

Next, start a large load and watch the autoscaler:
```bash
kubectl apply -f k8s/07-hpa.yaml
python scripts/load_test.py --url http://localhost:30080 --count 1500
```
In another terminal:
```bash
kubectl get hpa -n certiflow -w
```
Worker CPU rises above the 50 % target, and **REPLICAS climbs from 2 toward 8** automatically. About 1 minute after the queue empties, it scales back down.

### Demo 3: fault tolerance (self-healing)
While a big batch is processing:
```bash
kubectl get pods -n certiflow -l app=worker
kubectl delete pod <one-worker-pod-name> -n certiflow
kubectl get pods -n certiflow -l app=worker      # a new pod is created automatically
```
The batch still completes, because the other workers keep consuming the queue and Kubernetes replaces the killed pod.

### Demo 4: tamper detection
1. Copy any certificate ID from the dashboard, then **Verify** it. Result: ✔ Authentic.
2. Act as a forger and change the name directly in the database:
   ```bash
   kubectl exec -n certiflow deploy/redis -- redis-cli HSET cert:<CERT-ID> name "Fake Person"
   ```
3. Verify again. Result: ✘ **Signature mismatch, record has been TAMPERED.**

### Demo 5: inter-service communication and load balancing
```bash
curl http://localhost:30080/health
```
This shows the gateway reaching both internal services, with their pod names. Refresh the dashboard: the footer line *"Stats served by event-service pod …"* switches between the 2 event-service pods (Service load balancing). Verification results likewise show which verify-service pod answered.

Inspect the logs:
```bash
kubectl logs -n certiflow -l app=worker --tail=20 --prefix
```

---

## 8. Cleanup
```bash
kubectl delete namespace certiflow       # removes everything in Kubernetes
docker compose down                      # if you used Compose
```

---

## 9. Troubleshooting

| Symptom | Cause / Fix |
|---|---|
| Pods stuck in `ErrImagePull` / `ImagePullBackOff` | K8s can't see your local images. **Docker Desktop:** make sure you built the images *after* enabling Kubernetes and that the cluster type is kubeadm. **minikube:** run the `minikube image load` commands. Then run `kubectl rollout restart deploy -n certiflow`. |
| `localhost:30080` doesn't open | Use `kubectl port-forward svc/gateway 8080:80 -n certiflow` and open http://localhost:8080 |
| `kubectl get hpa` shows `<unknown>/50%` | metrics-server isn't installed or ready yet; see Demo 2 and wait a minute |
| Dashboard shows 0 workers | `kubectl logs -n certiflow -l app=worker`. Usually Redis isn't ready yet, and workers retry automatically. |
| Pods `CrashLoopBackOff` | `kubectl describe pod <name> -n certiflow` and `kubectl logs <name> -n certiflow` |
| Throughput doesn't increase beyond N workers | Your laptop has run out of CPU cores. Lower `CPU_WORK_MS` in `01-config.yaml`, or give Docker Desktop more CPUs. |
| `docker compose` not found | Use `docker-compose` (older versions) or update Docker Desktop |

---

## 10. API Reference

| Method | Path (via gateway) | Forwarded to | Purpose |
|---|---|---|---|
| POST | `/api/events` | event-service `/events` | Submit batch `{name, organizer, date, participants:[{name,email,role}]}` |
| GET | `/api/events` | event-service | List events with progress |
| GET | `/api/events/{id}` | event-service | Event + all certificate statuses |
| GET | `/api/stats` | event-service `/stats` | Queue length, active workers, per-pod counts |
| GET | `/api/verify/{certId}` | verify-service | Authenticity check |
| GET | `/api/certificate/{certId}/pdf` | verify-service | Download the PDF |
| GET | `/health` | gateway → both services | Aggregated health check |

---

## 11. Possible Viva Questions

- **Why a queue instead of the event-service calling workers directly?** It decouples the producer from the consumers. Workers can be added or removed at any time without anyone knowing their addresses, bursts are absorbed (back-pressure), and the load balances itself (a fast worker simply pulls more jobs).
- **What happens if a worker crashes mid-job?** `BLMOVE` moved the job atomically into `certiflow:processing`, so it is not lost. Errors inside processing are retried up to 3 times. K8s restarts the pod. *(A production improvement would be a "reaper" that re-queues jobs stuck in the processing list for too long.)*
- **Why HMAC and not just a database lookup?** A lookup only proves the ID exists. The HMAC proves the *content* (name, event, date) hasn't been altered, and only services holding the secret can create valid signatures.
- **What is the bottleneck / single point of failure?** Redis (1 replica). Improvements would be Redis Sentinel or Cluster, or a managed queue (RabbitMQ, Kafka), plus object storage (S3 or MinIO) for the PDFs instead of storing them in Redis.
- **Why is the gateway NodePort and the others ClusterIP?** Only the gateway should be reachable from outside. The internal services are reachable only inside the cluster.
- **How could autoscaling be smarter?** Scale on queue length instead of CPU, using **KEDA** with its Redis-list scaler.

---

## 12. Future Scope
- KEDA autoscaling on queue length
- Email delivery microservice (another queue consumer)
- MinIO / S3 for PDF storage; Redis Sentinel for high availability
- Ingress + TLS, and a Helm chart for one-command install
- Blockchain anchoring of signature hashes for public auditability

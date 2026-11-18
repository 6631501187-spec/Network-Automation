# AI-Driven Network Configuration and Automation Using OSPF

A web-based system that reads a network diagram image with a vision AI model, turns it into a validated
topology and Cisco IOS OSPFv2 configuration, and — only after explicit administrator approval — deploys it to
the routers through **Apache Kafka → Python worker → Paramiko SSH**, then verifies OSPF automatically.

Final result in the GNS3 lab: R1 and R2 form a **FULL** OSPF adjacency in area 0, each learns the other LAN via
OSPF, and **PC1 (10.0.0.10) pings PC2 (10.2.0.10)** with no manual router configuration.

## Architecture

```
Browser ── upload ▸ AI analyze ▸ review & resolve ▸ review commands ▸ validate ▸ approve ▸ deploy
   │                                                                     ▲ job monitor (/jobs)
   ▼                                                                     │
FastAPI backend (app/main.py) ── plans/jobs as JSON (data/) ── read-only SSH for validate/discover
   │ deploy: job PENDING + Kafka message (IDs + config SHA-256 only)
   ▼
Kafka topic network-config-jobs (Docker, KRaft)
   ▼
worker.py ── claim job from backend (re-checks approval + hash, PENDING→RUNNING once)
   │         per router: SSH ▸ hostname check ▸ backup running-config ▸ apply ▸ collect output
   │         verify: OSPF FULL ▸ O routes to remote LANs ▸ process/router ID ▸ no static routes ▸ ping
   ▼
R1 / R2 (Cisco c7200, IOS 15.2) — data-plane interfaces only; management interface never touched
```

| Component | File |
|---|---|
| Web UI (upload, resolve editor, review, validate, approve, deploy) | `app/topology.html` |
| Backend API, AI analysis, approval, Kafka producer, job monitor | `app/main.py` |
| IOS/OSPF configuration template (Jinja2) | `app/templates/router_ospf.j2` |
| Config generation, config hash, rollback block | `app/ios.py` |
| Validation (static + live) and interface discovery | `app/validation.py` |
| Paramiko SSH helpers (legacy IOS algorithms) | `app/router_ssh.py` |
| Kafka consumer / deployment worker, `--rollback` recovery | `worker.py` |
| Kafka (KRaft, single node) | `compose.yaml` |
| Start/stop script | `run.sh` |
| Lab helpers: router bootstrap, pre-demo SSH check, test diagrams | `lab/` |
| Demo runbook | `docs/DEMO.md` |

## Workflow and safety

1. **Upload** a PNG/JPG diagram (≤ 5 MB) and **Generate**. The image-capable model returns structured JSON
   (routers, interfaces, links, PCs, OSPF settings). Missing, unreadable or contradictory information is
   **flagged as an ambiguity instead of guessed**.
2. **Review & resolve**: the administrator can correct interface names, roles, addresses and router IDs.
   *Discover router interfaces* reads the routers' real interface names (`show ip interface brief`, read-only)
   and marks rows that do not exist or that carry the SSH management session. Each flagged item must be marked
   resolved with a note; the original AI output and all resolutions are kept.
3. **Review commands**: per-router IOS generated from the Jinja2 template — interface addressing, OSPF process 1,
   area 0, router IDs, passive LAN interfaces, active transit interface. No static routes.
4. **Validate**: duplicate addresses, subnet consistency per link, gateways, router IDs, real interface names,
   hostname, SSH privilege 15, and protection of the management interface.
5. **Approve** (explicit confirmation). The approved commands are stored with their SHA-256 hash.
6. **Deploy**: a job is created as **PENDING** and a Kafka message is sent to `network-config-jobs` containing only
   `job_id`, `plan_id`, `device_ids`, `config_sha256` and a reference to the approved config — no config text,
   no credentials.
7. **Worker**: claims the job from the backend, which atomically re-checks approval and the hash and moves the job
   to **RUNNING** exactly once (redelivered messages are rejected — no duplicate execution). It deploys over SSH,
   reports progress, and verifies OSPF (up to 120 s per router). Result: **SUCCESS** or **FAILED** with output.
8. **Job monitor** (`/jobs`): status, routers, timestamps, deployment output, and ✔/✘ verification checks.

The AI never connects to routers. Only the worker changes router configuration, and only for approved jobs.

## Requirements

- GNS3 lab: two Cisco routers (tested: c7200, IOS 15.2(4)S5) with SSH management on a separate interface
  (tested wiring: f0/0 management via DHCP, f1/0 R1–R2, f2/0 LAN), two VPCS hosts. Router bootstrap:
  `lab/router_bootstrap.txt`.
- Linux host with IP reachability to the routers' management addresses (tested: Ubuntu VM with a NIC on the
  same VMware network as the GNS3 management cloud), Docker + Docker Compose, Python 3.12+ with `venv`.
- A Groq API key (OpenAI-compatible API, image-capable model).

## Setup

```bash
git clone <this repository> ai-network-exam && cd ai-network-exam
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env && chmod 600 .env     # fill in GROQ_API_KEY, SSH_PASSWORD, R1_HOST, R2_HOST, interfaces
.venv/bin/python lab/check_routers.py      # trusts router host keys, checks SSH; must print "All routers ready."
./run.sh start                             # Kafka + topic + backend (:8000) + worker
```

Open `http://<host>:8000` (job monitor at `/jobs`). Stop with `./run.sh stop`; status and logs with
`./run.sh status` (logs in `data/logs/`).

Notes:
- `paramiko` is pinned to **3.5.1**: IOS 15.2 only offers `diffie-hellman-group14-sha1`, `ssh-rsa` host keys and
  CBC ciphers, which Paramiko 5 removed. `app/router_ssh.py` enables exactly those algorithms for router sessions.
- Groq's free tier allows roughly one diagram analysis per minute (HTTP 429 otherwise).
- The interface inventory in `.env` only gives the AI physical interface names; addresses always come from the image.

## Recovery

- A FAILED job shows the error and per-router output in the job monitor. Fix the cause and press **Deploy** again
  (a new job for the same approved plan).
- Before applying commands the worker saves each router's running-config in the job record.
- `.venv/bin/python worker.py --rollback latest` (or a plan ID) removes the OSPF process and resets the plan's LAN
  and transit interfaces. The management interface and SSH settings are never touched.

## Test results (GNS3 lab)

| Test | Result |
|---|---|
| Exam diagram → AI topology | Correct addresses, interfaces, router IDs; no ambiguities |
| Generated configuration | Matches the expected per-router OSPF configuration exactly |
| Validation against live routers | Passed (hostname, privilege, interfaces, management protection) |
| Kafka → worker → Paramiko deployment | Job PENDING → RUNNING → SUCCESS in about one minute |
| OSPF verification | Neighbor FULL on both routers; `O 10.2.0.0/24` on R1 and `O 10.0.0.0/24` on R2; no static routes |
| PC1 `ping 10.2.0.10` | 4/4 replies, TTL 62 |
| Duplicate Kafka delivery | Rejected by the claim step; not executed twice |
| Alternative diagram (different addressing, diagram labels conflicting with the lab's interfaces, no PCs) | Conflict flagged, resolved in the UI, deployed and verified (FULL, remote LAN via OSPF, LAN-to-LAN ping 100%) |
| Management interface chosen for a LAN | Blocked in the UI and by validation |
| Rollback | Data-plane interfaces reset; management SSH still up |

## Report and evidence

- Short report (architecture and test results): [`docs/REPORT.md`](docs/REPORT.md)
- Evidence screenshots with an index mapping each exam requirement and checklist item to a file:
  [`evidence/README.md`](evidence/README.md)
- Demo runbook: [`docs/DEMO.md`](docs/DEMO.md) · submission guide: [`docs/SUBMISSION.md`](docs/SUBMISSION.md)

## Security

- Secrets (`GROQ_API_KEY`, `SSH_PASSWORD`) live only in `.env`, which is git-ignored; `.env.example` has empty values.
- Router host keys are verified (`known_hosts`); unknown keys are rejected.
- Runtime data (`data/`: uploads, plans, jobs with router output, logs) is git-ignored.

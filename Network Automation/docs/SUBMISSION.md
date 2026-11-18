# Submission guide — steps, screenshots, final checks

What the exam asks for (§8): source code · Docker Compose files · configuration templates · README ·
evidence screenshots (AI analysis, Kafka jobs, deployment status, OSPF neighbors/routes, successful ping) ·
short report (architecture + test results) · **no secrets**.

| Required item | Where it is |
|---|---|
| Source code | `app/`, `worker.py`, `lab/check_routers.py`, `run.sh` |
| Docker Compose | `compose.yaml` |
| Configuration templates | `app/templates/router_ospf.j2` (IOS/OSPF), `lab/router_bootstrap.txt` (management bootstrap), `.env.example` |
| README / setup | `README.md`, `docs/DEMO.md` |
| Evidence screenshots | `evidence/` (you capture these — list below) |
| Short report | `docs/REPORT.md` (add your name, then optionally export to PDF) |

---

## Step 1 — Prepare (10 min)

1. Change the router password from the placeholder on **both** routers (console), and in `~/ai-network-exam/.env`:
   ```
   configure terminal
   username admin privilege 15 secret <new password>
   end
   write memory
   ```
2. Power-on and start the stack as in `docs/DEMO.md` §1–2. `lab/check_routers.py` must print **All routers ready.**
3. Reset to the instructor's starting state:
   ```bash
   .venv/bin/python worker.py --rollback latest
   ```
4. Check the PCs (VPCS consoles): PC1 `show ip` → 10.0.0.10/24 gw 10.0.0.1, PC2 → 10.2.0.10/24 gw 10.2.0.2.
5. Create a folder `evidence/` on Windows. Take screenshots with **Win + Shift + S** (window or rectangle), paste/save
   each as PNG using the names below.
6. Have these windows ready: browser on `http://192.168.177.131:8000` and a second tab on `/jobs`;
   Ubuntu terminal 1 (`tail -f ~/ai-network-exam/data/logs/worker.log`); Ubuntu terminal 2 (project folder);
   R1, R2 and PC1 consoles (GNS3).

## Step 2 — Run the workflow and capture (15 min)

Use the exam diagram `lab/exam_topology.png`. Capture in this order:

| # | File name | What to capture | Proves |
|---|---|---|---|
| 1 | `01-gns3-topology.png` | GNS3 canvas with R1, R2, PC1, PC2, Switch1, Cloud1 all green | Lab setup |
| 2 | `02-stack-running.png` | Terminal 2: `./run.sh status` and `docker compose ps` (Kafka healthy) | Services running |
| 3 | `03-upload-preview.png` | Web page after choosing the diagram: preview visible, **Generate** button | Task 1 · checklist 1 |
| 4 | `04-ai-analysis.png` | After Generate: topology editor + “No open ambiguities”; expand **Structured topology JSON** | Task 2 · checklist 2 |
| 5 | `05-discover-interfaces.png` | Click **Discover router interfaces**: real names, ✔ on each row, f0/0 marked SSH management | Task 4 (actual interface mapping) |
| 6 | `06-ios-commands.png` | **Review commands**: R1 and R2 command blocks (scroll if needed, two shots are fine) | Task 3 · checklist 2 |
| 7 | `07-validation-passed.png` | **Validate**: “Validation passed” + the checks JSON | Task 4 |
| 8 | `08-approved.png` | Tick the confirmation, **Approve**: status APPROVED with config SHA-256 | Task 4 (explicit approval) |
| 9 | `09-deploy-pending.png` | **Deploy** → confirm: job ID shown as PENDING | Task 5 |
| 10 | `10-kafka-message.png` | Terminal 2 (command below): the JSON message with job_id, plan_id, device_ids, config_sha256 | Task 5 · checklist 3 |
| 11 | `11-worker-log.png` | Terminal 1 after it finishes: Received → claimed → deploying R1/R2 → verified → SUCCESS → offset committed | Task 6 · checklist 4 |
| 12 | `12-job-monitor.png` | `/jobs` list: status badge SUCCESS, routers, created/started/finished times | Task 9 |
| 13 | `13-deployment-output.png` | Job **Details**: “R1 deployment (192.168.115.133)” with the IOS output (and R2) | Task 7 · checklist 4 |
| 14 | `14-verification.png` | Job Details: ✔ OSPF neighbor FULL, ✔ remote networks via OSPF, ✔ router ID, ✔ no static routes, ✔ ping | Task 8 |
| 15 | `15-r1-ospf.png` | R1 console: `show ip ospf neighbor` and `show ip route ospf` | checklist 5 + 6 |
| 16 | `16-r2-ospf.png` | R2 console: same two commands | checklist 5 + 6 |
| 17 | `17-r1-protocols.png` | R1 console: `show ip protocols` (ospf 1, Router ID 1.1.1.1, passive FastEthernet2/0) | Task 8 |
| 18 | `18-pc1-ping.png` | PC1 console: `ping 10.2.0.10` with replies | Task 10 · checklist 7 |

Kafka message command (terminal 2, in the project folder):
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic network-config-jobs --from-beginning --timeout-ms 5000
```

### Optional extra evidence (shows robustness — worth it if time allows)

| File name | How |
|---|---|
| `19-duplicate-rejected.png` | Terminal 2: re-send the last Kafka message (below); terminal 1 shows “Job is SUCCESS; it will not be executed again.” |
| `20-alt-diagram-flagged.png` | Wait 1 min, upload `lab/alt_topology.png` → Generate: AI flags the interface-label conflict instead of guessing |
| `21-alt-diagram-resolved.png` | Discover → tick the item + note → Save → Review → Validate passed (do **not** deploy it, or roll back after) |
| `22-management-protected.png` | In the editor, type `FastEthernet0/0` in a LAN row: “✘ carries SSH — must be management”, Review/Validate disabled (don’t save) |

Re-send the last message (duplicate test):
```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic network-config-jobs --from-beginning --timeout-ms 5000 2>/dev/null | tail -n 1 > /tmp/last.json
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 \
  --topic network-config-jobs < /tmp/last.json
```

## Step 3 — Finish the report

Open `docs/REPORT.md`: add your name/student ID, fill the timing row from screenshot 12, and reference the
screenshot files. Optionally export to PDF (e.g. open in VS Code → Markdown preview → print to PDF).

## Step 4 — Package and final checks

Put `evidence/` in the project root, then check **before** you zip or push:

- [ ] No `.env` anywhere in the submission; `.env.example` has empty `GROQ_API_KEY=` and `SSH_PASSWORD=`
- [ ] No `data/` folder (job records contain router running-configs with the password hash)
- [ ] No backup files (`*.backup`, `*.before-*`, `backups/`)
- [ ] Screenshots show **no API key, no password, no `show running-config`**, and no `.env` contents
- [ ] README opens and explains setup; `docs/REPORT.md` has your name
- [ ] Student checklist (§9) — each item has a screenshot:

| Checklist item | Screenshot |
|---|---|
| Frontend loads and accepts the diagram | 03 |
| AI produces a valid topology and OSPF plan | 04, 06 |
| Kafka job is visible | 09, 10, 12 |
| Worker deploys to R1 and R2 through Paramiko | 11, 13 |
| OSPF neighbor state is FULL | 14, 15, 16 |
| Remote LAN routes appear as OSPF routes | 14, 15, 16 |
| PC1 successfully pings PC2 | 18 |

Push to GitHub (your repo, private) or zip the folder — see the commands in the previous instructions /
README. Share the private repo with the instructor if required.

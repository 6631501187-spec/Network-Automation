# Demo runbook — from power-on to PC1 → PC2 ping

Lab: Windows host · VMware Workstation Pro · GNS3 (project **LabExam**) · Ubuntu VM `lone@192.168.177.131`
(project in `~/ai-network-exam`). Web app: **http://192.168.177.131:8000** · Job monitor: **/jobs**

---

## 0. One-time, before exam day

- [ ] Change the router password from the placeholder on **both** routers (console):
  `configure terminal` → `username admin privilege 15 secret <new>` → `end` → `write memory`
- [ ] Put the same password in `~/ai-network-exam/.env` (`SSH_PASSWORD=`). Never commit `.env`.
- [ ] PCs have their addresses saved (VPCS console):
  PC1 `ip 10.0.0.10/24 10.0.0.1` → `save` · PC2 `ip 10.2.0.10/24 10.2.0.2` → `save`

---

## 1. Power on (≈5 min)

1. **VMware Workstation** → start **GNS3 VM** and **Ubuntu 64-bit**.
2. **GNS3** → open project **LabExam** → **Start all nodes** (▶). Wait ~60 s for the routers to boot.
3. Read the routers' DHCP management addresses (R1 console `telnet 127.0.0.1 5000`, R2 `5001`):
   ```
   show ip interface brief | include FastEthernet0/0
   ```
   If either changed from `192.168.115.133` / `.134`, update `R1_HOST` / `R2_HOST` in `~/ai-network-exam/.env`.

## 2. Start the system (Ubuntu terminal)

```bash
cd ~/ai-network-exam
./run.sh start                       # Kafka + topic + backend + worker
.venv/bin/python lab/check_routers.py  # SSH to R1/R2, trusts new host keys; must say "All routers ready."
```

Keep the worker log open in a second terminal (good to show the examiner):

```bash
tail -f ~/ai-network-exam/data/logs/worker.log
```

## 3. Reset state (the instructor normally does this)

To rehearse from a clean state yourself:

```bash
.venv/bin/python worker.py --rollback latest   # removes OSPF, resets f1/0 + f2/0; f0/0 untouched
```

`check_routers.py` should then show f1/0 and f2/0 `unassigned / administratively down`.

---

## 4. The demonstration (≈5 min)

Open **http://192.168.177.131:8000** on Windows, and **/jobs** in a second tab.

| # | Do | Show / say |
|---|---|---|
| 1 | Choose the instructor's diagram (PNG/JPG) | Preview appears (Task 1) |
| 2 | **Generate** (wait 10–30 s) | Structured topology + JSON; AI flags only real ambiguities (Task 2) |
| 3 | If items are flagged: **Discover router interfaces** → fix names/IPs in the table → tick each item, write a note → **Save changes** | Real interface names come from the routers; resolutions are recorded |
| 4 | **Review commands** | Per-router IOS: interfaces, OSPF 1 area 0, router IDs, passive LAN (Task 3) |
| 5 | **Validate** | Subnets, gateways, real interfaces, SSH/management protection (Task 4) |
| 6 | Tick the confirmation → **Approve** | Approval + config SHA-256 shown |
| 7 | **Deploy** → confirm | Job PENDING; Kafka message has job ID, device IDs, config hash (Task 5) |
| 8 | Watch the worker log + job monitor | PENDING → RUNNING → SUCCESS; per-router output (Tasks 6, 7, 9) |
| 9 | Job monitor → **Details** | ✔ FULL neighbor, ✔ remote LAN via OSPF, ✔ router ID, ✔ no static routes (Task 8) |
| 10 | PC1 console: `ping 10.2.0.10` | Replies (Task 10) |

Show the Kafka message itself (Task 5 evidence):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic network-config-jobs --from-beginning --timeout-ms 5000
```

Optional router proof (read-only, on the consoles): `show ip ospf neighbor` · `show ip route ospf` · `show ip protocols`

### Screenshots to take for the submission
AI analysis (step 2) · validation result (5) · Kafka message (command above) · job monitor SUCCESS + details (9) ·
`show ip ospf neighbor` / `show ip route ospf` on R1 and R2 · PC1 ping.

---

## 5. If something goes wrong

| Symptom | Fix |
|---|---|
| `Groq error 429` on Generate | Free tier ≈ 1 analysis/minute. Wait ~40 s, click Generate again. |
| Validation: SSH timeout / host key not found | Management IP changed → update `.env`, run `lab/check_routers.py`, `./run.sh start`. |
| Validation: "carries SSH and must be management" | A LAN/transit row uses f0/0. Fix the interface name in the table, Save. |
| Job stays **PENDING** | Worker not running → `./run.sh status`, then `./run.sh start`. The job is picked up from Kafka. |
| Job **FAILED** | Read the error in the job monitor. Fix the cause, then click **Deploy** again (a new job is queued for the same approved plan). |
| Routers half-configured after a failure | `.venv/bin/python worker.py --rollback latest`, then deploy again. |
| Job stuck **RUNNING** (worker crashed mid-job) | It will not run twice by design. Roll back, generate/approve again. |

## 6. Shut down

```bash
./run.sh stop && docker compose stop
```
GNS3: stop all nodes (do **not** `write memory` on the routers after the demo, so they boot clean). VMware: suspend VMs.

# Evidence

Screenshots from the live GNS3 lab run on 2026-11-18 (`Screenshots.pdf` is the original capture).
The run used an instructor-style alternative diagram: R1 LAN 10.20.0.0/24, R1–R2 172.16.5.0/24,
R2 LAN 192.168.50.0/24 (PC1 10.20.0.11, PC2 192.168.50.11). Its interface labels (Fa0/0 for the LANs) conflict
with the lab's real wiring (f0/0 is management), so the AI flagged the conflict and it was resolved by the
administrator before validation. This shows the system is not hardcoded to one addressing plan.

Job `554327e6-6117-4822-a7d6-3031bc84c8b6` (plan `35447ddf-…`) is the job shown in files 07–12.
Worker log times are UTC (local time − 7 h): 10:49 UTC = 17:49 local.

## Required by the exam (§8)

| Requirement | Evidence |
|---|---|
| AI analysis | `04-ai-analysis-flagged.png` (structured topology, flagged ambiguity), `04b-ai-structured-json.png` (structured JSON in the UI), `04c-ai-structured-json.json` (full AI output, resolution record, approved topology, status APPROVED, config SHA-256 `25ca453c…` = the hash in the Kafka message), `06-validation-passed.png` |
| Kafka jobs | `07-kafka-topic-and-jobs.png` / `.txt` (topic, job messages: job_id, plan_id, device_ids, config_sha256, approved_config_ref; consumer group lag 0) |
| Deployment status | `09-job-monitor-running.png` (RUNNING), `10-job-monitor-success.png` (SUCCESS), `11-deployment-output.png`, `08-worker-log.png` |
| OSPF neighbors / routes | `12-ospf-verification.png`, `13`–`16` (R1/R2 `show ip ospf neighbor`, `show ip route ospf`, `show ip protocols`) |
| Successful ping | `17-pc1-ping-pc2.png`, `18-pc2-ping-pc1.png` |

## Student checklist (§9)

| Checklist item | Evidence |
|---|---|
| Frontend loads and accepts the diagram | `02-frontend-loaded.png`, `03-upload-preview.png` |
| AI produces a valid topology and OSPF plan | `04-ai-analysis-flagged.png`, `04b-ai-structured-json.png`, `04c-ai-structured-json.json`, `05-ios-commands.png`, `06-validation-passed.png` |
| Kafka job is visible | `07-kafka-topic-and-jobs.png`, `09`/`10` job monitor |
| Worker deploys to R1 and R2 through Paramiko | `08-worker-log.png`, `11-deployment-output.png` |
| OSPF neighbor state is FULL | `12-ospf-verification.png`, `13-r1-ospf-neighbor-routes.png`, `14-r2-ospf-neighbor-routes.png` |
| Remote LAN routes appear as OSPF routes | `12`, `13` (`O 192.168.50.0/24` on R1), `14` (`O 10.20.0.0` on R2) |
| PC1 successfully pings PC2 | `17-pc1-ping-pc2.png` (5/5, TTL 62) |

## Tasks 1–10

| Task | Evidence |
|---|---|
| 1 Web frontend | 02, 03 |
| 2 AI diagram analysis | 04, 04b, 04c (structured JSON; ambiguity flagged, not guessed) |
| 3 Cisco IOS and OSPF generation | 05 |
| 4 Validation and approval | 06 (live checks per router: hostname, privilege 15, interface names, management protection) |
| 5 Kafka producer | 07 |
| 6 Kafka consumer and worker | 08, 07 (consumer group offsets) |
| 7 Paramiko SSH deployment | 11 |
| 8 Automated OSPF verification | 12 |
| 9 Web job monitoring | 09, 10 |
| 10 End-to-end connectivity | 17, 18, 01 |

Files 07 and 08 are the real command output from the Ubuntu VM (the `.txt` files), rendered as images.

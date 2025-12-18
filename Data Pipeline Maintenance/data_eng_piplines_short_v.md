# Short Version: Data Engineering Pipelines Overview

This is a condensed version of the full pipeline ownership, runbooks, and on-call documentation.

## Pipelines
- **Profit:** Unit-Level (Experiments), Aggregate (Investor, P0)
- **Growth:** Daily (Experiments), Aggregate (Investor, P1)
- **Engagement:** Aggregate (Investor, P1)

## Ownership (Q1)

| Pipeline | Primary | Secondary |
|--------|---------|-----------|
| Unit Profit | Engineer A | Engineer B |
| Aggregate Profit | Engineer B | Engineer A |
| Daily Growth | Engineer D | Engineer C |
| Aggregate Growth | Engineer C | Engineer D |
| Aggregate Engagement | Engineer A | Engineer C |

## Risk Tiers
- **P0:** Aggregate Profit  
- **P1:** Aggregate Growth, Aggregate Engagement  
- **P2:** Experimental pipelines  

## On-Call
- Weekly rotation: A → B → C → D
- Pre-assigned backups by risk tier
- Holiday swaps allowed
- Backup covers 3–5 AM UTC if primary unavailable

## Investor-Facing Pipelines

### Aggregate Profit (P0)
- Schedule: 3:00 AM UTC
- SLA: Fresh by 6 AM; Ack ≤15m; Mitigate ≤2h
- Alerts: Runtime >2×, missing data
- Escalation: On-call → Backup → Owner → Eng Manager

### Aggregate Growth (P1)
- Schedule: 4:00 AM UTC
- SLA: Fresh by 7 AM; Ack ≤30m
- Alerts: 20% drop for 2 consecutive days

### Aggregate Engagement (P1)
- Schedule: 5:00 AM UTC
- SLA: Fresh by 8 AM; Ack ≤30m
- Alerts: Event volume drops, schema issues

## Handoff Checklist
- Open incidents
- Silenced alerts
- Upstream issues
- Pending schema changes

## Monitoring
- Airflow DAGs
- Datadog/Splunk logs
- PagerDuty → Slack

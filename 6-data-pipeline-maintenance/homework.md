## Org structure (roles)
DE4 — Tech Lead & Metrics Governance (TL): owns metric definitions, data contracts, QA standards; primary for the most finance-sensitive pipeline; secondary on all investor aggregates.
DE1 — Monetization Lead: Profit domain.
DE2 — Growth Lead: Growth domain.
DE3 — Engagement & Platform: Engagement domain + CI/CD, cost, observability.

## Pipeline ownership (primary / secondary)
| Pipeline (ID)                 | Description                                        | Primary                | Secondary                     |
| ----------------------------- | -------------------------------------------------- | ---------------------- | ----------------------------- |
| **profit\_unit\_experiment**  | Unit-level profit for experiments (near-real-time) | **DE1** (Monetization) | **DE4** (TL)                  |
| **profit\_agg\_investor**     | Aggregate profit reported to investors (daily)     | **DE4** (TL)           | **DE1** (Monetization)        |
| **growth\_agg\_investor**     | Aggregate growth reported to investors (daily)     | **DE2** (Growth)       | **DE4** (TL)                  |
| **growth\_daily\_experiment** | Daily growth for experiments (intra-day)           | **DE2** (Growth)       | **DE3** (Engagement/Platform) |
| **engagement\_agg\_investor** | Aggregate engagement reported to investors (daily) | **DE3** (Engagement)   | **DE4** (TL)                  |

## Ways of working (concise)
**RACI:** Primary = DRI/doer; Secondary = backup + code review; TL = approver for investor-facing changes.
**On-call:** Weekly rotation across DE1–DE4; investor aggregates get 08:00 Europe/Madrid SLA with paging.
**Quality gates:** dbt tests (schema/nulls/ranges), anomaly detection on key KPIs, contract checks on upstreams.
**Change control:** Any metric definition change on investor pipelines requires TL approval + Finance sign-off.
**Resilience:** Secondaries rotate quarterly; runbooks per pipeline; shadow week when ownership changes.
This keeps domain expertise clear, spreads load, and protects the investor-grade outputs with tight governance.

## On-call Operating Model
**Coverage:** 24/7 for P1 only; P2–P3 during 07:00–22:00. Investor KPIs get a 08:00 daily check.
Roles per week: Primary (P) handles pages; Secondary (S) is backup + reviewer. (TL/manager is escalation only.)

**Cadence:** 1-week blocks, Mon 09:00 → Mon 09:00. Everyone is Primary every 4th week; Secondary follows the next person in order.
Fairness points (balance quarterly):
Weekday on-call day = 1 pt
Weekend day = 1.5 pts
Public holiday (per team’s holiday calendar) = 2 pts
Keep each engineer’s total within ±10% by quarter (use swaps/credits to rebalance).

**Holidays (Spain + local/regional):**
If your week includes a public holiday, you get either time-off in lieu (1 day) or stipend, and those 2 pts go to your balance.
No one gets the same “major holiday” (e.g., Dec 25/Jan 1) two years in a row.
Publish a Q4 “holiday draft”: highest point-balance picks first which major holiday to avoid; rotate annually.

**Swaps & PTO:** Allowed anytime with mutual agreement; announce in Slack + update rota. If Primary is on PTO, Secondary becomes Primary and the next person becomes Secondary.
SLO & guardrails: Page only for clear SLO/SLA breaches; auto-tickets for non-urgent noise. Post-mortems required for sleep-interrupting pages.

### Example 8-week rotation
Week 1 (Mon 2025-09-01 → 09-08): P=DE1, S=DE2
Week 2 (09-08 → 09-15): P=DE2, S=DE3
Week 3 (09-15 → 09-22): P=DE3, S=DE4
Week 4 (09-22 → 09-29): P=DE4, S=DE1
Week 5 (09-29 → 10-06): P=DE1, S=DE2
Week 6 (10-06 → 10-13): P=DE2, S=DE3
Week 7 (10-13 → 10-20): P=DE3, S=DE4 (note: Oct-12 is a Spain public holiday; award 2 pts/comp time)
Week 8 (10-20 → 10-27): P=DE4, S=DE1

## What can go wrong with investor-reporting pipelines:
1. Ingestion & sources
Upstream outages/lag (CDP, app events, ERP, payments, ads APIs).
Schema changes (new/renamed fields, type changes, enum expansions).
Duplicate/missing events (retries, network drops, client SDK bugs).
Backfills without flags overwriting current data.

2. Time, calendars & currencies
Timezone drift (UTC vs Europe/Madrid vs user local); DST and leap day.
Cut-off mismatches (EoD vs finance close vs fiscal calendar).
Late/early events falling outside windows due to watermarks.
FX gaps (weekends/holidays), stale rates, wrong conversion direction.
Rounding/precision differences (banker’s rounding vs standard).

3. Identity & joins
User dedupe (Anon ↔ Auth merge errors); device resets; cookie loss (ITP/ATT).
Key mismatches across systems (SKU/offer ids, customer ids).
As-of joins done incorrectly with SCDs (point-in-time leakage).

4. Business logic defects
Revenue recognition vs bookings/billings conflated.
Profit: missing/late COGS, shipping, taxes, chargebacks, refunds timing.
Growth: DAU/WAU/MAU definitions drift; bots/test users included/excluded.
Engagement: event renames/new features breaking metric mappings.
Attribution windows wrong (install vs first purchase vs campaign touch).

5. Aggregations & windows
Double counting (overlapping windows, multi-touch).
Partial-day/partial-month published as final.
Sampling accidentally used instead of full data.
Reprocessing not idempotent (upserts append duplicates).

6. Data quality & contracts
Null spikes/out-of-range values (negative revenue, impossible counts).
Volume anomalies (sudden drops/spikes).
Contract breaches from upstream teams (breaking changes without notice).

7. Storage & table management
Partition/key skew causing job OOM/timeouts.
Compaction/vacuum misfires leading to stale snapshots.
Catalog permissions or table locks blocking writes/reads.

8. Orchestration & infra
Scheduler races (downstream runs before upstream completes).
Retries without jitter amplifying load; dead-letter queues filling.
Credential expiry/rotation (DB/KMS/warehouse).
Cost caps/quotas halting clusters at month-end peak.

9. Publication & governance
Definition drift from ad-hoc queries vs certified definitions.
Unapproved metric changes released before Finance sign-off.
Versioning missing (can’t reproduce numbers shown to investors).
Access controls (leaks or accidental exposure pre-earnings).

10. Backfills & corrections
Backfill rebases moving already-reported numbers.
Partial backfills creating discontinuities.
Historical FX/COGS updates not re-applied consistently.

## Pipeline Runbooks

### Aggregate Profit Reported to Investors
**Primary Owner:** DE4 — Tech Lead & Metrics Governance
**Secondary Owner:** DE1 — Monetization Lead

#### Common Issues

##### Upstream Datasets
**Orders & Payments (ERP/checkout):** missing/late exports; refunds/chargebacks arrive after close. 
**Action:** re-run extractor; if daily export fails, temporarily roll forward prior day’s snapshot and flag for true-up next run (mirrors the “use yesterday’s export” fallback pattern in the reference). 

**COGS & Taxes:** delayed postings cause profit spikes/dips. Action: mark day as provisional; reprocess when postings land.

**FX Rates:** stale/missing rates on weekends/holidays. Action: backfill official daily rates; freeze publishing until FX present.

**Schema changes:** renamed fields (e.g., net_revenue → net_sales). Action: apply data contract; hotfix mapping + backfill.

##### Downstream Consumers
Finance close & CFO deck, Investor dashboards (strict definition control). 

#### SLAs
Data available by 04:00 UTC (≈ 06:00 Europe/Madrid) each day, matching the “land 4 hours after UTC midnight” pattern in the reference.


### Aggregate Growth Reported to Investors
**Primary Owner:** DE2 — Growth Lead
**Secondary Owner:** DE4 — Tech Lead & Metrics Governance

#### Common Issues
##### Upstream Datasets
**Website/App events:** spikes/drops; source/referrer null inflation skews acquisition mix. 

**Action:** validate event volume vs 7-day baseline; apply downstream fix if known benign nulls (similar anomaly note in the reference).

**User database exports:** daily extract fails or is incomplete. Action: ingest prior day’s export as stopgap; annotate run and re-ingest when available (same fallback pattern as the reference). 

**Bot/QA traffic:** sudden growth from single ASN/device model. Action: re-apply filters; reprocess affected window.

##### Downstream Consumers
Experimentation platform (eligibility/denominators), Executive growth dashboards. 

#### SLAs
Data available by 04:00 UTC daily (≈ 06:00 Europe/Madrid), aligned to the reference SLA style.


### Aggregate Engagement Reported to Investors
**Primary Owner:** DE3 — Engagement & Platform
**Secondary Owner:** DE4 — Tech Lead & Metrics Governance

### Common Issues
#### Upstream Datasets
**Engagement events (sessions, lessons, time-in-app):** release renames/breaks event mappings; late/early events around DST. Action: hotfix mapping; widen lateness window; reprocess day.

**Feature flags/rollouts:** variant events not included in KPI set. Action: add variant → KPI mapping; backfill from launch date.

**Identity merges:** anon→auth stitching failures create double-counts. Action: re-run dedupe; point-in-time join with user table.

#### Downstream Consumers
Investor engagement dashboards, Product leadership summaries. 

### SLAs
Data available by 04:00 UTC daily (≈ 06:00 Europe/Madrid), following the same “4 hours after UTC midnight” expectation as the reference document.
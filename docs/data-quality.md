# Data Quality

ALI implements Bronze and Silver quality jobs with persisted result and quarantine tables. Gold quality is implemented as an operational gate, but Gold quality results are not persisted as a dashboard-readable result table in the current implementation.

## Quality Tables

| Table | Purpose |
|---|---|
| `demo.quality.bronze_quality_results` | One result row per Bronze quality rule |
| `demo.quality.bronze_quarantine` | Row-level audit copies for failed Bronze records |
| `demo.quality.silver_quality_results` | One result row per Silver quality rule |
| `demo.quality.silver_quarantine` | Row-level audit copies for failed Silver records |

## Result Semantics

Quality rules use:

- Severities: `CRITICAL`, `WARNING`.
- Statuses: `PASS`, `WARNING`, `FAIL`.
- Actions include no action, logged warning, quarantine copy, and blocking behavior for unsafe failures.

Result rows include check id, check time, source table, rule name, severity, total rows, failed rows, status, action taken, and details.

Quarantine rows include quarantine id, detected time, source table, record id, failed rule, severity, failure reason, raw record JSON, optional raw payload, and quarantine status.

## Bronze Quality

Bronze quality validates raw ingestion tables:

- Required fields.
- Raw payload availability and parseability.
- Duplicate source identifiers.
- Event and feedback type validity.
- Timestamp validity.
- Business-rule ranges that can be checked before Silver normalization.

Failed source rows are auditable in `demo.quality.bronze_quarantine`.

## Silver Quality

Silver quality validates normalized Silver tables:

- Required fields at each Silver grain.
- Duplicate grain checks.
- Valid enumerations and score ranges.
- Foreign-key-style checks across learner, event, question, reference, taxonomy, and evidence tables.
- Taxonomy parent integrity.
- Learner concept evidence type-specific fields and numeric ranges.

Failed Silver rows are auditable in `demo.quality.silver_quarantine`.

## Downstream Filtering

Silver transformation reads `demo.quality.bronze_quarantine` and excludes unresolved `OPEN` source records from normal Silver processing. Gold jobs read `demo.quality.silver_quarantine` and exclude unresolved `OPEN` Silver source rows from normal Gold processing.

This design keeps bad rows visible for audit without silently mixing them into curated analytics.

## Operational Failure Behavior

Handled row-level invalid data is copied to quarantine. Systemic failures, unsafe transformation failures, validation failures, and nonzero exit codes propagate to Airflow. Airflow keeps quality and validation gates blocking because task success depends on the job exit code.

Late-arriving feedback validation is separate from row-level quarantine. It fails when accepted feedback is missing from its expected Silver destination or too-late feedback appears in a normal Silver feedback table.

## Dashboard Quality Output

The dashboard export reads Bronze and Silver quality result tables and quarantine tables. It reports readable quality summaries and quarantine counts based on stored quarantine statuses. Gold table readability is displayed as table availability, not as persisted Gold quality pass/fail history.


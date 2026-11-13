# Operations and deployment runbook

## Environment and ownership

Use separate schemas and Volumes for dev and prod. The bundle targets provide this isolation. The configured catalog is existing `wsadbnov`; override with `--var catalog=...` when needed. The source manifest is intentionally fixed to this training release. The pipeline is not designed to infer unknown business schemas.

Production requirements are organization-specific: nominate an owner, approve a service principal as job `run_as`, grant only required access, configure failure notification recipients, agree recovery objectives, choose a retention window and unpause the schedule after a verified production run. No organization groups, secrets, email recipients or SLA values have been fabricated.

The supplied job runs as its deploying identity. For production configure `run_as.service_principal_name` in the production target and permissions for the actual operator groups; test that identity on all input and output paths. The deployer needs access to the workspace folder and permission to manage the Job. The runtime principal needs USE CATALOG, CREATE SCHEMA (first bootstrap), USE SCHEMA, CREATE TABLE, CREATE VOLUME, READ VOLUME, WRITE VOLUME, SELECT and MODIFY on the dedicated namespace, plus ownership needed to create/replace its views. Consider administrator-precreated schemas and volumes to remove bootstrap privileges.

## Access and PII

Bronze customer data, Silver customer history and quarantine contain synthetic contact details. Treat them as restricted if adapting this project to real data. Gold customer marts intentionally omit email, name and postal code. Age and location can still be personal data; apply your actual classification policy.

Do not grant consumer groups schema-wide SELECT: that would expose Bronze/Silver and bypass the reporting views. Administrators should grant USE CATALOG / USE SCHEMA and SELECT only on approved `report_*` views. Example with organization-supplied group names:

```sql
-- Replace groups and namespace before execution; these grants are not auto-applied.
GRANT USE CATALOG ON CATALOG wsadbnov TO `actual_analyst_group`;
GRANT USE SCHEMA ON SCHEMA wsadbnov.shopstream_prod TO `actual_analyst_group`;
GRANT SELECT ON VIEW wsadbnov.shopstream_prod.report_daily_sales TO `actual_analyst_group`;
```

The checkpoint and landing Volume requires engineer/runtime access, not analyst access. Use Unity Catalog lineage and audit logs under the organization's audit policy. No tokens should be stored in source, job parameters or notebook output.

## Normal operation

1. Validate the bundle with `--strict`, run the ten independent release and control tests, deploy to dev, and execute `shopstream_job`.
2. Verify all five tasks succeed, `ops_acceptance_checks` has 21 passing rows for that run, and `ops_publication.run_id` matches it.
3. Review `quality_*`, `customer_warnings` and `quarantine_*`. These tables are scoped by `_run_id`; always filter explicitly.
4. Promote the same source manifest and code to prod under the production runtime identity. The schedule stays paused until the operator deliberately enables it.
5. Configure Job UI failure notifications to approved recipients/webhooks and job duration health rules to an agreed threshold. No external messages are sent by this project setup.

The managed Job has `max_concurrent_runs: 1` and queues later runs. This is the supported writer entry point. It does not lock out a separately configured Job or a manual launcher; avoid these concurrent writers in the same schema. Use a separate manual schema when testing.

## Failure and repair

- **Preflight hash/header/count mismatch**: stop and identify the changed file. Restore the exact supplied file or deliberately create a reviewed new release with new manifest, contracts and acceptance checks. Do not disable the gate to accept an unexplained change.
- **Schema or Volume permission failure**: give the runtime identity the required grants or precreate the schema/Volume. Do not switch catalogs merely to bypass permission checks.
- **Quality threshold failure**: inspect the restricted quarantine table for the run and the corresponding `quality_*` counts. Do not lower a threshold silently. A change to a rule or threshold requires review and a fresh complete run.
- **Conflicting natural key / CDC lifecycle failure**: inspect the run's Bronze data. This is a source ambiguity; both variants stay in Bronze, and publication is blocked.
- **Transient task failure**: one automatic retry occurs after 60 seconds. Repair the failed task and its dependents in the Job UI, keeping the original job run ID. Writes replace that run and do not duplicate rows. Preflight repair points at the same checksummed immutable source.
- **Publication failure**: inspect `ops_acceptance_checks`. Prior `report_*` results remain active because the pointer has not advanced. For a first run, no usable publication exists until the checks pass.
- **Driver error details**: Job task output is authoritative. `ops_run_audit` records stage state and exception class without row data; a compute process terminated before exception handling can leave RUNNING there. Use Job state for timeouts/cancellation, not audit state alone.

A manual `00_run_all` invocation creates a fresh run ID. To repair only one stage, invoke its notebook with the original run ID and all original parameters. Never reuse a published run ID for source or transformation changes, as that would mutate an active publication.

## Streaming semantics and replay

The five original JSON files are read with an explicit schema and `availableNow`. A durable checkpoint lives under `<landing>/checkpoints/stream_events_v1`. Bronze MERGE uses the payload hash, so a replay after a commit/checkpoint gap is safe. Conflicting payloads sharing an event ID survive in Bronze and fail Silver uniqueness checks. The run freezes the complete event Bronze table for downstream consistency.

The checkpoint is shared by the managed Job's serialized runs. Do not delete it for a normal rerun; the second run should see 25 Bronze events and ingest no duplicate events. To rebuild after a damaged checkpoint, coordinate downtime, use a new checkpoint version and a new schema or deliberately replay the immutable input into the existing hash-MERGE table. Never reset a checkpoint while a stream is running.

This is a bounded file-stream demonstration using the supplied micro-batches, not a live Kafka feed or an Auto Loader implementation. Business event timestamps are retained. There is no watermark or late-data discard policy because the supplied events are finite and deduplicated in Delta. New arbitrary events are outside the fixed release contract.

## Rollback and reader consistency

All candidate data is retained by run ID. For rollback, identify a previously successful run with 21 passing checks and all eleven Gold tables populated; then, under change control, update only `ops_publication` for `pipeline='shopstream'` to that run ID. Do not publish an incomplete or failed run. View definitions are stable for this release and are created only if absent, preserving their existing grants. A future schema change requires a reviewed ALTER VIEW migration; do not drop/recreate granted views. See the [Databricks ALTER VIEW reference](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-alter-view).

A single publication pointer swap keeps each reporting query on one run. Independent queries can straddle a publication boundary. A reporting session requiring cross-query consistency should read `ops_publication` once and query `gold_* WHERE _run_id = '<pinned-id>'` through a controlled interface. Provide that interface to an authorized consumer instead of broadening schema-wide SELECT.

## Retention and scale

No destructive retention or VACUUM task is enabled. The small training dataset retains all run snapshots and quarantine history. Before production, implement an approved retention job: preserve the current publication and rollback window, remove expired run IDs from data/quality tables, archive audit evidence as required, then use supported Delta VACUUM retention. Landing snapshots and checkpoints have distinct retention needs. Never vacuum with zero retention or delete an active checkpoint.

Avoid date partitioning and OPTIMIZE on these tiny inputs. Broadcast the seven-row city lookup. For a materially larger release, benchmark query plans, small files and shuffle skew before introducing clustering, incremental change tracking or skew salting. The source is intentionally fixed; do not substitute another dataset to demonstrate scale.

## CI and release

The local release test uses Python's standard library and verifies exact checksums/contracts, decimal financial reconciliation, keys, CDC outcomes, nested arrays and notebook syntax. The remote integration run validates Spark, Delta, Unity Catalog, file streaming and reporting gates. Package with `python tools/build_import.py` after a passing run. Production deployment should use your actual CI identity and approval process; no arbitrary CI-provider credentials or repository integration is assumed.

The original taxi code and sample resource files are preserved but not used by `shopstream_job`. Its notebooks use built-in Databricks PySpark/Delta libraries; no wheel build, external business data or extra pip installation is required on serverless.

The generated taxi-test fixtures and Databricks Connect auto-start configuration are preserved under `examples/original_template/tests`. Pytest discovery is scoped to the active `tests/` directory so local checks cannot start the unrelated taxi workflow.

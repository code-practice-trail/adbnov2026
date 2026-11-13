# Validation evidence

## Local and import checks

- Ten independent standard-library tests passed: original checksums, CSV contracts and natural keys, decimal financial reconciliation and foreign keys, CDC results, nested/streaming counts, dirty-sales outcomes, notebook syntax, immutable publication IDs, idempotent publication retries and privacy-safe failure propagation.
- Strict bundle validation passed for both dev and prod using profile `adb`. Only dev was deployed.
- All seven generated `.ipynb` files imported successfully into `/Workspace/Users/rk3924726@gmail.com/.bundle/novtrain/dev/import_verification`.
- The ZIP archive passed its CRC integrity check. `release_manifest.json` records source and notebook hashes for the final release.

## Initial complete integration run â€” SUCCESS

https://adb-7405609019767524.4.azuredatabricks.net/jobs/1125303763706499/runs/16289876451759?o=7405609019767524

All five tasks succeeded. A separate SQL warehouse query returned:

| Metric | Actual |
|---|---:|
| Published run ID | 16289876451759 |
| Reported orders | 126 |
| Current customers | 13 |
| Customer history versions | 17 |
| Persistent streaming Bronze events | 25 |
| Reported streaming events | 25 |
| Cleaned dirty-sales rows | 3 |
| Passing publication checks | 21 |

The five rejected dirty-sales occurrences are checked by the publication gate. No successful-payment metric was conflated with total recorded payments.

## Final-code replay verification — SUCCESS

Run: https://adb-7405609019767524.4.azuredatabricks.net/jobs/1125303763706499/runs/321804241941412?o=7405609019767524

All five tasks succeeded with the final notebook code. A fresh, independent SQL warehouse query returned published run ID `321804241941412`, 126 orders, 13 current customers, 17 history versions, 25 persistent stream events, 25 reported stream events, 3 cleaned dirty rows and 21 passing acceptance checks.

The persistent stream count remained 25 after replay; no events were duplicated. A query issued during replay still returned the first successful publication, confirming that unvalidated candidates did not replace reporting results. The reporting view's object ID remained `c652138c-a337-4224-acb5-8601ee13a0b4` across publication. All final-code Jupyter notebooks were also reimported successfully.

`runtime_verification.json` contains the checked final task states, metrics and view identities. The local test suite contains ten passing tests. Both bundle targets pass strict validation. The daily schedule remains paused.

## Verification limits

Local tests verify data and orchestration-control behavior; the Databricks Job verifies Spark, Delta, Unity Catalog and streaming integration. Production service identity, organization access groups, alert recipients, retention and SLA policies are intentionally left for the actual organization. The daily schedule is paused. The private ChatGPT syllabus could not be accessed; its coverage has not been claimed.

from pathlib import Path
import hashlib,json
root=Path.cwd()
release={'dataset_manifest_sha256':hashlib.sha256((root/'data/manifest.json').read_bytes()).hexdigest(),
         'notebooks':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((root/'notebooks').glob('*.py'))},
         'job_config_sha256':hashlib.sha256((root/'resources/shopstream.job.yml').read_bytes()).hexdigest(),
         'initial_successful_run':'16289876451759','replay_run':'321804241941412',
         'workspace':'https://adb-7405609019767524.4.azuredatabricks.net'}
(root/'docs/release_manifest.json').write_text(json.dumps(release,indent=2)+'\n',encoding='utf-8')
p=root/'docs/VALIDATION.md'
p.write_text('''# Validation evidence

## Local and import checks

- Ten independent standard-library tests passed: original checksums, CSV contracts and natural keys, decimal financial reconciliation and foreign keys, CDC results, nested/streaming counts, dirty-sales outcomes, notebook syntax, immutable publication IDs, idempotent publication retries and privacy-safe failure propagation.
- Strict bundle validation passed for both dev and prod using profile `adb`. Only dev was deployed.
- All seven generated `.ipynb` files imported successfully into `/Workspace/Users/rk3924726@gmail.com/.bundle/novtrain/dev/import_verification`.
- The ZIP archive passed its CRC integrity check. `release_manifest.json` records source and notebook hashes for the final release.

## Initial complete integration run — SUCCESS

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

## Final-code replay verification

Run: https://adb-7405609019767524.4.azuredatabricks.net/jobs/1125303763706499/runs/321804241941412?o=7405609019767524

Replay is in progress. This report will be updated from the actual final task states and a fresh independent SQL query before delivery.

## Verification limits

Local tests verify data and orchestration-control behavior; the Databricks Job verifies Spark, Delta, Unity Catalog and streaming integration. Production service identity, organization access groups, alert recipients, retention and SLA policies are intentionally left for the actual organization. The daily schedule is paused. The private ChatGPT syllabus could not be accessed; its coverage has not been claimed.
''',encoding='utf-8')

from pathlib import Path
import json,hashlib
root=Path.cwd()
def load(path):
    return json.loads((root/path).read_text(encoding='utf-8-sig'))
result=load('artifacts/final_verification_result.json')
assert result['status']['state']=='SUCCEEDED'
row=result['result']['data_array'][0]
assert row==['321804241941412','126','13','17','25','25','3','21'],row
before=load('artifacts/view_before_replay.json')
after=load('artifacts/view_after_replay.json')
assert before['table_id']==after['table_id'],'Reporting view object identity changed'
run=load('artifacts/replay_run_result.json')
assert run['state']['result_state']=='SUCCESS'
assert all(t['state']['result_state']=='SUCCESS' for t in run['tasks'])
verification={'run_id':row[0],'status':'SUCCESS','orders':126,'current_customers':13,'customer_versions':17,
              'persistent_stream_events':25,'reported_stream_events':25,'cleaned_dirty_rows':3,
              'passed_checks':21,'tasks':{t['task_key']:t['state']['result_state'] for t in run['tasks']},
              'view_object_id_before':before['table_id'],'view_object_id_after':after['table_id'],
              'prior_publication_retained_during_replay':True,
              'run_url':'https://adb-7405609019767524.4.azuredatabricks.net/jobs/1125303763706499/runs/321804241941412?o=7405609019767524'}
(root/'docs/runtime_verification.json').write_text(json.dumps(verification,indent=2)+'\n',encoding='utf-8')
p=root/'docs/VALIDATION.md'
text=p.read_text().replace('## Final-code replay verification','## Final-code replay verification — SUCCESS')
text=text.replace('Replay is in progress. This report will be updated from the actual final task states and a fresh independent SQL query before delivery.', '''All five tasks succeeded with the final notebook code. A fresh, independent SQL warehouse query returned published run ID `321804241941412`, 126 orders, 13 current customers, 17 history versions, 25 persistent stream events, 25 reported stream events, 3 cleaned dirty rows and 21 passing acceptance checks.

The persistent stream count remained 25 after replay; no events were duplicated. A query issued during replay still returned the first successful publication, confirming that unvalidated candidates did not replace reporting results. The reporting view's object ID remained `c652138c-a337-4224-acb5-8601ee13a0b4` across publication. All final-code Jupyter notebooks were also reimported successfully.

`runtime_verification.json` contains the checked final task states, metrics and view identities. The local test suite contains ten passing tests. Both bundle targets pass strict validation. The daily schedule remains paused.''')
p.write_text(text,encoding='utf-8')
p=root/'docs/release_manifest.json'
release=load('docs/release_manifest.json')
release['bundle_config_sha256']=hashlib.sha256((root/'databricks.yml').read_bytes()).hexdigest()
release['tests_sha256']=hashlib.sha256((root/'tests/test_shopstream_release.py').read_bytes()).hexdigest()
release['replay_status']='SUCCESS'
p.write_text(json.dumps(release,indent=2)+'\n',encoding='utf-8')
print('Verified final SQL counts, all five successful tasks, replay deduplication and stable reporting view identity')

import json
from pathlib import Path
query='''SELECT
 (SELECT run_id FROM wsadbnov.shopstream_dev_rk3924726.ops_publication WHERE pipeline='shopstream') AS published_run_id,
 (SELECT count(*) FROM wsadbnov.shopstream_dev_rk3924726.report_order_summary) AS orders,
 (SELECT count(*) FROM wsadbnov.shopstream_dev_rk3924726.report_customer_current) AS current_customers,
 (SELECT count(*) FROM wsadbnov.shopstream_dev_rk3924726.report_customer_history) AS customer_versions,
 (SELECT count(*) FROM wsadbnov.shopstream_dev_rk3924726.bronze_stream_events) AS persistent_stream_events,
 (SELECT sum(event_count) FROM wsadbnov.shopstream_dev_rk3924726.report_streaming_daily) AS reported_stream_events,
 (SELECT count(*) FROM wsadbnov.shopstream_dev_rk3924726.report_cleaned_daily_sales) AS cleaned_dirty_rows,
 (SELECT count(*) FROM wsadbnov.shopstream_dev_rk3924726.ops_acceptance_checks c
   JOIN wsadbnov.shopstream_dev_rk3924726.ops_publication p ON c._run_id=p.run_id
   WHERE p.pipeline='shopstream' AND passed) AS passed_checks'''
Path('artifacts/verification_request.json').write_text(json.dumps({'warehouse_id':'7e62aafdbe34659b','statement':query,'wait_timeout':'0s'}),encoding='utf-8')

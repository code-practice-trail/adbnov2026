# Databricks notebook source
# MAGIC %md
# MAGIC # 05 â€” Reconcile and publish
# MAGIC All marts resolve through one Delta publication pointer. A failed run leaves the prior publication active.
# MAGIC Consumers needing a consistent multi-query session should pin the returned run ID.
# COMMAND ----------
# MAGIC %run ./_common
# COMMAND ----------
GOLD_TABLES = ['order_summary','daily_sales','product_sales','payment_reconciliation','inventory',
               'customer_current','customer_history','clickstream_daily','streaming_daily','regional_skew_sales','cleaned_daily_sales']

def validate_publish():
    checks = []
    def check(name, condition):
        checks.append((name,bool(condition)))
    orders, items = read('silver_orders'), read('silver_order_items')
    summary, daily = read('gold_order_summary'), read('gold_daily_sales')
    check('orders_row_count',summary.count() == orders.count())
    check('unique_order_grain',summary.select('order_id').distinct().count() == summary.count())
    check('all_orders_have_items',orders.join(items.select('order_id').distinct(),'order_id','left_anti').count() == 0)
    check('all_orders_have_payment',summary.where('recorded_payment_amount IS NULL').count() == 0)
    check('recorded_payment_matches_lines',read('gold_payment_reconciliation').where(
        'recorded_difference IS NULL OR abs(recorded_difference) > 0.01').count() == 0)
    def total(df, expression):
        return df.agg(F.expr(expression).alias('v')).first()['v']
    check('line_amount_reconciles',total(summary,'sum(order_amount)') == total(items,'sum(line_amount)'))
    check('daily_order_count',total(daily,'sum(order_count)') == orders.count())
    check('product_sales_reconciles',total(read('gold_product_sales'),'sum(sales)') ==
          total(summary.where("order_status != 'CANCELLED'"),'sum(order_amount)'))
    check('inventory_reconciles',total(read('gold_inventory'),'sum(stock_qty)') ==
          total(read('silver_inventory_snapshot'),'sum(stock_qty)'))
    check('streaming_reconciles',total(read('gold_streaming_daily'),'sum(event_count)') == read('silver_streaming_orders').count())
    check('clickstream_reconciles',total(read('gold_clickstream_daily'),'sum(event_count)') == read('silver_clickstream').count())
    check('skew_reconciles',total(read('gold_regional_skew_sales'),'sum(amount)') == total(read('silver_skew_sales'),'sum(amount)'))
    history = read('silver_customer_history')
    check('one_current_customer_version',history.where('is_current').groupBy('customer_id').count().where('count != 1').count() == 0)
    check('valid_history_intervals',history.where('valid_to IS NOT NULL AND valid_to <= valid_from').count() == 0)
    check('cdc_c004_latest',read('silver_customer_current').where("customer_id='C004' AND email='anjali.latest@example.com'").count() == 1)
    check('cdc_c013_inserted',read('silver_customer_current').where("customer_id='C013'").count() == 1)
    # Golden-source acceptance checks intentionally pin this training release, not an unknown feed.
    check('provided_orders_126',orders.count() == 126)
    check('provided_items_259',items.count() == 259)
    check('provided_stream_events_25',read('silver_streaming_orders').count() == 25)
    check('dirty_sales_expected_3',read('silver_dirty_daily_sales').count() == 3)
    check('dirty_quarantine_expected_5',read('quarantine_dirty_daily_sales').count() == 5)
    save(spark.createDataFrame(checks,'check_name string, passed boolean'),'ops_acceptance_checks')
    failed = [name for name,passed in checks if not passed]
    require(not failed,'Publication blocked: ' + ', '.join(failed))
    # Stable view definitions preserve object identity and consumer grants across runs.
    # Schema changes require a reviewed ALTER VIEW migration before publication.
    for name in GOLD_TABLES:
        columns = [c for c in read('gold_'+name).columns if c != '_run_id']
        projection = ', '.join('g.`'+c+'`' for c in columns)
        spark.sql(f'''CREATE VIEW IF NOT EXISTS {table('report_'+name)} AS SELECT {projection}, g._run_id
            FROM {table('gold_'+name)} g JOIN {table('ops_publication')} p
            ON g._run_id=p.run_id WHERE p.pipeline='shopstream' ''')
    spark.sql(f'''MERGE INTO {table('ops_publication')} t
        USING (SELECT 'shopstream' pipeline, '{RUN_ID}' run_id, current_timestamp() published_at) s
        ON t.pipeline=s.pipeline WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *''')
    print(json.dumps({'status':'PUBLISHED','run_id':RUN_ID,'checks_passed':len(checks),'schema':f'{CATALOG}.{SCHEMA}'}))
execute('validate_publish',validate_publish)

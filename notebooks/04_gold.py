# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold candidate marts
# MAGIC Each candidate remains isolated by run ID until reconciliation succeeds.
# MAGIC June transactional sales, July dirty sales, streaming orders and skew data are separate scenarios.
# MAGIC Product cost is the supplied current cost; margin is an estimate, not historical accounting margin.
# COMMAND ----------
# MAGIC %run ./_common
# COMMAND ----------
GOLD_TABLES = ['order_summary','daily_sales','product_sales','payment_reconciliation','inventory',
               'customer_current','customer_history','clickstream_daily','streaming_daily','regional_skew_sales','cleaned_daily_sales']

def gold():
    orders, items, payments = read('silver_orders'), read('silver_order_items'), read('silver_payments')
    products, cities = read('silver_products'), read('silver_city_region_lookup')
    line = items.join(products.select('product_id','product_name','category','cost_price'),'product_id')
    line = line.withColumn('estimated_cost',F.col('quantity')*F.col('cost_price'))
    merchandise = line.groupBy('order_id').agg(F.sum('line_amount').cast('decimal(20,2)').alias('order_amount'),
        F.sum('estimated_cost').cast('decimal(20,2)').alias('estimated_cost'),F.sum('quantity').alias('units'))
    payment = payments.groupBy('order_id').agg(
        F.sum('amount').cast('decimal(20,2)').alias('recorded_payment_amount'),
        F.sum(F.when(F.col('payment_status') == 'SUCCESS',F.col('amount')).otherwise(0)).cast('decimal(20,2)').alias('successful_payment_amount'),
        F.sum(F.when(F.col('payment_status') == 'REFUNDED',F.col('amount')).otherwise(0)).cast('decimal(20,2)').alias('refunded_payment_amount'),
        F.count('*').alias('payment_attempts'))
    summary = orders.join(merchandise,'order_id','left').join(payment,'order_id','left')
    summary = summary.withColumn('order_date',F.to_date('order_ts'))
    summary = summary.join(F.broadcast(cities.select(F.col('city').alias('shipping_city'),'region')),'shipping_city','left')
    summary = summary.withColumn('region',F.coalesce('region',F.lit('Unknown')))
    summary = summary.select('order_id','customer_id','order_ts','order_date','order_status','shipping_city','region','channel',
        'order_amount','estimated_cost','units','recorded_payment_amount','successful_payment_amount','refunded_payment_amount','payment_attempts')
    save(summary,'gold_order_summary')
    daily = summary.groupBy('order_date','region','channel').agg(
        F.count('*').alias('order_count'),
        F.sum(F.when(F.col('order_status') != 'CANCELLED',F.col('order_amount')).otherwise(0)).cast('decimal(22,2)').alias('booked_sales'),
        F.sum(F.when(F.col('order_status') == 'DELIVERED',F.col('order_amount')).otherwise(0)).cast('decimal(22,2)').alias('delivered_sales'),
        F.sum('successful_payment_amount').cast('decimal(22,2)').alias('successful_payments'),
        F.sum(F.when(F.col('order_status') == 'CANCELLED',1).otherwise(0)).alias('cancelled_orders'))
    save(daily,'gold_daily_sales')
    product_sales = (line.join(orders.select('order_id','order_ts','order_status'),'order_id')
        .where("order_status != 'CANCELLED'").withColumn('order_date',F.to_date('order_ts'))
        .groupBy('order_date','product_id','product_name','category').agg(F.sum('quantity').alias('units'),
            F.sum('line_amount').cast('decimal(22,2)').alias('sales'),
            F.sum(F.col('line_amount')-F.col('estimated_cost')).cast('decimal(22,2)').alias('estimated_margin')))
    save(product_sales,'gold_product_sales')
    reconciliation = summary.select('order_id','order_status','order_amount','recorded_payment_amount',
        'successful_payment_amount','refunded_payment_amount').withColumn('recorded_difference',
        F.col('recorded_payment_amount')-F.col('order_amount')).withColumn('uncollected_amount',
        F.col('order_amount')-F.coalesce('successful_payment_amount',F.lit(0)))
    save(reconciliation,'gold_payment_reconciliation')
    inventory = read('silver_inventory_snapshot').groupBy('snapshot_date','product_id').agg(
        F.sum('stock_qty').alias('stock_qty'),F.countDistinct('warehouse_id').alias('warehouse_count'))
    save(inventory,'gold_inventory')
    # Minimize PII in serving tables; email/name/postal code remain only in restricted Silver.
    public_customer_columns = ['customer_id','city','state','registration_date','status','source_system','age',
                               'valid_from','valid_to','is_current','is_deleted','operation']
    save(read('silver_customer_current').select(*public_customer_columns),'gold_customer_current')
    save(read('silver_customer_history').select(*public_customer_columns),'gold_customer_history')
    clicks = read('silver_clickstream').withColumn('event_date',F.to_date('event_ts'))
    save(clicks.groupBy('event_date',F.col('device.type').alias('device_type')).agg(
        F.count('*').alias('event_count'),F.countDistinct('customer_id').alias('unique_customers')),'gold_clickstream_daily')
    events = read('silver_streaming_orders').withColumn('event_date',F.to_date('event_ts'))
    save(events.groupBy('event_date','status').agg(F.count('*').alias('event_count'),
        F.sum('amount').cast('decimal(22,2)').alias('amount')),'gold_streaming_daily')
    # Broadcast the tiny lookup; do not salt or repartition a classroom-size dataset needlessly.
    skew = read('silver_skew_sales').join(F.broadcast(cities.select('city','region')),'city','left')
    save(skew.withColumn('region',F.coalesce('region',F.lit('Unknown'))).groupBy('region').agg(
        F.count('*').alias('transaction_count'),F.sum('amount').cast('decimal(22,2)').alias('amount')),'gold_regional_skew_sales')
    dirty = read('silver_dirty_daily_sales')
    save(dirty.select('order_id','customer_id','city','product_name','quantity','unit_price','payment_mode','order_date','customer_status')
         .withColumn('sales_amount',(F.col('quantity')*F.col('unit_price')).cast('decimal(20,2)')),'gold_cleaned_daily_sales')
execute('gold',gold)

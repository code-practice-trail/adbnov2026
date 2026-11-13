# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver: contracts, quarantine, relationships and customer history
# MAGIC Invalid optional ages become null with a warning; missing city becomes `Unknown`.
# MAGIC No missing quantity, price, key, or event time is invented. CDC ordering is by source update time.
# COMMAND ----------
# MAGIC %run ./_common
# COMMAND ----------
def clean(name):
    df = read('bronze_' + name)
    for field in df.schema.fields:
        if isinstance(field.dataType, T.StringType) and not field.name.startswith('_'):
            c = field.name
            df = df.withColumn(c, F.when(F.length(F.trim(F.col(c))) > 0, F.trim(F.col(c))))
    return df


def silver():
    city = clean('city_region_lookup').withColumn('city', F.initcap('city'))
    cities = quality('city_region_lookup', city, ['city'], {
        'city_required': not_blank('city'), 'region_required': not_blank('region')})
    customers = clean('customers').withColumn('status', F.upper('status')).withColumn('email', F.lower('email'))
    customers = customers.withColumn('_age_raw', F.col('age'))
    customers = cast_col(customers, 'age', 'int').withColumn('registration_date', parse_date('registration_date'))
    customers = customers.withColumn('_age_warning', F.col('_age_raw').isNotNull() &
        (F.col('age').isNull() | ~F.col('age').between(0,120)))
    save(customers.where('_age_warning').select('customer_id', F.lit('invalid_optional_age').alias('warning')), 'customer_warnings')
    customers = (customers.withColumn('age', F.when(F.col('age').between(0,120), F.col('age')))
                 .withColumn('city', F.coalesce(F.initcap('city'), F.lit('Unknown')))
                 .drop('_age_raw', '_age_warning'))
    base = quality('customers', customers, ['customer_id'], {
        'id_required': not_blank('customer_id'), 'name_required': not_blank('customer_name'),
        'email_invalid': F.col('email').rlike(r'^[^\s@]+@[^\s@]+\.[^\s@]+$'),
        'registration_invalid': F.col('registration_date').isNotNull(),
        'status_invalid': allowed('status',['ACTIVE','INACTIVE'])})
    changes = clean('customer_updates_cdc').withColumn('op', F.upper('op')).withColumn('status', F.upper('status'))
    changes = changes.withColumn('email', F.lower('email')).withColumn('city', F.initcap('city'))
    changes = changes.withColumn('update_ts', timestamp('update_ts'))
    cdc = quality('customer_updates_cdc', changes, ['customer_id','update_ts'], {
        'id_required': not_blank('customer_id'), 'timestamp_invalid': F.col('update_ts').isNotNull(),
        'operation_invalid': allowed('op',['I','U','D']),
        'name_required': (F.col('op') == 'D') | not_blank('customer_name'),
        'email_invalid': (F.col('op') == 'D') | F.col('email').rlike(r'^[^\s@]+@[^\s@]+\.[^\s@]+$'),
        'status_invalid': (F.col('op') == 'D') | allowed('status',['ACTIVE','INACTIVE'])})
    # The supplied CDC is an ordered complete patch for these five fields, not a complete customer row.
    # Retain base-only attributes. Null patch values on D are intentional tombstones.
    attrs = ['customer_id','customer_name','email','city','state','postal_code','registration_date','status','source_system','age']
    patches = cdc.alias('c').join(base.alias('b'), 'customer_id', 'left')
    patch_fields = ['customer_name','email','city','status']
    patch = patches.select(F.col('customer_id'), *[
        F.col(('c.' if c in patch_fields else 'b.')+c).alias(c) for c in attrs[1:]],
        F.col('c.update_ts').alias('valid_from'), F.col('c.op').alias('operation'))
    initial = base.select(*attrs, F.col('registration_date').cast('timestamp').alias('valid_from'), F.lit('I').alias('operation'))
    history = initial.unionByName(patch)
    require(history.groupBy('customer_id','valid_from').count().where('count > 1').count() == 0,
            'Ambiguous customer history timestamps')
    # U/D must have a predecessor; I must not introduce an already existing entity.
    w = Window.partitionBy('customer_id').orderBy('valid_from')
    history = history.withColumn('_previous', F.lag('operation').over(w))
    invalid_ops = ((F.col('operation').isin('U','D') & (F.col('_previous').isNull() | (F.col('_previous') == 'D'))) |
                   ((F.col('operation') == 'I') & F.col('_previous').isNotNull() & (F.col('_previous') != 'D')))
    require(history.where(invalid_ops).count() == 0, 'Invalid CDC lifecycle')
    history = (history.drop('_previous').withColumn('valid_to', F.lead('valid_from').over(w))
               .withColumn('is_current', F.col('valid_to').isNull())
               .withColumn('is_deleted', F.col('operation') == 'D'))
    save(history, 'silver_customer_history')
    current = history.where('is_current AND NOT is_deleted')
    save(current, 'silver_customer_current')
    # Facts can legitimately refer to a deleted customer. Validate against all known identities.
    known_customers = history.select('customer_id').distinct()
    products = clean('products').withColumn('status', F.upper('status'))
    for c in ['list_price','cost_price']:
        products = cast_col(products, c, 'decimal(18,2)')
    products = quality('products', products, ['product_id'], {
        'id_required': not_blank('product_id'), 'name_required': not_blank('product_name'),
        'price_invalid': F.col('list_price') >= 0, 'cost_invalid': F.col('cost_price') >= 0,
        'status_invalid': allowed('status',['ACTIVE','INACTIVE'])})
    orders = clean('orders').withColumn('order_ts', timestamp('order_ts')).withColumn('order_status', F.upper('order_status'))
    orders = orders.withColumn('shipping_city', F.initcap('shipping_city')).join(
        known_customers.withColumn('_customer_found', F.lit(True)), 'customer_id', 'left')
    orders = quality('orders', orders, ['order_id'], {
        'id_required': not_blank('order_id'), 'customer_fk': F.col('_customer_found'),
        'timestamp_invalid': F.col('order_ts').isNotNull(),
        'status_invalid': allowed('order_status',['PLACED','SHIPPED','DELIVERED','CANCELLED']),
        'channel_invalid': allowed('channel',['ONLINE','STORE']), 'shipping_city_required': not_blank('shipping_city')})
    items = clean('order_items')
    for c,dtype in [('quantity','int'),('unit_price','decimal(18,2)'),('discount_pct','decimal(7,4)'),('line_amount','decimal(18,2)')]:
        items = cast_col(items,c,dtype)
    items = items.join(orders.select('order_id').withColumn('_order_found',F.lit(True)), 'order_id','left')
    items = items.join(products.select('product_id').withColumn('_product_found',F.lit(True)), 'product_id','left')
    expected = F.round(F.col('quantity') * F.col('unit_price') * (F.lit(1)-F.col('discount_pct')),2)
    items = quality('order_items', items, ['order_item_id'], {
        'id_required': not_blank('order_item_id'), 'order_fk': F.col('_order_found'), 'product_fk': F.col('_product_found'),
        'quantity_invalid': F.col('quantity') > 0, 'price_invalid': F.col('unit_price') >= 0,
        'discount_invalid': F.col('discount_pct').between(0,1),
        'line_amount_invalid': (F.col('line_amount') >= 0) & (F.abs(F.col('line_amount')-expected) <= F.lit('0.01').cast('decimal(18,2)'))})
    payments = clean('payments').withColumn('payment_ts',timestamp('payment_ts')).withColumn('payment_status',F.upper('payment_status'))
    payments = cast_col(payments,'amount','decimal(18,2)').join(
        orders.select('order_id').withColumn('_order_found',F.lit(True)), 'order_id','left')
    quality('payments',payments,['payment_id'],{
        'id_required':not_blank('payment_id'), 'order_fk':F.col('_order_found'), 'amount_invalid':F.col('amount') >= 0,
        'timestamp_invalid':F.col('payment_ts').isNotNull(),
        'status_invalid':allowed('payment_status',['SUCCESS','FAILED','REFUNDED']),
        'mode_invalid':allowed('payment_mode',['CARD','UPI','COD','NETBANKING','WALLET'])})
    dirty = clean('dirty_daily_sales').withColumn('city',F.initcap('city')).withColumn('customer_name',F.initcap('customer_name'))
    dirty = (dirty.withColumn('payment_mode',F.upper('payment_mode')).withColumn('customer_status',F.upper('customer_status'))
             .withColumn('order_date',parse_date('order_date')))
    dirty = cast_col(cast_col(dirty,'quantity','int'),'unit_price','decimal(18,2)')
    dirty = dirty.join(known_customers.withColumn('_customer_found',F.lit(True)), 'customer_id','left')
    dirty = dirty.join(products.select('product_name').distinct().withColumn('_product_found',F.lit(True)), 'product_name','left')
    quality('dirty_daily_sales',dirty,['order_id'],{
        'id_required':not_blank('order_id'), 'customer_fk':F.col('_customer_found'), 'product_fk':F.col('_product_found'),
        'quantity_invalid':F.col('quantity') > 0, 'price_invalid':F.col('unit_price') >= 0,
        'date_invalid':F.col('order_date').isNotNull(),
        'payment_mode_invalid':allowed('payment_mode',['UPI','CARD','COD','NETBANKING','WALLET']),
        'status_invalid':allowed('customer_status',['ACTIVE','INACTIVE'])}, DIRTY_REJECT_RATE)
    inventory = clean('inventory_snapshot').withColumn('snapshot_date',parse_date('snapshot_date'))
    inventory = cast_col(inventory,'stock_qty','int').join(
        products.select('product_id').withColumn('_product_found',F.lit(True)), 'product_id','left')
    quality('inventory_snapshot',inventory,['product_id','warehouse_id','snapshot_date'],{
        'product_fk':F.col('_product_found'), 'warehouse_required':not_blank('warehouse_id'),
        'stock_invalid':F.col('stock_qty') >= 0, 'date_invalid':F.col('snapshot_date').isNotNull()})
    skew = cast_col(clean('skew_sales'),'amount','decimal(18,2)').withColumn('city',F.initcap('city'))
    quality('skew_sales',skew,['txn_id'],{'id_required':not_blank('txn_id'),'city_required':not_blank('city'),'amount_invalid':F.col('amount') >= 0})
    click = read('bronze_clickstream').withColumn('event_ts',F.try_to_timestamp('event_ts',F.lit("yyyy-MM-dd'T'HH:mm:ss")))
    click = click.join(known_customers.withColumn('_customer_found',F.lit(True)), 'customer_id','left')
    click = quality('clickstream',click,['event_id'],{
        'event_id_required':not_blank('event_id'),'customer_fk':F.col('_customer_found'),
        'timestamp_invalid':F.col('event_ts').isNotNull(),'device_required':not_blank('device.type'),
        'items_invalid':F.col('items').isNotNull() & F.forall('items', lambda x: x.product_id.isNotNull() & (x.qty > 0))})
    exploded = click.select('event_id',F.posexplode('items').alias('item_position','item')).select(
        'event_id','item_position',F.col('item.product_id').alias('product_id'),F.col('item.qty').alias('quantity'))
    require(exploded.join(products,'product_id','left_anti').count() == 0,'Clickstream product FK failed')
    save(exploded,'silver_clickstream_items')
    streaming = read('bronze_streaming_orders')
    streaming = streaming.withColumn('event_ts',F.try_to_timestamp('event_ts',F.lit("yyyy-MM-dd'T'HH:mm:ss")))
    streaming = cast_col(streaming,'amount','decimal(18,2)').join(known_customers.withColumn('_customer_found',F.lit(True)), 'customer_id','left')
    quality('streaming_orders',streaming,['event_id'],{
        'event_id_required':not_blank('event_id'),'order_id_required':not_blank('order_id'),
        'customer_fk':F.col('_customer_found'),'timestamp_invalid':F.col('event_ts').isNotNull(),
        'amount_invalid':F.col('amount') >= 0,'status_invalid':allowed('status',['PLACED','SHIPPED','DELIVERED','CANCELLED'])})
execute('silver',silver)

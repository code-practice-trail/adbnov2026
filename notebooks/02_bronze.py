# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze: unmodified values and source provenance
# MAGIC One immutable raw snapshot per run; retries replace only their own run partition.
# COMMAND ----------
# MAGIC %run ./_common
# COMMAND ----------
def bronze():
    source = Path(manifest_root())
    contracts = json.loads((source / 'contracts.json').read_text(encoding='utf-8'))
    for name, contract in contracts.items():
        schema = T.StructType([T.StructField(c, T.StringType()) for c in contract['columns']])
        df = (spark.read.schema(schema).option('header', True).option('mode', 'FAILFAST')
              .csv(str(source / contract['file']))
              .select('*', F.col('_metadata.file_path').alias('_source_file')))
        df = df.withColumn('_row_hash', F.sha2(F.to_json(F.struct(*contract['columns'])), 256))
        require(df.count() == contract['rows'], name + ': bronze row count mismatch')
        save(df.withColumn('_ingested_at', F.current_timestamp()), 'bronze_' + name)
    event_schema = 'event_id string, customer_id string, event_ts string, device struct<type:string,os:string>, items array<struct<product_id:string,qty:int>>, attributes map<string,string>'
    click = (spark.read.schema(event_schema).option('mode', 'FAILFAST').json(str(source / 'clickstream_nested.json'))
             .select('*', F.col('_metadata.file_path').alias('_source_file')))
    click = click.withColumn('_row_hash', F.sha2(F.to_json(F.struct(*[c for c in click.columns if not c.startswith('_')])), 256))
    save(click.withColumn('_ingested_at', F.current_timestamp()), 'bronze_clickstream')
    # File stream: explicit schema, durable checkpoint, availableNow for serverless.
    # foreachBatch MERGE is replay-safe if a crash happens after commit but before checkpoint.
    event_schema = 'event_id string, order_id string, customer_id string, event_ts string, amount string, status string'
    stream = (spark.readStream.schema(event_schema).option('mode', 'FAILFAST')
              .json(str(source / 'streaming_orders'))
              .select('*', F.col('_metadata.file_path').alias('_source_file')))
    target = table('bronze_stream_events')
    spark.sql(f'''CREATE TABLE IF NOT EXISTS {target} (event_id STRING, order_id STRING,
        customer_id STRING, event_ts STRING, amount STRING, status STRING, _source_file STRING,
        _row_hash STRING) USING DELTA''')
    def merge_events(batch, batch_id):
        batch = batch.withColumn('_row_hash', F.sha2(F.to_json(F.struct('event_id','order_id','customer_id','event_ts','amount','status')), 256))
        batch = batch.dropDuplicates(['_row_hash'])
        # Hash merge preserves conflicting event IDs for downstream rejection rather than silently choosing.
        DeltaTable.forName(spark, target).alias('t').merge(batch.alias('s'), 't._row_hash=s._row_hash').whenNotMatchedInsertAll().execute()
    checkpoint = f'{LANDING}/checkpoints/stream_events_v1'
    query = (stream.writeStream.foreachBatch(merge_events)
             .option('checkpointLocation', checkpoint).trigger(availableNow=True).start())
    query.awaitTermination()
    # Freeze the stream's current state so downstream tasks share one run snapshot.
    save(spark.table(target), 'bronze_streaming_orders')
execute('bronze', bronze)

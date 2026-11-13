# Databricks notebook source
# MAGIC %md
# MAGIC # ShopStream shared configuration and controls
# MAGIC Import this notebook beside the numbered notebooks. Run `00_run_all` for a complete execution.

# COMMAND ----------
import csv
import hashlib
import json
import os
import re
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from functools import reduce
from pyspark.sql import functions as F, types as T, Window
from delta.tables import DeltaTable


def parameter(name, default):
    try:
        return dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, str(default))
        return dbutils.widgets.get(name)


CATALOG = parameter('catalog', 'wsadbnov')
SCHEMA = parameter('schema', 'shopstream_dev')
VOLUME = parameter('volume', 'shopstream_landing')
SOURCE_PATH = parameter('source_path', '')
RUN_ID = parameter('run_id', str(uuid.uuid4()))
MAX_REJECT_RATE = float(parameter('max_reject_rate', '0.10'))
DIRTY_REJECT_RATE = float(parameter('dirty_reject_rate', '0.65'))
for value in (CATALOG, SCHEMA, VOLUME):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', value):
        raise ValueError('Catalog, schema and volume must be simple SQL identifiers')
if not re.fullmatch(r'[A-Za-z0-9_-]{1,100}', RUN_ID):
    raise ValueError('Invalid run_id')
if not (0 <= MAX_REJECT_RATE <= 1 and 0 <= DIRTY_REJECT_RATE <= 1):
    raise ValueError('Reject rates must be between zero and one')
spark.sql("SET TIME ZONE 'UTC'")
NS = f'`{CATALOG}`.`{SCHEMA}`'
LANDING = f'/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}'
# UTC is explicit in parser patterns and run metadata. Source timestamps have no timezone;
# this training dataset is interpreted as UTC, not converted from an inferred local zone.


def table(name):
    if not re.fullmatch(r'[a-z][a-z0-9_]*', name):
        raise ValueError(name)
    return f'{NS}.`{name}`'


def read(name):
    return spark.table(table(name)).where(F.col('_run_id') == RUN_ID)


def save(df, name):
    # Atomic replacement of this run only makes task retries idempotent.
    out = df.withColumn('_run_id', F.lit(RUN_ID))
    (out.write.format('delta').mode('overwrite')
        .option('replaceWhere', f"_run_id = '{RUN_ID}'")
        .saveAsTable(table(name)))


def scalar(query):
    return spark.sql(query).first()[0]


def audit(stage, status, detail=''):
    # Deliberately keep row values and PII out of operational logs.
    df = spark.createDataFrame(
        [(RUN_ID, stage, status, datetime.now(timezone.utc).isoformat(), detail[:2000])],
        'run_id string, stage string, status string, recorded_at string, detail string')
    df.createOrReplaceTempView('_audit_update')
    spark.sql(f'''MERGE INTO {table('ops_run_audit')} t USING _audit_update s
        ON t.run_id=s.run_id AND t.stage=s.stage
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *''')


def execute(stage, function):
    published = spark.table(table('ops_publication')).where(
        (F.col('pipeline') == 'shopstream') & (F.col('run_id') == RUN_ID)).limit(1).count()
    if published:
        require(stage == 'validate_publish', 'Published run IDs are immutable; start a new complete run')
        audit(stage, 'SUCCEEDED', 'Publication already committed; idempotent retry')
        return
    audit(stage, 'RUNNING')
    try:
        function()
        audit(stage, 'SUCCEEDED')
    except Exception as exc:
        audit(stage, 'FAILED', type(exc).__name__)
        raise


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def cast_col(df, column, dtype):
    return df.withColumn(column, F.expr(f'try_cast(`{column}` as {dtype})'))


def parse_date(column):
    return F.coalesce(*[F.try_to_timestamp(F.col(column), F.lit(fmt)).cast('date')
                        for fmt in ('yyyy-MM-dd', 'yyyy/MM/dd', 'dd-MM-yyyy')])


def timestamp(column):
    return F.try_to_timestamp(F.col(column), F.lit('yyyy-MM-dd HH:mm:ss'))


def not_blank(column):
    return F.col(column).isNotNull() & (F.length(F.trim(F.col(column).cast('string'))) > 0)


def allowed(column, values):
    return F.col(column).isin(*values)


def quality(name, df, keys, rules, reject_rate=None):
    # Preserve every rejected occurrence. Exact duplicates count as rejected occurrences.
    # Different payloads at one natural key are ambiguous and block the run.
    raw = read('bronze_' + name)
    total = raw.count()
    require(total > 0, f'{name}: empty source')
    errors = F.filter(F.array(*[
        F.when(~F.coalesce(condition, F.lit(False)), F.lit(reason))
        for reason, condition in rules.items()]), lambda x: x.isNotNull())
    df = df.withColumn('_errors', errors)
    valid = df.where(F.size('_errors') == 0)
    conflicts = valid.groupBy(*keys).agg(F.countDistinct('_row_hash').alias('n')).where('n > 1')
    require(conflicts.limit(1).count() == 0, f'{name}: conflicting natural keys; inspect bronze')
    w = Window.partitionBy(*keys).orderBy('_row_hash', '_source_file')
    ranked = valid.withColumn('_rank', F.row_number().over(w))
    duplicates = ranked.where('_rank > 1').drop('_rank').withColumn('_errors', F.array(F.lit('duplicate_key')))
    rejected = df.where(F.size('_errors') > 0).unionByName(duplicates)
    accepted = ranked.where('_rank = 1').drop('_rank', '_errors')
    # Quarantine stores raw source values, plus stable codes; do not log these rows.
    q = rejected.select('_row_hash', '_source_file', '_errors').join(
        raw.select('_row_hash', F.to_json(F.struct(*[F.col(c) for c in raw.columns
                  if not c.startswith('_')])).alias('raw_json')).dropDuplicates(['_row_hash']),
        '_row_hash', 'left')
    save(q, 'quarantine_' + name)
    accepted_count, rejected_count = accepted.count(), rejected.count()
    require(accepted_count + rejected_count == total, f'{name}: row conservation failed')
    metric = spark.createDataFrame([(name, total, accepted_count, rejected_count)],
                                  'dataset string, source_count long, accepted_count long, rejected_count long')
    save(metric, 'quality_' + name)
    limit = MAX_REJECT_RATE if reject_rate is None else reject_rate
    require(rejected_count / total <= limit, f'{name}: rejection threshold exceeded')
    save(accepted, 'silver_' + name)
    return read('silver_' + name)


def fk(df, column, parent, parent_column=None):
    return df.join(parent.select(F.col(parent_column or column).alias(column)).distinct()
                   .withColumn('_parent_exists', F.lit(True)), column, 'left')


def manifest_root():
    path = Path(LANDING) / 'control' / f'{RUN_ID}.json'
    return json.loads(path.read_text(encoding='utf-8'))['snapshot_path']

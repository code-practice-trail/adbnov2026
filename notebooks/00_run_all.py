# Databricks notebook source
# MAGIC %md
# MAGIC # ShopStream — run the complete pipeline
# MAGIC Import this folder with all sibling notebooks into Azure Databricks.
# MAGIC Set `catalog`, `schema`, `source_path` and optional quality thresholds below.
# MAGIC `source_path` must contain the bundled data directory including manifest.json and contracts.json.
# MAGIC Use a fresh run ID per complete execution; stage retries retain the existing run ID.
# MAGIC Do not run this launcher concurrently with the scheduled Job against the same schema.
# COMMAND ----------
import uuid

def widget(name, default):
    try:
        return dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name)

args = {
    'catalog':widget('catalog','wsadbnov'),
    'schema':widget('schema','shopstream_dev'),
    'volume':widget('volume','shopstream_landing'),
    'source_path':widget('source_path',''),
    'max_reject_rate':widget('max_reject_rate','0.10'),
    'dirty_reject_rate':widget('dirty_reject_rate','0.65'),
    'run_id':str(uuid.uuid4()),
}
for stage in ['01_preflight','02_bronze','03_silver','04_gold','05_validate_publish']:
    print('Starting',stage,'run',args['run_id'])
    dbutils.notebook.run('./'+stage,3600,args)
print('Published run',args['run_id'])

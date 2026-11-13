# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Preflight and immutable source landing
# MAGIC Source must be the supplied dataset directory in Workspace Files or a Unity Catalog Volume.
# MAGIC A checked-in SHA-256 manifest prevents accidental source substitution or drift.
# COMMAND ----------
# MAGIC %run ./_common
# COMMAND ----------
def bootstrap():
    require(bool(SOURCE_PATH), 'Set source_path to the imported data directory or a /Volumes path')
    source = Path(SOURCE_PATH)
    manifest = json.loads((source / 'manifest.json').read_text(encoding='utf-8'))
    require(len(manifest) == 17, 'Expected supplied dataset manifest (17 files including README)')
    for item in manifest:
        relative = Path(item['path'])
        require(not relative.is_absolute() and '..' not in relative.parts, 'Unsafe manifest path')
        payload = (source / relative).read_bytes()
        require(hashlib.sha256(payload).hexdigest() == item['sha256'], 'Source checksum mismatch: ' + item['path'])
        require(len(payload) == item['bytes'], 'Source length mismatch')
    contracts = json.loads((source / 'contracts.json').read_text(encoding='utf-8'))
    for name, contract in contracts.items():
        with (source / contract['file']).open(newline='', encoding='utf-8-sig') as stream:
            reader = csv.reader(stream)
            require(next(reader) == contract['columns'], name + ': header drift')
            rows = list(reader)
            require(all(len(row) == len(contract['columns']) for row in rows), name + ': malformed CSV')
            require(len(rows) == contract['rows'], name + ': unexpected source count')
    digest = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
    target = Path(LANDING) / 'snapshots' / digest
    target.mkdir(parents=True, exist_ok=True)
    for item in manifest:
        dest = target / item['path']
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            require(hashlib.sha256(dest.read_bytes()).hexdigest() == item['sha256'], 'Immutable landing changed')
        else:
            shutil.copyfile(source / item['path'], dest)
    (target / 'contracts.json').write_text(json.dumps(contracts), encoding='utf-8')
    control = Path(LANDING) / 'control'
    control.mkdir(exist_ok=True)
    (control / f'{RUN_ID}.json').write_text(json.dumps({'snapshot_path': str(target), 'sha256': digest}), encoding='utf-8')
    audit('source_manifest', 'SUCCEEDED', digest)

spark.sql(f'CREATE SCHEMA IF NOT EXISTS {NS}')
spark.sql(f'CREATE VOLUME IF NOT EXISTS {NS}.`{VOLUME}`')
spark.sql(f'''CREATE TABLE IF NOT EXISTS {table('ops_run_audit')}
    (run_id STRING, stage STRING, status STRING, recorded_at STRING, detail STRING) USING DELTA''')
spark.sql(f'''CREATE TABLE IF NOT EXISTS {table('ops_publication')}
    (pipeline STRING, run_id STRING, published_at TIMESTAMP) USING DELTA''')
execute('preflight', bootstrap)

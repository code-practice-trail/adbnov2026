from pathlib import Path
root=Path.cwd()
p=root/'notebooks/_common.py'
text=p.read_text()
text=text.replace("def execute(stage, function):\n    audit(stage, 'RUNNING')", "def execute(stage, function):\n    published = spark.table(table('ops_publication')).where(\n        (F.col('pipeline') == 'shopstream') & (F.col('run_id') == RUN_ID)).limit(1).count()\n    if published:\n        require(stage == 'validate_publish', 'Published run IDs are immutable; start a new complete run')\n        audit(stage, 'SUCCEEDED', 'Publication already committed; idempotent retry')\n        return\n    audit(stage, 'RUNNING')")
p.write_text(text,encoding='utf-8')
p=root/'tools/build_import.py'
text=p.read_text(encoding='utf-8-sig').replace("'nbformat_minor':5", "'nbformat_minor':4")
p.write_text(text,encoding='utf-8')
archive=root/'artifacts/build_provenance'
archive.mkdir(exist_ok=True)
for name in ['original_seed_script.py','finalize_sources.py','package_release.py']:
    src=root/'tools'/name
    if src.exists():
        src.rename(archive/name)

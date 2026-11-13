from pathlib import Path
for p in Path('notebooks').glob('*.py'):
    text=p.read_text(encoding='utf-8-sig').replace("'dirty_reject_rate', '0.60'", "'dirty_reject_rate', '0.65'").replace("'dirty_reject_rate','0.60'", "'dirty_reject_rate','0.65'")
    if p.name=='_common.py':
        text=text.replace("NS = f'`{CATALOG}`.`{SCHEMA}`'", "spark.sql(\"SET TIME ZONE 'UTC'\")\nNS = f'`{CATALOG}`.`{SCHEMA}`'")
    p.write_text(text,encoding='utf-8')
p=Path('databricks.yml')
text=p.read_text(encoding='utf-8-sig').replace("    - tools/**\n",'').replace("    - artifacts/**\n",'').replace("    - '*.zip'\n",'')
p.write_text(text,encoding='utf-8')
p=Path('resources/shopstream.job.yml');p.write_text(p.read_text(encoding='utf-8-sig'),encoding='utf-8')

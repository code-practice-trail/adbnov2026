from pathlib import Path
p=Path('notebooks/05_validate_publish.py')
text=p.read_text().replace('CREATE OR REPLACE VIEW','CREATE VIEW IF NOT EXISTS')
text=text.replace('# Create/reconcile views before advancing pointer. They stay empty on a first failed run.', '# Stable view definitions preserve object identity and consumer grants across runs.\n    # Schema changes require a reviewed ALTER VIEW migration before publication.')
p.write_text(text,encoding='utf-8')
p=Path('docs/OPERATIONS.md');text=p.read_text()
text=text.replace('View definitions are stable for this release.', 'View definitions are stable for this release and are created only if absent, preserving their existing grants. A future schema change requires a reviewed ALTER VIEW migration; do not drop/recreate granted views. See the [Databricks ALTER VIEW reference](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-alter-view).')
p.write_text(text,encoding='utf-8')

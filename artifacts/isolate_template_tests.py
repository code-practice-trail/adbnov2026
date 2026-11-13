from pathlib import Path
root=Path.cwd().resolve()
dest=root/'examples/original_template/tests'
dest.mkdir(parents=True,exist_ok=True)
for name in ['conftest.py','sample_taxis_test.py']:
    source=(root/'tests'/name).resolve()
    target=(dest/name).resolve()
    assert source.is_relative_to(root) and target.is_relative_to(root)
    source.rename(target)
p=root/'pyproject.toml'
text=p.read_text(encoding='utf-8-sig')
text+='\n[tool.pytest.ini_options]\ntestpaths = ["tests"]\n'
p.write_text(text,encoding='utf-8')
p=root/'docs/OPERATIONS.md'
text=p.read_text().replace('The original generated `pyproject.toml`, taxi code and sample resource files are preserved but not used', 'The original taxi code and sample resource files are preserved but not used')
text+='\nThe generated taxi-test fixtures and Databricks Connect auto-start configuration are preserved under `examples/original_template/tests`. Pytest discovery is scoped to the active `tests/` directory so local checks cannot start the unrelated taxi workflow.\n'
p.write_text(text,encoding='utf-8')
p=root/'tools/build_import.py'
text=p.read_text().replace("ROOT/'databricks.yml', ROOT/'resources/shopstream.job.yml'", "ROOT/'databricks.yml', ROOT/'pyproject.toml', ROOT/'resources/shopstream.job.yml'")
p.write_text(text,encoding='utf-8')
p=root/'databricks.yml'
text=p.read_text().replace('    - artifacts/**\n','    - artifacts/**\n    - examples/**\n')
p.write_text(text,encoding='utf-8')

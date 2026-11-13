"""Create standard Jupyter notebooks and a portable import archive without third-party libraries."""
import json
import zipfile
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'artifacts'
IPYNB=OUT/'ipynb'
IPYNB.mkdir(parents=True,exist_ok=True)
for source in sorted((ROOT/'notebooks').glob('*.py')):
    text=source.read_text(encoding='utf-8-sig')
    blocks=text.split('# COMMAND ----------')
    cells=[]
    for index,block in enumerate(blocks):
        lines=block.strip().splitlines()
        if index==0 and lines and lines[0]=='# Databricks notebook source':
            lines=lines[1:]
        while lines and not lines[0].strip():
            lines.pop(0)
        if not lines:
            continue
        converted=[line[len('# MAGIC '):] if line.startswith('# MAGIC ') else ('' if line=='# MAGIC' else line) for line in lines]
        markdown=converted[0].strip()=='%md'
        if markdown:
            converted=converted[1:]
        cell={'cell_type':'markdown' if markdown else 'code','metadata':{},'source':[line+'\n' for line in converted]}
        if not markdown:
            cell.update(execution_count=None,outputs=[])
        cells.append(cell)
    notebook={'cells':cells,'metadata':{'kernelspec':{'display_name':'Python 3','language':'python','name':'python3'},'language_info':{'name':'python'}},'nbformat':4,'nbformat_minor':4}
    (IPYNB/(source.stem+'.ipynb')).write_text(json.dumps(notebook,indent=2)+'\n',encoding='utf-8')
archive=OUT/'shopstream_databricks_import.zip'
with zipfile.ZipFile(archive,'w',compression=zipfile.ZIP_DEFLATED) as bundle:
    for folder in [ROOT/'notebooks',IPYNB,ROOT/'data',ROOT/'docs']:
        for path in sorted(folder.rglob('*')):
            if path.is_file():
                arcname=Path('ipynb')/path.name if folder==IPYNB else path.relative_to(ROOT)
                bundle.write(path,str(arcname))
    for path in [ROOT/'README.md', ROOT/'databricks.yml', ROOT/'pyproject.toml', ROOT/'resources/shopstream.job.yml', ROOT/'tests/test_shopstream_release.py', ROOT/'tools/build_import.py']:
        bundle.write(path,str(path.relative_to(ROOT)))
print('Packaged',len(list(IPYNB.glob('*.ipynb'))),'notebooks:',archive)

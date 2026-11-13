from pathlib import Path
import csv, json, hashlib, shutil
root=Path.cwd()
source=Path('//TENALI/Users/ravis/Documents/Azure Classes/Mine/Pyspark_excecises/pyspark_databricks_datasets')
(root/'tools').mkdir(exist_ok=True)
(root/'notebooks').mkdir(exist_ok=True)
(root/'data').mkdir(exist_ok=True)
manifest=[]
for p in sorted(source.rglob('*')):
    if p.is_file():
        rel=p.relative_to(source)
        dest=root/'data'/rel
        dest.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(p,dest)
        manifest.append({'path':rel.as_posix(),'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size})
(root/'data/manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
contracts={}
keys={'customers':['customer_id'],'products':['product_id'],'orders':['order_id'],'order_items':['order_item_id'],'payments':['payment_id'],'city_region_lookup':['city'],'customer_updates_cdc':['customer_id','update_ts'],'dirty_daily_sales':['order_id'],'inventory_snapshot':['product_id','warehouse_id','snapshot_date'],'skew_sales':['txn_id']}
for p in sorted(source.glob('*.csv')):
    with p.open(newline='',encoding='utf-8-sig') as f:
        reader=csv.DictReader(f)
        contracts[p.stem]={'file':p.name,'columns':reader.fieldnames,'keys':keys[p.stem],'rows':sum(1 for _ in reader)}
(root/'data/contracts.json').write_text(json.dumps(contracts,indent=2)+'\n',encoding='utf-8')
for name in ['orders','order_items','payments']:
    with (source/f'{name}.csv').open() as f:
        rows=list(csv.DictReader(f))
    print(name,len(rows),len({r[keys[name][0]] for r in rows}))
print('Copied',len(manifest),'original files; contracts:',len(contracts))

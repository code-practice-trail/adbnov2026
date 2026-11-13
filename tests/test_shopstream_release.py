"""Independent release checks against the exact supplied retail data (standard library only)."""
import ast
import csv
import hashlib
import json
import unittest
from collections import Counter, defaultdict
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'data'


def rows(name):
    with (DATA / (name + '.csv')).open(newline='',encoding='utf-8-sig') as stream:
        return list(csv.DictReader(stream))


class SourceReleaseTests(unittest.TestCase):
    def test_original_files_match_manifest(self):
        manifest = json.loads((DATA/'manifest.json').read_text())
        self.assertEqual(len(manifest),17)
        for item in manifest:
            with self.subTest(file=item['path']):
                payload=(DATA/item['path']).read_bytes()
                self.assertEqual(len(payload),item['bytes'])
                self.assertEqual(hashlib.sha256(payload).hexdigest(),item['sha256'])

    def test_exact_csv_contracts(self):
        contracts=json.loads((DATA/'contracts.json').read_text())
        self.assertEqual(len(contracts),10)
        for name,contract in contracts.items():
            with self.subTest(dataset=name):
                actual=rows(name)
                self.assertEqual(list(actual[0]),contract['columns'])
                self.assertEqual(len(actual),contract['rows'])
                # Only the dirty exercise intentionally repeats a key.
                counts=Counter(tuple(r[k] for k in contract['keys']) for r in actual)
                self.assertEqual(sum(v-1 for v in counts.values()),int(name=='dirty_daily_sales'))

    def test_financials_and_foreign_keys(self):
        customers={r['customer_id'] for r in rows('customers')}
        products={r['product_id'] for r in rows('products')}
        orders={r['order_id']:r for r in rows('orders')}
        self.assertEqual(len(orders),126)
        self.assertTrue(all(r['customer_id'] in customers for r in orders.values()))
        totals=defaultdict(Decimal)
        for r in rows('order_items'):
            self.assertIn(r['order_id'],orders)
            self.assertIn(r['product_id'],products)
            calculated=(Decimal(r['quantity'])*Decimal(r['unit_price'])*(1-Decimal(r['discount_pct']))).quantize(Decimal('.01'),rounding=ROUND_HALF_UP)
            self.assertEqual(calculated,Decimal(r['line_amount']))
            totals[r['order_id']]+=calculated
        self.assertEqual(len(rows('order_items')),259)
        recorded=defaultdict(Decimal)
        for r in rows('payments'):
            self.assertIn(r['order_id'],orders)
            recorded[r['order_id']]+=Decimal(r['amount'])
        self.assertEqual(dict(totals),dict(recorded))

    def test_cdc_release_and_lifecycle(self):
        state={r['customer_id']:dict(r) for r in rows('customers')}
        counts=Counter({k:1 for k in state})
        for change in sorted(rows('customer_updates_cdc'),key=lambda r:r['update_ts']):
            key=change['customer_id']
            if change['op']=='I':
                self.assertNotIn(key,state)
                state[key]={}
            else:
                self.assertIn(key,state)
            state[key].update(change)
            counts[key]+=1
        self.assertEqual(len(state),13)
        self.assertEqual(sum(counts.values()),17)
        self.assertEqual(state['C004']['email'],'anjali.latest@example.com')
        self.assertEqual(state['C002']['status'],'INACTIVE')

    def test_nested_and_streaming_release(self):
        clicks=[json.loads(line) for line in (DATA/'clickstream_nested.json').read_text().splitlines() if line.strip()]
        self.assertEqual(len(clicks),3)
        self.assertEqual(sum(len(r['items']) for r in clicks),3)
        events=[json.loads(line) for p in sorted((DATA/'streaming_orders').glob('*.json')) for line in p.read_text().splitlines() if line.strip()]
        self.assertEqual(len(events),25)
        self.assertEqual(len({r['event_id'] for r in events}),25)
        self.assertTrue(all(Decimal(str(r['amount'])) >= 0 for r in events))

    def test_dirty_release_expected_valid_and_quarantine(self):
        from datetime import datetime
        accepted=[]
        rejected=[]
        seen=set()
        for r in rows('dirty_daily_sales'):
            try:
                if not r['customer_id'] or not r['payment_mode']:
                    raise ValueError('required')
                if int(r['quantity']) <= 0 or Decimal(r['unit_price']) < 0:
                    raise ValueError('amount')
                parsed=None
                for fmt in ('%Y-%m-%d','%Y/%m/%d','%d-%m-%Y'):
                    try:
                        parsed=datetime.strptime(r['order_date'],fmt)
                        break
                    except ValueError:
                        pass
                if parsed is None or r['order_id'] in seen:
                    raise ValueError('date or duplicate')
                accepted.append(r)
                seen.add(r['order_id'])
            except (ValueError,ArithmeticError):
                rejected.append(r)
        self.assertEqual([r['order_id'] for r in accepted],['O9001','O9002','O9007'])
        self.assertEqual(len(rejected),5)
        self.assertLessEqual(len(rejected)/8,Decimal('0.65'))

    def test_notebooks_compile_and_share_parameters(self):
        files=list((ROOT/'notebooks').glob('*.py'))
        self.assertGreaterEqual(len(files),7)
        for path in files:
            text=path.read_text(encoding='utf-8-sig')
            self.assertTrue(text.startswith('# Databricks notebook source'))
            ast.parse(text,filename=str(path))
            if path.name[0].isdigit() and not path.name.startswith('00'):
                self.assertIn('# MAGIC %run ./_common',text)
        common=(ROOT/'notebooks/_common.py').read_text()
        self.assertIn("'dirty_reject_rate', '0.65'",common)
        self.assertNotIn('dbfs:/mnt',common)


class PublicationSafetyTests(unittest.TestCase):
    def controls(self, published):
        from unittest.mock import MagicMock
        from types import SimpleNamespace
        tree=ast.parse((ROOT/'notebooks/_common.py').read_text())
        functions=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in {'execute','require'}]
        class Predicate:
            def __eq__(self, other):
                return self
            def __and__(self, other):
                return self
        spark_mock=MagicMock()
        spark_mock.table.return_value.where.return_value.limit.return_value.count.return_value=published
        audit_mock=MagicMock()
        scope={'spark':spark_mock,'F':SimpleNamespace(col=lambda c:Predicate()),'table':lambda n:n,
               'RUN_ID':'published-run','audit':audit_mock}
        exec(compile(ast.Module(body=functions,type_ignores=[]),'publication_controls','exec'),scope)
        return scope['execute'],audit_mock

    def test_a_published_run_cannot_be_rewritten(self):
        from unittest.mock import MagicMock
        execute,audit=self.controls(published=1)
        write=MagicMock()
        with self.assertRaisesRegex(AssertionError,'immutable'):
            execute('silver',write)
        write.assert_not_called()
        audit.assert_not_called()

    def test_publication_retry_does_not_rewrite_candidates(self):
        from unittest.mock import MagicMock
        execute,audit=self.controls(published=1)
        write=MagicMock()
        execute('validate_publish',write)
        write.assert_not_called()
        self.assertEqual(audit.call_args.args[1],'SUCCEEDED')

    def test_failed_work_is_recorded_and_propagated(self):
        from unittest.mock import MagicMock,call
        execute,audit=self.controls(published=0)
        write=MagicMock(side_effect=RuntimeError('restricted row detail'))
        with self.assertRaises(RuntimeError):
            execute('silver',write)
        self.assertEqual(audit.call_args_list,[call('silver','RUNNING'),call('silver','FAILED','RuntimeError')])


if __name__=='__main__':
    unittest.main()

from pathlib import Path
p=Path('tests/test_shopstream_release.py')
text=p.read_text(encoding='utf-8-sig')
extra='''

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
'''
text=text.replace("\n\nif __name__=='__main__':",extra+"\n\nif __name__=='__main__':")
p.write_text(text,encoding='utf-8')
for file in ['README.md','docs/OPERATIONS.md','docs/VALIDATION.md']:
    p=Path(file);text=p.read_text(encoding='utf-8-sig')
    text=text.replace('seven independent release tests','ten independent release and control tests')
    text=text.replace('Seven independent standard-library release tests','Ten independent standard-library release and publication-control tests')
    text=text.replace('seven independent','ten independent')
    p.write_text(text,encoding='utf-8')

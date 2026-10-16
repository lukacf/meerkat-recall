"""Synthetic exact native exporter envelope, including streaming shadows."""
import copy
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import preservation_plan as p
from test_preservation_plan import row, snapshot


def notes(tables):
    return {
        'source_store_path':'/synthetic/disposable-copy', 'namespace':'test', 'database':'test',
        'source_tables':{name:{'expected_rows':len(rows),'exported_rows':len(rows),
            'native_insert_batches':1 if rows else 0,'rows_without_string_space_id':0,
            'rows_without_string_id':0,'converted_decimal_values':0,'inexact_decimal_values':0,
            'converted_geometry_values':0,'output_table':name,'output_row_start':0} for name,rows in tables.items()},
        'partition_mapping':{}, 'binary_identity':{'sha256':'a'*64,'package_version':'0.1.0'},
        'source_identity':{'operator_declared_revision':'e40da5088940f58d30511f39531a4cd8ae7348b8','base_revision':'166866278dbf172d2398564a425d965a57f635f3'},
        'custody':'disposable second copy', 'compatibility':'elephant-snapshot-v1',
        'geometry_fidelity':'native parser may change precision', 'native_history':'current persisted rows',
        'normalization':'native records/dates to JSON', 'name_embedding_metadata_shadows':[]}


class ExportNotesTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.doc = row('doc','one',title='Original',text='unchanged')
        self.entity = row('entity','one',metadata={'name_embedding':[1.0]})
        self.tables = {'doc':[self.doc],'entity':[self.entity],'source_watch':[]}

    def tearDown(self):
        self.temp.cleanup()

    def build(self, export_notes, suffix=''):
        snap = self.root / ('source'+suffix)
        snap.write_bytes(p.canonical({**snapshot(self.tables),'export_notes':export_notes}))
        ids = self.root / ('idmap'+suffix)
        ids.write_bytes(p.canonical({'old_doc_id':'doc:one','class':'source_less','content_sha256':p.value_hash({'title':'Original','text':'unchanged'})})+b'\n')
        out = self.root / ('plan'+suffix)
        return out,p.inventory(snap,ids,out,space='household',snapshot_sha256=p.file_sha(snap),idmap_sha256=p.file_sha(ids))

    def test_exact_fifth_field_retains_shadow_and_source_identity(self):
        n=notes(self.tables)
        n['name_embedding_metadata_shadows']=[{'source_table':'entity','id':'entity:one','output_row_index':0,'metadata':{'name_embedding':[9.0]}}]
        out,plan=self.build(n)
        self.assertEqual(plan['export_notes']['fields']['source_identity'],n['source_identity'])
        self.assertEqual(plan['export_notes']['shadow_count'],1)
        db=sqlite3.connect(out/'inventory.sqlite')
        raw,digest=db.execute("SELECT lexical,lexical_sha256 FROM export_note_parts WHERE field='name_embedding_metadata_shadows'").fetchone()
        self.assertEqual(json.loads(raw),n['name_embedding_metadata_shadows'][0])
        self.assertEqual(p.sha(raw),digest)
        self.assertEqual(p.metadata(db,'export_notes'),plan['export_notes'])
        self.assertEqual(plan['tables']['source_watch'],0)
        db.close()

    def test_large_shadow_array_is_streamed_with_small_per_value_bound(self):
        tables={'entity':[row('entity',str(i)) for i in range(1000)]}
        n=notes(tables)
        n['name_embedding_metadata_shadows']=[{'source_table':'entity','id':f'entity:{i}','output_row_index':i,'metadata':{'name_embedding':[9.0]}} for i in range(1000)]
        path=self.root/'large';path.write_bytes(p.canonical({**snapshot(tables),'export_notes':n}))
        retained=[];stream=p.JsonStream(path,max_value_bytes=1024,chunk_bytes=13)
        stream.export_notes_sink=lambda field,key,value,raw,digest:retained.append((key,len(raw))) if field=='name_embedding_metadata_shadows' else None
        try:
            self.assertEqual(sum(1 for _ in stream.snapshot()),1000)
            self.assertEqual(stream.export_notes['shadow_count'],1000)
            self.assertEqual(len(retained),1000)
            self.assertTrue(all(size<1024 for _,size in retained))
            self.assertEqual(stream.digest.hexdigest(),p.file_sha(path))
        finally:
            stream.close()

    def test_unknown_missing_duplicate_and_wrong_counts_refuse(self):
        for i,mutate in enumerate([
            lambda n:n.update(unknown='no'),
            lambda n:n.pop('normalization'),
            lambda n:n['source_tables']['doc'].update(expected_rows=2),
            lambda n:n['source_tables']['doc'].update(expected_rows=2,exported_rows=2),
            lambda n:n['source_tables']['doc'].update(output_row_start=1),
            lambda n:n['binary_identity'].update(sha256='wrong')]):
            n=notes(self.tables);mutate(n)
            with self.subTest(i=i),self.assertRaises(p.Refused):self.build(n,str(i))
        raw=p.canonical({**snapshot(self.tables),'export_notes':notes(self.tables)})
        raw=raw.replace(b'"normalization":',b'"normalization":"first","normalization":')
        path=self.root/'duplicate';path.write_bytes(raw);stream=p.JsonStream(path)
        try:
            with self.assertRaisesRegex(p.Refused,'duplicate_or_invalid_member'):list(stream.snapshot())
        finally:stream.close()

    def test_shadow_wrong_original_id_or_duplicate_ordinal_refuses(self):
        shadow={'source_table':'entity','id':'entity:wrong','output_row_index':0,'metadata':{'name_embedding':[9.0]}}
        n=notes(self.tables);n['name_embedding_metadata_shadows']=[shadow]
        with self.assertRaisesRegex(p.Refused,'shadow_row_binding'):self.build(n,'wrong')
        shadow['id']='entity:one';n['name_embedding_metadata_shadows']=[shadow,copy.deepcopy(shadow)]
        with self.assertRaisesRegex(p.Refused,'duplicate_identity'):self.build(n,'duplicate')

    def test_partition_count_join_and_loss_review_are_explicit(self):
        tables={'chunk_embedding':[row('chunk_embedding','one'),row('chunk_embedding','two')]}
        n=notes(tables);definition=n['source_tables'].pop('chunk_embedding')
        definition.update(expected_rows=1,exported_rows=1,output_table='chunk_embedding',converted_decimal_values=1,inexact_decimal_values=1)
        for i in range(2):
            name=f'native_partition_{i}';n['source_tables'][name]={**definition,'output_row_start':i}
            n['partition_mapping'][name]={'target_table':'chunk_embedding','policy_id':'policy','level_bucket':'0','model':'model','dims':3}
        path=self.root/'partition';path.write_bytes(p.canonical({**snapshot(tables),'export_notes':n}));stream=p.JsonStream(path)
        try:
            self.assertEqual(len(list(stream.snapshot())),2)
            self.assertEqual(stream.export_notes['inexact_decimal_values'],2)
        finally:stream.close()
        n=notes(self.tables);n['source_tables']['entity']['converted_geometry_values']=1
        _,plan=self.build(n,'loss')
        self.assertIn('native_export_geometry_review',plan['runtime_blockers'])


if __name__=='__main__':unittest.main()

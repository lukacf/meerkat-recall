"""Metadata-only driver controls; never generate rows or execute a product process."""
import copy
import importlib.util
from pathlib import Path
import sys
import unittest

# Dependency is the existing source-only household shape constructor.
sys.path.insert(0, str(Path(__file__).parent))
spec = importlib.util.spec_from_file_location('archive_scale_driver', Path(__file__).with_name('synthetic_scale.py'))
s = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s)


def fixture(mode):
    tables = {'doc': 2, 'work_item': 1, 'work_item_archive': 0, 'migration_history': 1, 'knowledge_view': 0}
    archived = {t:n for t,n in tables.items() if t in s.OPERATIONAL_TABLES} if mode == 'managed_archive_v1' else {}
    source = {'tables':tables, 'snapshot_sha256':'a'*64, 'operational_disposition':mode}
    inventory = {'tables':archived, 'ordered_inventory_sha256':'b'*64, 'lexical_bytes':123}
    plan = {'operational_disposition':mode, 'inventory_sha256':'c'*64, 'plan_id':'d'*64,
            'operational_archive_inventory':inventory}
    baseline = {'operational_disposition':mode, 'journal_identity':'journal'}
    accounts = {}
    for t,n in tables.items():
        kind = 'recognized_global_retained_control' if t == 'migration_history' else 'managed_archive_exact_lexical' if t in archived else 'native_exact_projection'
        accounts[t] = {'rows':n, 'declared_storage':kind, 'disposition':kind if n else 'declared_empty'}
    report = {'operational_disposition':mode, 'source_tables':tables, 'archived_tables':archived,
        'copied_tables':{'doc':2, 'work_item':0 if archived else 1, 'work_item_archive':0, 'unpopulated_supported_table':0},
        'source_accounting':{'operational_disposition':mode, 'rows':sum(tables.values()), 'tables':accounts},
        'catchup_baseline':baseline, 'historical_work_holds':{'verified':True, 'rows':{'work_item':1, 'work_item_archive':0},
          'mode':'managed_archive_inert' if archived else 'native_historical_holds', 'native_holds':0 if archived else 1}}
    if archived:
        binding = {'snapshot_sha256':source['snapshot_sha256'], 'spool_sha256':plan['inventory_sha256'], 'plan_id':plan['plan_id'],
            'original_table_declarations':tables, 'parent_generation_sha256':None, 'source_space_id':'default', 'target_space_id':'household', 'journal_identity':'journal'}
        seal = {'table_counts':archived, 'redacted':False, 'ordered_inventory_sha256':'b'*64, 'binding_sha256':'e'*64,
                'bytes':4096, 'file_sha256':'f'*64}
        receipt = {'binding':binding, 'seal':seal, 'registration':{'binding_sha256':'e'*64,'snapshot_sha256':'a'*64}}
        report.update(operational_archive=receipt, archive_lexical_bytes=123, archive_ordered_inventory_sha256='b'*64)
        baseline['operational_archive'] = receipt
    return source, plan, report


class Accounting(unittest.TestCase):
    def test_native_and_archive_partition_count_once(self):
        for mode in s.OPERATIONAL_DISPOSITIONS:
            source,plan,report=fixture(mode)
            result=s.verify_disposition(mode,source,plan,report)
            self.assertEqual(result['source_rows'],4)
            self.assertEqual(result['archived_rows'],int(mode=='managed_archive_v1'))
            self.assertEqual(result['retained_global_rows'],1)

    def test_mode_null_double_native_missing_empty_and_lexical_mismatch_refuse(self):
        for mutation in [
            lambda r:r.update(operational_disposition=None),
            lambda r:r['copied_tables'].update(work_item=1),
            lambda r:r['archived_tables'].pop('work_item_archive'),
            lambda r:r['source_accounting']['tables'].pop('knowledge_view'),
            lambda r:r.update(archive_ordered_inventory_sha256='0'*64),
            lambda r:r['historical_work_holds'].update(native_holds=1),
        ]:
            source,plan,report=fixture('managed_archive_v1')
            # Separate references as serialized producer output does.
            report=copy.deepcopy(report);source=copy.deepcopy(source);plan=copy.deepcopy(plan)
            mutation(report)
            with self.assertRaises(ValueError):s.verify_disposition('managed_archive_v1',source,plan,report)
        source,plan,report=fixture('native');report['operational_archive']=None
        with self.assertRaises(ValueError):s.verify_disposition('native',source,plan,report)

    def test_complete_household_profile_unchanged_without_generation(self):
        counts=s.PROFILES[s.household.PROFILE]
        self.assertEqual(len(counts),40)
        self.assertEqual(counts['chunk_embedding'],179684)
        self.assertEqual(counts['doc_node'],187614)
        self.assertEqual(sum(n for t,n in counts.items() if t in s.OPERATIONAL_TABLES),761732)


if __name__ == '__main__':
    unittest.main()

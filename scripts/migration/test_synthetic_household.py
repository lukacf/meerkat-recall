"""Bounded source controls; execution is assigned to the existing sole lane."""
import hashlib
import base64
import unittest
import synthetic_household as full
import synthetic_scale as driver


def text(i):
    return full.document_text(i, driver.text)


def row(table, i):
    return full.row(table, i, full.COUNTS, driver.base, driver.rid, text, driver.STAMP, driver.row)


class FullShapeContract(unittest.TestCase):
    def test_retained_exact_count_and_lifecycle_contract(self):
        self.assertEqual(len(full.COUNTS), 40)
        self.assertEqual(sum(full.COUNTS.values()), 1207001)
        self.assertEqual(full.COUNTS['doc'] - full.LIVE_DOCS, 99)
        self.assertEqual(full.COUNTS['doc_revision'] - full.COUNTS['doc'], 301)
        self.assertNotIn('deleted_at', row('doc', full.LIVE_DOCS - 1))
        self.assertIn('deleted_at', row('doc', full.LIVE_DOCS))
        self.assertEqual(row('doc_revision', 7917)['doc'], driver.rid('doc', 300))
        self.assertEqual(row('doc_revision', 7917)['status'], 'superseded')
        self.assertEqual(sum(full.media_size(i) for i in range(86)), 145302409)

    def test_original_node_spans_and_assertion_hashes(self):
        for document in (0, 1, 7517, 7518, 7616):
            body = text(document).encode()
            spans = [full.node_span(i, text) for i in range(document, full.COUNTS['doc_node'], full.COUNTS['doc'])]
            self.assertEqual(spans[0]['start'], 0)
            self.assertEqual(spans[-1]['end'], len(body))
            for previous, following in zip(spans, spans[1:]):
                self.assertEqual(previous['end'], following['start'])
            for span in spans:
                body[span['start']:span['end']].decode('utf8')
        for i in (0, 7517, 7518, 11665):
            assertion = row('assertion', i)
            span = assertion['evidence']
            node = row('doc_node', i % full.LIVE_DOCS)
            self.assertEqual(span['node'], node['id'])
            self.assertEqual(span['revision'], node['revision'])
            self.assertEqual({k: span[k] for k in ('start', 'end')}, node['span'])
            body = text(i % full.LIVE_DOCS).encode()
            self.assertEqual(span['quote_hash'], hashlib.sha256(body[span['start']:span['end']]).hexdigest())

    def test_truth_conflict_and_evidence_references(self):
        for i in (0, 625):
            conflict = row('conflict_group', i)
            first, second = row('attribute', i), row('attribute', i + 626)
            self.assertEqual(conflict['claim_ids'], [first['id'], second['id']])
            self.assertEqual(first['subject'], second['subject'])
            self.assertEqual(first['attr_type'], second['attr_type'])
            self.assertNotEqual(first['value'], second['value'])
            truth = row('truth_slot', i)
            self.assertEqual(truth['current_values'][0]['claim_id'], first['id'])
            self.assertEqual(truth['conflict_group'], conflict['id'])
            expected_key = '::'.join((first['subject'], first['attr_type'], full.record_partition(first)))
            self.assertEqual(conflict['slot_key'], expected_key)
            self.assertEqual(truth['slot_key'], expected_key)
            self.assertEqual(truth['security_hash'], full.record_partition(first))
        last = row('truth_slot', 6225)
        self.assertEqual(last['current_values'][0]['claim_id'], driver.rid('attribute', 6851))

    def test_claims_match_original_assertion_evidence(self):
        for i in (0, 625, 626, 1251, 6876):
            claim, assertion = row('attribute', i), row('assertion', i)
            self.assertEqual(claim['subject'], assertion['subject_entity'])
            self.assertEqual(claim['attr_type'], assertion['predicate_id'])
            self.assertEqual(claim['value']['value'], assertion['object_literal'])
            self.assertEqual(claim['evidence'][0]['assertion'], assertion['id'])
        for i in (0, 2474):
            claim, assertion = row('rel', i), row('assertion', full.COUNTS['attribute'] + i)
            self.assertEqual(claim['in'], assertion['subject_entity'])
            self.assertEqual(claim['out'], assertion['object_entity'])
            self.assertEqual(claim['rel_type'], assertion['predicate_id'])
            self.assertEqual(claim['evidence'][0]['assertion'], assertion['id'])

    def test_truth_partition_matches_product_envelope_contract(self):
        base = driver.base('attribute', 0)
        # Source algorithm: policy bytes + signed i32 big-endian level, then
        # sorted labels/handling; JSON map of digest, sorted subjects, state.
        security = hashlib.sha256(b'fixture-policy' + b'\x00' * 4).hexdigest()
        expected = hashlib.sha256(driver.canonical({'security': security, 'subjects': [], 'subjects_state': 'unknown'})).hexdigest()
        self.assertEqual(full.record_partition(base), expected)
        changed = dict(base, subjects_state='complete')
        self.assertNotEqual(full.record_partition(changed), expected)

    def test_original_operational_and_vector_identity(self):
        counts = full.COUNTS
        cold = row('work_item_archive', counts['work_item_archive'] - 1)
        self.assertEqual(cold['original_id'], cold['record']['id'])
        self.assertNotEqual(cold['original_id'], driver.rid('work_item', counts['work_item'] - 1))
        vector = row('chunk_embedding', counts['chunk_embedding'] - 1)
        self.assertEqual(vector['node'], driver.rid('doc_node', 179683))
        self.assertEqual(len(vector['vector']), 3072)
        self.assertEqual(vector['dims'], 3072)
        control = row('migration_history', 0)
        self.assertNotIn('space_id', control)
        self.assertEqual(set(control), {'id', 'name', 'checksum', 'applied_at'})

    def test_bounded_media_bytes_and_doc_attachment(self):
        for size in (0, 1, 31, 32, 33, 257):
            value = full.media_fields(7, size)
            decoded = base64.b64decode(value['payload_base64'], validate=True)
            self.assertEqual(len(decoded), size)
            self.assertEqual(hashlib.sha256(decoded).hexdigest(), value['sha256'])
        with self.assertRaises(ValueError):
            full.media_fields(0, full.FIRST_MEDIA_BYTES + 1)
        attachment = row('doc', 7)['media_attachments'][0]
        self.assertEqual(attachment['location'], {'transport': 'managed_asset', 'asset_id': driver.rid('media_asset', 7)})
        self.assertEqual(attachment['size_bytes'], full.media_size(7))

    def test_event_metadata_matches_native_object_schema_without_losing_nested_values(self):
        # Real event schema requires the metadata root to be an object; nested
        # arrays/nulls still exercise exact native original-value preservation.
        for ordinal in (0, 1, 2, full.COUNTS['event'] - 1):
            metadata = row('event', ordinal)['metadata']
            self.assertIsInstance(metadata, dict)
            expected = {'original_values': [{'original_ordinal': ordinal}, None]} if ordinal % 3 == 0 else {}
            self.assertEqual(metadata, expected)

    def test_every_nonempty_constructor_has_bound_identity(self):
        for table, count in full.COUNTS.items():
            if not count or table == 'media_asset':
                continue  # Large inline bytes belong to admitted generation, not these controls.
            for i in {0, count - 1}:
                value = row(table, i)
                self.assertTrue(value['id'].startswith(table + ':'))
                if table != 'migration_history':
                    self.assertEqual(value['space_id'], 'default')
                driver.canonical(value)


if __name__ == '__main__':
    unittest.main()

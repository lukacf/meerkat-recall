#!/usr/bin/env python3
"""Bounded constructor selections for strict native round-trip controls.

This is deliberately not an import/adoption snapshot or a throughput fixture.
It retains first/last/boundary constructors and records every synthetic media
size substitution. Some references point outside this selection; it cannot
establish full graph closure. Execution belongs to the existing test owner.
"""
import argparse
import hashlib
import json
from pathlib import Path
import synthetic_household as full
import synthetic_scale as driver

MAX_ROWS = 128
MAX_ROW_BYTES = 1024 * 1024
MAX_TOTAL_BYTES = 16 * 1024 * 1024
MEDIA_TEST_BYTES = 257


def selected_ordinals():
    selected = {table: {0, count - 1} for table, count in full.COUNTS.items() if count}
    selected['entity'].update({1, 985, 986})  # Enforced endpoints of rel0 and rel2474.
    selected['doc'].update({85, 7517, 7518})
    selected['doc_revision'].update({7517, 7518, 7616, 7617})
    selected['doc_node'].update({7517, 7518, 7616, 7617})
    selected['assertion'].update({625, 626, 1251, 6876, 6877, 9351, 9352})
    selected['attribute'].update({625, 626, 1251})
    selected['truth_slot'].update({625, 626})
    return {table: sorted(ordinals) for table, ordinals in sorted(selected.items())}


def selected_row(table, ordinal):
    value = full.row(table, ordinal, full.COUNTS, driver.base, driver.rid,
                     lambda i: full.document_text(i, driver.text), driver.STAMP, driver.row) if table != 'media_asset' else driver.base(table, ordinal)
    substitutions = []
    if table == 'media_asset':
        value.update(full.media_fields(ordinal, MEDIA_TEST_BYTES))
        substitutions.append({'field': 'payload/size/hash', 'reason': 'bounded native byte preservation control',
                              'full_profile_decoded_bytes': full.media_size(ordinal), 'selected_decoded_bytes': MEDIA_TEST_BYTES})
    elif table == 'doc':
        for attachment in value.get('media_attachments', []):
            original = attachment['size_bytes']
            attachment['size_bytes'] = MEDIA_TEST_BYTES
            substitutions.append({'field': 'media_attachments.size_bytes', 'reason': 'matches bounded selected media',
                                  'full_profile_decoded_bytes': original, 'selected_decoded_bytes': MEDIA_TEST_BYTES})
    return value, substitutions


def write_selection(output):
    output = Path(output)
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    selection = selected_ordinals()
    driver.require(sum(map(len, selection.values())) <= MAX_ROWS, 'representative row count bound')
    declarations = []
    total = 0
    for table, ordinals in selection.items():
        for ordinal in ordinals:
            driver.require(0 <= ordinal < full.COUNTS[table], 'representative ordinal outside full profile')
            value, substitutions = selected_row(table, ordinal)
            encoded = driver.canonical(value) + b'\n'
            driver.require(len(encoded) <= MAX_ROW_BYTES, 'representative row byte bound')
            total += len(encoded)
            driver.require(total <= MAX_TOTAL_BYTES, 'representative total byte bound')
            filename = f'{table}-{ordinal}.json'
            driver.save(output / filename, value)
            declarations.append({'table': table, 'ordinal': ordinal, 'id': value['id'], 'path': filename,
                                 'bytes': len(encoded), 'sha256': hashlib.sha256(encoded).hexdigest(),
                                 'synthetic_substitutions': substitutions,
                                 'disposition': ('source-global-retained-control' if table == 'migration_history'
                                     else 'partitioned-vector-native-roundtrip' if table == 'chunk_embedding'
                                     else 'strict-typed-native-roundtrip')})
    driver.save(output / 'manifest.json', {
        'format': 'recall-synthetic-representative-v1', 'synthetic': True,
        'scope': 'bounded strict native type/field/schema controls; not a full initial-transfer/adoption snapshot',
        'full_profile': full.PROFILE, 'full_profile_counts': full.COUNTS,
        'source': {'helper_sha256': driver.sha(full.__file__), 'driver_sha256': driver.sha(driver.__file__),
                   'selector_sha256': driver.sha(__file__)},
        'selection': declarations, 'selected_rows': len(declarations), 'row_bytes': total,
        'limits': {'rows': MAX_ROWS, 'row_bytes': MAX_ROW_BYTES, 'total_bytes': MAX_TOTAL_BYTES},
        'graph_reference_closure': False, 'full_shape_generated': False,
        'native_prerequisites': [{'table': 'rel', 'before': 'entity',
            'required_endpoint_ordinals': [0, 1, 985, 986],
            'reason': 'real schema TYPE RELATION IN entity OUT entity ENFORCED'}],
        'native_control_contract': {
            'schema': 'full real migrations under transfer custody; no schemaless substitute',
            'ordinary': 'strict_json plus TransferRecord::from_json and import_batch; exact native afterimage and typed readback',
            'chunk_embedding': 'NodeEmbedding plus import_embedding_batch; exact original identity mapping/partition/vector readback',
            'migration_history': 'exact known receipt validation and retention; never tenant import'},
        'empty_tables': sorted(t for t, n in full.COUNTS.items() if not n),
        'whole_household_clock_qualified': False,
    })


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    write_selection(args.output)


if __name__ == '__main__':
    main()

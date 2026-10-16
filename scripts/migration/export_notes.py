"""Exact e40da508 native exporter notes; stream arrays without aggregate loading."""
import hashlib
import re

FIELDS = frozenset('source_store_path namespace database source_tables partition_mapping binary_identity source_identity custody compatibility geometry_fidelity native_history normalization name_embedding_metadata_shadows'.split())
TABLE_FIELDS = frozenset('expected_rows exported_rows native_insert_batches rows_without_string_space_id rows_without_string_id converted_decimal_values inexact_decimal_values converted_geometry_values output_table output_row_start'.split())
PARTITION_FIELDS = frozenset('target_table policy_id level_bucket model dims'.split())
SHADOW_FIELDS = frozenset('source_table id metadata output_row_index'.split())
HEX40 = re.compile(r'[0-9a-fA-F]{40}\Z')
HEX64 = re.compile(r'[0-9a-f]{64}\Z')
MAX_MEMBERS = 100_000


def read_notes(stream, require, sink=None):
    fields = {}
    tables = {}
    partitions = {}
    shadows = 0
    hashes = hashlib.sha256()
    metadata_bytes = 0

    def retain(key, subkey, value):
        nonlocal metadata_bytes
        raw = stream.last_value_raw
        if key != 'name_embedding_metadata_shadows':
            metadata_bytes += len(raw) + len(subkey.encode())
            require(metadata_bytes <= 64 * 1024 * 1024, 'export_notes_metadata_bound')
        digest = hashlib.sha256(raw).hexdigest()
        hashes.update(key.encode() + b'\0' + subkey.encode() + b'\0' + digest.encode() + b'\n')
        if sink:
            sink(key, subkey, value, raw, digest)

    for key in stream.members():
        require(key in FIELDS, 'unsupported_export_notes_field')
        fields[key] = True
        if key in {'source_tables', 'partition_mapping'}:
            target = tables if key == 'source_tables' else partitions
            for name in stream.members():
                require(len(target) < MAX_MEMBERS, 'export_notes_member_bound')
                value = stream.value()
                require(isinstance(value, dict), 'export_notes_entry_object')
                if key == 'source_tables':
                    require(set(value) == TABLE_FIELDS, 'export_source_table_schema')
                    require(isinstance(value['output_table'], str) and value['output_table'], 'export_output_table')
                    require(all(type(value[k]) is int and value[k] >= 0 for k in TABLE_FIELDS - {'output_table'}), 'export_source_table_count')
                    require(value['expected_rows'] == value['exported_rows'], 'export_source_table_incomplete')
                else:
                    require(set(value) == PARTITION_FIELDS, 'export_partition_schema')
                    require(value['target_table'] == 'chunk_embedding' and type(value['dims']) is int and value['dims'] > 0, 'export_partition_target')
                    require(all(type(value[k]) is str and value[k] for k in ('policy_id', 'level_bucket', 'model')), 'export_partition_identity')
                target[name] = value
                retain(key, name, value)
        elif key == 'name_embedding_metadata_shadows':
            for value in stream.array():
                require(isinstance(value, dict) and set(value) == SHADOW_FIELDS, 'export_shadow_schema')
                require(value['source_table'] == 'entity' and type(value['id']) is str and value['id'].startswith('entity:'), 'export_shadow_identity')
                require(type(value['output_row_index']) is int and value['output_row_index'] >= 0, 'export_shadow_ordinal')
                require(isinstance(value['metadata'], dict) and value['metadata'] and set(value['metadata']) <= {'name_embedding','name_embedding_model','name_embedding_text_hash'}, 'export_shadow_metadata')
                retain(key, str(shadows), value)
                shadows += 1
        else:
            value = stream.value()
            if key == 'binary_identity':
                require(isinstance(value, dict) and set(value) == {'sha256','package_version'} and type(value['sha256']) is str and HEX64.fullmatch(value['sha256']) and type(value['package_version']) is str and value['package_version'], 'export_binary_identity')
            elif key == 'source_identity':
                require(isinstance(value, dict) and set(value) == {'operator_declared_revision','base_revision'} and all(type(v) is str and HEX40.fullmatch(v) for v in value.values()), 'export_source_identity')
            else:
                require(type(value) is str and bool(value), 'export_notes_string')
            fields[key] = value
            retain(key, '', value)
    require(set(fields) == FIELDS, 'export_notes_missing_field')
    require(set(partitions) <= set(tables), 'export_partition_missing_source')
    output = {}
    for name, table in tables.items():
        target = table['output_table']
        require(target == (partitions[name]['target_table'] if name in partitions else name), 'export_output_mapping')
        output.setdefault(target, []).append((table['output_row_start'], table['exported_rows']))
    counts = {}
    for name, ranges in output.items():
        end = 0
        for start, count in sorted(ranges):
            require(start == end, 'export_output_range_gap_or_overlap')
            end += count
        counts[name] = end
    fields.pop('source_tables'); fields.pop('partition_mapping'); fields.pop('name_embedding_metadata_shadows')
    return {'format':'recall-native-export-notes-e40da508-v1', 'fields':fields,
            'source_tables':tables, 'partition_mapping':partitions, 'output_counts':counts,
            'shadow_count':shadows, 'lexical_parts_sha256':hashes.hexdigest(),
            'inexact_decimal_values':sum(t['inexact_decimal_values'] for t in tables.values()),
            'converted_geometry_values':sum(t['converted_geometry_values'] for t in tables.values())}

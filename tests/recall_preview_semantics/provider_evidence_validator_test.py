"""F-owned closed-world original-evidence controls; no gateway/product execution."""
from __future__ import annotations

from collections.abc import Mapping
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import socket
import subprocess
import time
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('f_provider_validator', Path(__file__).with_name('provider_evidence_validator.py'))
validator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(validator)


def raw(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def sha(value):
    return hashlib.sha256(value).hexdigest()


class LazyOriginal(Mapping):
    def __init__(self, values):
        self.values, self.reads = values, []

    def __getitem__(self, key):
        self.reads.append(key)
        return self.values[key]

    def __len__(self):
        return len(self.values)

    def __iter__(self):
        raise AssertionError('evidence must never be materialized or enumerated')


class OriginalFixture:
    def __init__(self, backend='elephant-db'):
        self.identity = {key: sha(key.encode()) for key in ('outer_configuration_sha256', 'provider_allocation_sha256',
            'workload_contract_sha256', 'corpus_sha256', 'preparation_contract_sha256', 'typed_delta_sha256')}
        self.identity['backend'] = backend
        self.cohort_id = 1 if backend == 'elephant-db' else 3
        self.clock = {'wall_start': 10000, 'mono_start': 100, 'boot_id': 'original-boot',
                      'deadline_wall': 17800, 'deadline_mono': 7900, 'time_cap_seconds': 7800}
        self.anchor = {'clock': self.clock, 'manifest_sha256': sha(b'manifest'), 'bindings_sha256': sha(b'original4bindings'),
                       'allocation_sha256': self.identity['provider_allocation_sha256'],
                       'configuration_sha256': self.identity['outer_configuration_sha256'],
                       'caps': {'physical_dispatches': 2100, 'maximum_nano_usd': 3000000000}}
        self.phase = {'format': 'recall-original-cohort-clock/v1', 'cohort_id': self.cohort_id, 'backend': backend,
            'phase': 'preparation', 'state': 'closed', 'run': {key: self.clock[key] for key in ('wall_start', 'mono_start', 'boot_id')},
            'phase_start': {'wall': 10010, 'mono': 110}, 'deadline_mono': 3710, 'time_cap_seconds': 3600,
            'closure_timestamp': None}
        self.authority = {'account_id': 'original-synthetic-account', 'credential_generation': 1,
            'bindings_sha256': self.anchor['bindings_sha256'],
            **{out: self.identity[source] for out, source in (
                ('configuration_sha256', 'outer_configuration_sha256'), ('allocation_sha256', 'provider_allocation_sha256'),
                ('contract_sha256', 'workload_contract_sha256'), ('corpus_sha256', 'corpus_sha256'),
                ('preparation_contract_sha256', 'preparation_contract_sha256'), ('typed_delta_sha256', 'typed_delta_sha256'))}}
        self.configs, self.calls = {}, []
        for index, (name, model, dimensions) in enumerate((
                ('qualification-household', 'text-embedding-3-small', 1536),
                ('qualification-business', 'text-embedding-3-large', 3072)), 1):
            capability = backend.replace('-', '_') + '_' + name.rsplit('-', 1)[1] + '_embedding'
            profile = {'id': 'document-profile', 'connection_id': 'embed', 'model': model, 'dimensions': dimensions}
            self.configs[name] = {'id': 'config-' + name, 'space_id': name, 'processing': {
                'active_embedding_profile_id': profile['id'],
                'bindings': {'document_embedding': {'id': 'product-binding', 'profile_id': profile['id'], 'revoked': False}},
                'profiles': {profile['id']: profile}, 'connections': {'embed': {
                    'id': 'embed', 'provider': 'openai', 'credential_ref': 'RECALL_GATEWAY_' + capability,
                    'credential_generation': 1, 'revoked': False, 'account_id': self.authority['account_id']}}}}
            request = raw({'model': model, 'input': ['literal original ' + name], 'dimensions': dimensions})
            response = raw({'object': 'list', 'data': [], 'usage': {'prompt_tokens': 4, 'total_tokens': 4}})
            binding = {'identity': copy.deepcopy(self.authority), 'scope': {'backend': backend, 'space': name,
                'operation': 'embedding', 'model': model, 'dimensions': dimensions, 'binding_id': capability,
                'connection_id': 'embed', 'profile_id': profile['id'], 'credential_generation': 1}}
            start = 112 + index * 5
            row = {'id': index, 'backend': backend, 'space': name, 'operation': 'embedding', 'model': model,
                'input_bound': len(request) + 64, 'output_cap': 0, 'reserved': 1064960,
                'request_hash': sha(request), 'forwarded_hash': sha(request), 'transforms': '',
                'started': 9900 + start, 'finished': 9900 + start + 2, 'outcome': 'complete', 'status': 200,
                'response_hash': sha(response), 'response_bytes': len(response), 'usage_input': 4, 'usage_output': 0,
                'requested_output_cap': 0}
            intent = {'format': 'recall-private-synthetic-capture/v1', 'dispatch_id': index,
                'original_sha256': sha(request), 'forwarded_sha256': sha(request), 'transforms': [],
                'transport_response_byte_cap': 8388608, 'credential_headers_retained': False, 'binding': binding}
            capture = {'complete': True, 'status': 200, 'response_sha256': sha(response), 'response_bytes': len(response),
                'provider_quality_acceptance': False, 'content_type': 'application/json', 'transport': {
                    'format': 'recall-provider-transport/v2', 'started_mono': start,
                    'request_started_mono': start + 0.1, 'deadline_mono': start + 120,
                    'read_completed_mono': start + 1, 'response_headers': {'content_length': [str(len(response))],
                    'transfer_encoding': [], 'content_encoding': [], 'content_type': ['application/json']},
                    'eof_observed': True, 'remaining_length': 0, 'chunked': False, 'response_closed': True}}
            self.calls.append({'row': row, 'binding': binding, 'intent': intent, 'request': request,
                               'forwarded': request, 'response': response, 'capture': capture})
        self.baseline_entries = []
        if backend == 'surreal-db':
            for index, call in enumerate(self.calls, 1):
                prior = copy.deepcopy(call['row'])
                prior.update(backend='elephant-db', started=10000 + index, finished=10000 + index + 0.5)
                self.baseline_entries.append(prior)
                call['row']['id'] += 2
                call['intent']['dispatch_id'] += 2
        self.after_entries = None
        self.observed = {'format': 'original-preparation-observation', 'literal_public_result': 'read separately by L'}

    def ledger(self, entries):
        return {'format': 'recall-gateway-receipt/v1', 'manifest_sha256': self.anchor['manifest_sha256'],
            'halted': None, 'reserved_nano_usd': sum(row['reserved'] for row in entries), 'dispatches': entries,
            'raw_capture': 'private-synthetic-bodies-v1', 'provider_quality_acceptance': False}

    def materialize(self, alter_bundle=None):
        values = {}
        def put(value, kind, binary=False):
            body = value if binary else raw(value)
            key = sha(body)
            values[key] = body
            return {'sha256': key, 'bytes': len(body), 'kind': kind}
        baseline = put(self.ledger(self.baseline_entries), 'provider-baseline')
        begin = {'format': 'recall-exclusive-cohort-begin/v1', 'cohort_id': self.cohort_id, 'backend': self.identity['backend'],
            'phase': 'preparation', 'baseline_sha256': baseline['sha256'],
            'allocation_sha256': self.identity['provider_allocation_sha256'], 'authority': 'exclusive_gateway_cohort'}
        inventory, records = [], []
        for call in self.calls:
            files = {name: put(value, kind, binary)['sha256'] for name, value, kind, binary in (
                ('request.original.json', call['request'], 'provider-request', True),
                ('request.forwarded.json', call['forwarded'], 'provider-request', True),
                ('intent.json', call['intent'], 'provider-dispatch-intent', False),
                ('response.body', call['response'], 'provider-response', True),
                ('capture.json', call['capture'], 'provider-capture', False))}
            inventory.append({'dispatch_id': call['row']['id'], 'files': files})
            records.append({'ledger': call['row'], 'binding': call['binding'], 'send_intent': 1})
        after = self.ledger(self.after_entries if self.after_entries is not None else self.baseline_entries + [call['row'] for call in self.calls])
        observed = {**self.observed, 'provider_cohort_begin': begin}
        sealed = {'before_sha256': baseline['sha256'], 'after': after, 'capture_inventory': inventory,
                  'observation_sha256': sha(raw(observed)), 'identity': self.authority}
        sealed_ref = put(sealed, 'provider-sealed')
        close = {'format': 'recall-exclusive-cohort-accounting/v1', 'verdict': 'passed', 'cohort_id': self.cohort_id,
            'before_sha256': baseline['sha256'], 'after_sha256': sha(raw(after)),
            'capture_inventory_sha256': sha(raw(inventory)), 'sealed_receipt_sha256': sealed_ref['sha256'],
            'physical_dispatch_reservations': len(self.calls), 'reserved_nano_usd': len(self.calls) * 1064960,
            'unsettled_dispatches': 0, 'per_request_association': 'unmet', 'admission_closed': True}
        observed['provider_accounting'] = close
        bundle = {'format': 'recall-dynamic-provider-accounting/v2', 'begin': put(begin, 'provider-cohort-begin'),
            'close': put(close, 'provider-cohort-close'), 'baseline': baseline,
            'phase_clock': put(self.phase, 'provider-phase-clock'), 'sealed': sealed_ref,
            'capture_inventory': put(inventory, 'provider-capture-inventory'),
            'dispatch_records': put(records, 'provider-dispatch-records')}
        if alter_bundle:
            alter_bundle(bundle)
        reference = put(bundle, 'provider-accounting')
        context = {'anchor': self.anchor, 'anchor_sha256': sha(raw(self.anchor))}
        context_raw = raw(context)
        wrapper = {'context': context, 'raw_bytes': context_raw, 'raw_sha256': sha(context_raw),
                   'observed_wall': 10060, 'observed_monotonic_ns': 160000000000}
        return reference, LazyOriginal(values), self.identity, wrapper, self.configs, observed, (111000000000, 150000000000)


class OriginalProviderEvidence(unittest.TestCase):
    def setUp(self):
        # Every test uses only synthetic in-memory literal bytes. Prevent even
        # accidental producer/process/clock use during predicate evaluation.
        for module, name in ((socket, 'socket'), (subprocess, 'Popen'), (time, 'time'), (time, 'monotonic')):
            guard = patch.object(module, name, side_effect=AssertionError('forbidden external boundary'))
            guard.start()
            self.addCleanup(guard.stop)

    def validate(self, fixture, alter_bundle=None):
        args = fixture.materialize(alter_bundle)
        result = validator.validate_provider_accounting(*args)
        self.assertEqual(set(result), {'begin', 'close', 'phase_clock'})
        self.assertEqual(result['close']['per_request_association'], 'unmet')
        return args, result

    def test_original_two_backend_byte_transcripts_and_lazy_mapping(self):
        for backend in ('elephant-db', 'surreal-db'):
            with self.subTest(backend=backend):
                args, result = self.validate(OriginalFixture(backend))
                self.assertEqual(result['close']['physical_dispatch_reservations'], 2)
                self.assertLess(len(args[1].reads), 30)
                self.assertNotIn('verdict', result)

    def test_actual_chunked_and_close_delimited_framing(self):
        for chunked in (False, True):
            fixture = OriginalFixture()
            for call in fixture.calls:
                proof = call['capture']['transport']
                proof['response_headers']['content_length'] = []
                proof['response_headers']['transfer_encoding'] = [' Chunked '] if chunked else []
                proof['chunked'], proof['remaining_length'] = chunked, None
            self.validate(fixture)

    def test_missing_transport_or_observations_never_inferred_from_finished(self):
        for name in ('transport', 'read_completed_mono', 'request_started_mono'):
            fixture = OriginalFixture()
            if name == 'transport':
                fixture.calls[0]['capture'].pop(name)
            else:
                fixture.calls[0]['capture']['transport'][name] = None
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.validate(fixture)

    def test_deadline_eof_and_parser_state_refusals(self):
        mutations = (
            lambda p: p.update(read_completed_mono=p['deadline_mono']),
            lambda p: p.update(deadline_mono=p['started_mono'] + 121),
            lambda p: p.update(eof_observed=False), lambda p: p.update(response_closed=False),
            lambda p: p.update(remaining_length=1), lambda p: p.update(chunked=True),
            lambda p: p.update(started_mono=109),
        )
        for mutate in mutations:
            fixture = OriginalFixture(); mutate(fixture.calls[0]['capture']['transport'])
            with self.subTest(mutate=mutate), self.assertRaises(ValueError): self.validate(fixture)

    def test_raw_duplicate_conflicting_encoding_and_length_headers(self):
        mutations = (
            lambda h: h['content_length'].append(h['content_length'][0]),
            lambda h: h.update(transfer_encoding=['chunked']),
            lambda h: h.update(content_length=['+10']), lambda h: h.update(content_length=['１２']),
            lambda h: h.update(content_length=['1']), lambda h: h.update(content_encoding=['gzip']),
            lambda h: h.update(content_type=['application/json', 'application/json']),
        )
        for mutate in mutations:
            fixture = OriginalFixture(); mutate(fixture.calls[0]['capture']['transport']['response_headers'])
            with self.subTest(mutate=mutate), self.assertRaises(ValueError): self.validate(fixture)

    def test_actual_request_profile_scope_and_revocation(self):
        for target in ('profile', 'capability', 'space', 'credential_generation', 'revoked', 'active', 'configuration_space'):
            fixture = OriginalFixture(); call = fixture.calls[0]
            if target == 'profile': call['binding']['scope']['profile_id'] = 'other'
            elif target == 'capability': call['binding']['scope']['binding_id'] += '_other'
            elif target == 'space': call['row']['space'] = 'foreign'
            elif target == 'credential_generation': call['binding']['scope']['credential_generation'] = 2
            elif target == 'active': fixture.configs['qualification-household']['processing']['active_embedding_profile_id'] = 'other'
            elif target == 'configuration_space': fixture.configs['qualification-household']['space_id'] = 'foreign'
            else: fixture.configs['qualification-household']['processing']['connections']['embed']['revoked'] = True
            with self.subTest(target=target), self.assertRaises(ValueError): self.validate(fixture)

    def test_unknown_usage_retains_complete_reservation_for_successful_json(self):
        fixture = OriginalFixture()
        for call in fixture.calls:
            response = raw({'object': 'list', 'data': []})
            call['response'] = response
            call['row'].update(usage_input=None, usage_output=None,
                               response_hash=sha(response), response_bytes=len(response))
            call['capture'].update(response_sha256=sha(response), response_bytes=len(response))
            call['capture']['transport']['response_headers']['content_length'] = [str(len(response))]
        _, result = self.validate(fixture)
        self.assertEqual(result['close']['reserved_nano_usd'], 2129920)
        fixture.calls[0]['row']['reserved'] = 0
        with self.assertRaises(ValueError): self.validate(fixture)

    def test_failed_dispatch_keeps_ledger_liability_but_refuses_qualification(self):
        for status in (101, 301, 429, 500):
            fixture = OriginalFixture()
            for call in fixture.calls:
                response = raw({'error': {'message': 'original synthetic upstream error'}})
                call['response'] = response
                call['row'].update(status=status, outcome='upstream_error' if status >= 300 else 'complete',
                    usage_input=None, usage_output=None, response_hash=sha(response), response_bytes=len(response))
                call['capture'].update(status=status, response_sha256=sha(response), response_bytes=len(response))
                call['capture']['transport']['response_headers']['content_length'] = [str(len(response))]
            ledger = fixture.ledger([call['row'] for call in fixture.calls])
            validator.provider_ledger(ledger, {'anchor': fixture.anchor})
            self.assertEqual(ledger['reserved_nano_usd'], 2129920)
            with self.subTest(status=status), self.assertRaisesRegex(ValueError, 'failed_dispatch_unqualified'):
                self.validate(fixture)
        fixture = OriginalFixture('surreal-db')
        fixture.baseline_entries[0].update(status=429, outcome='upstream_error')
        with self.assertRaisesRegex(ValueError, 'failed_dispatch_unqualified'): self.validate(fixture)

    def test_http_eof_cannot_complete_an_unexpected_embedding_event_stream(self):
        for body in (b'data: {"delta":"unterminated"}\n\n', b'data: [DONE]\n\n', b'{"usage":{}}'):
            fixture = OriginalFixture(); call = fixture.calls[0]
            call['response'] = body
            call['row'].update(usage_input=None, usage_output=None, response_hash=sha(body), response_bytes=len(body))
            call['capture'].update(response_sha256=sha(body), response_bytes=len(body), content_type='text/event-stream')
            call['capture']['transport']['response_headers'].update(content_type=['text/event-stream'], content_length=[str(len(body))])
            with self.subTest(body=body), self.assertRaisesRegex(ValueError, 'json_content_type'):
                self.validate(fixture)
        fixture = OriginalFixture(); call = fixture.calls[0]
        body = b'not json'
        call['response'] = body
        call['row'].update(usage_input=None, usage_output=None, response_hash=sha(body), response_bytes=len(body))
        call['capture'].update(response_sha256=sha(body), response_bytes=len(body))
        call['capture']['transport']['response_headers']['content_length'] = [str(len(body))]
        with self.assertRaisesRegex(ValueError, 'embedding_json_response'): self.validate(fixture)

    def test_fabricated_usage_and_explicit_incomplete_are_refused(self):
        for bad in (None, True, -1, 8193):
            fixture = OriginalFixture(); fixture.calls[0]['row']['usage_input'] = bad
            with self.subTest(usage=bad), self.assertRaises(ValueError): self.validate(fixture)
        fixture = OriginalFixture(); call = fixture.calls[0]
        response = raw({'status': 'incomplete'})
        call['response'] = response
        call['row'].update(usage_input=None, usage_output=None, response_hash=sha(response), response_bytes=len(response))
        call['capture'].update(response_sha256=sha(response), response_bytes=len(response))
        call['capture']['transport']['response_headers']['content_length'] = [str(len(response))]
        with self.assertRaises(ValueError): self.validate(fixture)

    def test_rehashed_wrong_original_response_still_refuses_ledger(self):
        fixture = OriginalFixture(); fixture.calls[0]['response'] = b'{"changed":true}'
        with self.assertRaises(ValueError): self.validate(fixture)

    def test_raw_hash_and_missing_original_reference(self):
        for missing in (False, True):
            args = list(OriginalFixture().materialize())
            key = args[0]['sha256']
            if missing: del args[1].values[key]
            else: args[1].values[key] += b' '
            with self.subTest(missing=missing), self.assertRaises(ValueError):
                validator.validate_provider_accounting(*args)

    def test_reference_kind_alias_and_phase_clock_not_reconstructed(self):
        for mutate in (lambda b: b['baseline'].update(kind='provider-sealed'),
                       lambda b: b['close'].update(bytes=True)):
            with self.assertRaises(ValueError): self.validate(OriginalFixture(), mutate)
        fixture = OriginalFixture(); fixture.phase['run']['boot_id'] = 'different-boot'
        with self.assertRaises(ValueError): self.validate(fixture)

    def test_equal_zero_duration_wall_samples_are_conservative_not_negative(self):
        fixture = OriginalFixture()
        for call in fixture.calls: call['row'].update(started=10020, finished=10020)
        self.validate(fixture)
        extra = copy.deepcopy(fixture.calls[0]); extra['row']['id'] = 3; extra['intent']['dispatch_id'] = 3
        fixture.calls.append(extra)
        with self.assertRaisesRegex(ValueError, 'concurrency'): self.validate(fixture)

    def test_no_renewed_phase_or_whole_clock_and_no_producer_pass_authority(self):
        fixture = OriginalFixture(); fixture.phase['deadline_mono'] += 1
        with self.assertRaises(ValueError): self.validate(fixture)
        args = list(OriginalFixture().materialize()); args[5]['provider_accounting']['physical_dispatch_reservations'] = 1
        with self.assertRaises(ValueError): validator.validate_provider_accounting(*args)

    def test_exact_450_calls_225_per_space_boundary_and_first_excess(self):
        def expand(count_a, count_b):
            fixture = OriginalFixture()
            original = fixture.calls
            fixture.calls = []
            for count, template in zip((count_a, count_b), original):
                for _ in range(count):
                    call = copy.deepcopy(template)
                    index = len(fixture.calls) + 1
                    start = 112 + index * 5
                    call['row'].update(id=index, started=9900 + start, finished=9902 + start)
                    call['intent']['dispatch_id'] = index
                    call['capture']['transport'].update(started_mono=start, request_started_mono=start + 0.1,
                        deadline_mono=start + 120, read_completed_mono=start + 1)
                    fixture.calls.append(call)
            return fixture
        # Extend the supplied original observation, not a refreshed clock.
        args = list(expand(225, 225).materialize())
        args[3]['observed_monotonic_ns'] = 2500000000000
        args[3]['observed_wall'] = 12400
        args[6] = (111000000000, 2450000000000)
        self.assertEqual(validator.validate_provider_accounting(*args)['close']['physical_dispatch_reservations'], 450)
        for counts, code in (((226, 1), 'per_space'), ((225, 226), 'cohort_reservations')):
            args = list(expand(*counts).materialize())
            args[3]['observed_monotonic_ns'] = 2500000000000
            args[3]['observed_wall'] = 12400
            args[6] = (111000000000, 2450000000000)
            with self.subTest(counts=counts), self.assertRaisesRegex(ValueError, code):
                validator.validate_provider_accounting(*args)

    def test_64_original_sdk_inputs_accept_and_65_refuse(self):
        for count in (64, 65):
            fixture = OriginalFixture(); call = fixture.calls[0]
            request = raw({'model': call['row']['model'], 'input': ['literal'] * count})
            call['request'] = call['forwarded'] = request
            call['row'].update(request_hash=sha(request), forwarded_hash=sha(request), input_bound=len(request) + 64 * count)
            call['intent'].update(original_sha256=sha(request), forwarded_sha256=sha(request))
            if count == 64: self.validate(fixture)
            else:
                with self.assertRaisesRegex(ValueError, 'embedding_bounds'): self.validate(fixture)

    def test_original_baseline_prefix_survives_exactly(self):
        fixture = OriginalFixture('surreal-db')
        self.validate(fixture)
        changed = copy.deepcopy(fixture.baseline_entries); changed[0]['usage_input'] = 3
        fixture.after_entries = changed + [call['row'] for call in fixture.calls]
        with self.assertRaisesRegex(ValueError, 'ledger_prefix'): self.validate(fixture)

    def test_original_fixed_cohort_order_cannot_reset_native_baseline(self):
        fixture = OriginalFixture('surreal-db')
        fixture.baseline_entries = []
        for index, call in enumerate(fixture.calls, 1):
            call['row']['id'] = index
            call['intent']['dispatch_id'] = index
        with self.assertRaisesRegex(ValueError, 'prior_cohort_prefix'): self.validate(fixture)
        fixture = OriginalFixture(); fixture.cohort_id = 2; fixture.phase['cohort_id'] = 2
        with self.assertRaisesRegex(ValueError, 'original_begin'): self.validate(fixture)

    def test_shared_hash_across_incompatible_raw_roles_refuses(self):
        fixture = OriginalFixture(); call = fixture.calls[0]
        call['response'] = call['request']
        call['row'].update(response_hash=sha(call['response']), response_bytes=len(call['response']), usage_input=None, usage_output=None)
        call['capture'].update(response_sha256=sha(call['response']), response_bytes=len(call['response']))
        call['capture']['transport']['response_headers']['content_length'] = [str(len(call['response']))]
        with self.assertRaisesRegex(ValueError, 'reference_alias'): self.validate(fixture)

    def test_duplicate_original_request_key_is_not_silently_collapsed(self):
        fixture = OriginalFixture(); call = fixture.calls[0]
        request = b'{"model":"x","model":"text-embedding-3-small","input":["literal"]}'
        call['request'] = request; call['row']['request_hash'] = sha(request); call['intent']['original_sha256'] = sha(request)
        with self.assertRaisesRegex(ValueError, 'duplicate'): self.validate(fixture)


if __name__ == '__main__':
    unittest.main()

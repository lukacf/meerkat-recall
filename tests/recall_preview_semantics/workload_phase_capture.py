"""Actual D28 preparation/measurement adapter for D's separately versioned chain.

The existing public-call collectors perform the work. This module retains their
original captures, consumes L's predicates, and uses G's explicit v2 close/export.
It has no provider-quality oracle and never creates a v1 qualified receipt.
"""
from __future__ import annotations

import asyncio
from collections.abc import Mapping
from pathlib import Path
import re
import urllib.parse

from workload_orchestration import (
    ROOT, EvidenceUnion, checked_reference, current_expiry, digest,
    encode, load_helper, read_original, reference, require, strict_json, profiles, file_bound,
)

PREPARATION = "tests/recall_preview_semantics/workload_preparation_driver.py"
TRANSPORT = "scripts/qualification/recall_workload.py"
BRIDGE = "tests/recall_preview_semantics/gateway/cohort_bridge.py"
ADAPTER = "tests/recall_preview/workload.py"
STATIC = {"contract": "workload_contract", "corpus": "corpus", "provider_allocation": "provider_allocation",
          "preparation_contract": "preparation_contract", "typed_delta": "typed_delta", "rendered_map": "rendered_map"}
CALLER_LIMIT = 131072
CHANNEL = {"sidecar": ("provider-accounting-sidecar", 16384),
           "mountinfo": ("provider-mountinfo", 1048576),
           "index": ("provider-accounting-index", 8388608),
           "directory": ("provider-accounting-directory", 16384)}


def record_file(files, path, kind, maximum):
    files.path(str(path))
    raw = read_original(path, maximum, private=True, profile_id=files.profile.id)
    return files.retain(reference(raw, kind), raw, str(path))


def phase_inputs(custody, *, phase, authorization):
    """Use reviewed static requests plus the actual validated generated records."""
    require(phase in ("preparation", "measurement") and type(authorization) is dict
            and authorization.get("execution_authorized") is True
            and authorization.get("provider_dispatch_authorized") is True, "d28_original_execution_allocation")
    review, files = custody.reviewed["review"], custody.files
    selected = files.profile
    if selected.id == profiles.SEMANTIC140_ID:
        require(profiles.profile_for_review(review) is selected
                and authorization.get("profile_id") == selected.id, "semantic140_original_profile_binding")
    identity = custody.component["identity"]
    loaded = {}
    manifest = {"format": "recall-dynamic-workload-phase/v2", "execution_authorized": True,
                "provider_dispatch_authorized": True, "phase": phase, "provider_phase": phase,
                **{name: identity[name] for name in ("source_commit", "artifact_sha256", "binary_sha256", "backend", "endpoint")},
                "expires_at": custody.live.phase["plan"]["expires_at"],
                "output_directory": str(custody.output / phase)}
    for name, static_name in STATIC.items():
        ref = review["input_refs"][static_name]
        loaded[name] = files.json(ref)
        manifest[name + "_path"] = files.original_path(ref)
        manifest[name + "_sha256"] = ref["sha256"]
    intents = files.json(review["input_refs"]["workload_intents"])
    require(type(intents) is dict and set(intents) == {"format", "backends"}
            and intents["format"] == "recall-workload-intents/v2", "d28_original_phase_intents")
    intent = intents["backends"][custody.backend]
    require(type(intent) is dict and set(intent) == {"fixture_timestamp", "preparation_nonce", "measurement_nonce"},
            "d28_original_phase_intent_fields")
    for name in ("preparation_nonce", "measurement_nonce"):
        require(type(intent[name]) is str and re.fullmatch(r"[A-Za-z0-9_-]{16,64}", intent[name]), "d28_original_phase_nonce")
    require(intent["preparation_nonce"] != intent["measurement_nonce"], "d28_distinct_original_phase_nonces")
    manifest["fixture_timestamp"] = intent["fixture_timestamp"]
    manifest["nonce"] = intent[phase + "_nonce"]
    blueprint = files.json(review["input_refs"]["expected_configurations"])
    actual = files.json(custody.envelope["actual_space_configurations"], "space-configurations")
    names = [row["id"] for row in loaded["corpus"]["spaces"]]
    require(set(actual) == set(blueprint) == set(names) == set(custody.live.token_files), "d28_actual_named_space_inventory")
    loaded["expected_configurations"] = {}
    for row in loaded["corpus"]["spaces"]:
        space = row["id"]
        expected = blueprint[space]
        require(type(expected) is dict and set(expected) == {"record_security", "embedding_partition_model", "input_envelope"},
                "d28_static_preparation_blueprint")
        config = actual[space]
        require(config["space_id"] == space and config["embedding_provider"] == row["provider"]
                and config["embedding_model"] == row["model"] and config["embedding_dims"] == row["dimensions"],
                "d28_actual_configuration_corpus_binding")
        loaded["expected_configurations"][space] = {**strict_json(encode(expected)), "configuration": config}
    # Preparation's literal role labels match C's capture labels. The retained
    # measurement collector restricts local role identifiers to underscores;
    # these labels still point to the same original per-space C signed files.
    roles = {space: "service-" + space if phase == "preparation" else f"service_{index}"
             for index, space in enumerate(names)}
    manifest["space_credentials"] = roles
    manifest["credential_files"] = {roles[space]: custody.live.token_files[space] for space in names}
    manifest["provider_bridge_path"] = str(ROOT / BRIDGE)
    manifest["provider_bridge_sha256"] = review["helper_sha256"][BRIDGE]
    if selected.id == profiles.SEMANTIC140_ID:
        manifest["profile_id"] = selected.id
        require(profiles.profile_for_manifest(manifest) is selected
                and profiles.profile_for_contract(loaded["contract"]) is selected, "semantic140_phase_data_binding")
        profiles.validate_corpus(loaded["corpus"], selected)
    return manifest, loaded


class OriginalCohort:
    """One actual G begin/explicit-v2 close, retaining the unchanged v1 close."""
    def __init__(self, custody, bridge, caller_reference):
        self.custody, self.bridge = custody, bridge
        self.caller_reference = checked_reference(caller_reference)
        require(self.caller_reference["kind"] == "workload-phase-manifest"
                and self.caller_reference["bytes"] <= CALLER_LIMIT, "d28_original_caller_reference")
        self.begin = self.close = self.close_envelope = self.manifest = None
        self.channel_reference = None
        self.begin_attempted = self.close_attempted = False
        self.export_attempted = False

    def guard(self, reserve):
        current_expiry(self.custody.reviewed["review"])
        self.custody.live.revalidate_custody()
        require(self.custody.live.instance.allocation.remaining() >= reserve, "d28_original_clock_reserve")

    def authorize_provider_allocation(self, manifest, contract, allocation):
        require(not self.begin_attempted and self.custody.component is not None, "d28_original_cohort_begin_once")
        self.begin_attempted = True
        self.guard(10)
        require(self.custody.files.get(self.caller_reference, "workload-phase-manifest") == encode(manifest),
                "d28_original_caller_changed")
        self.manifest = strict_json(encode(manifest))
        self.begin = self.bridge.authorize_provider_allocation(manifest, contract, allocation)
        return self.begin

    def provider_accounting(self, manifest, observation, allocation):
        require(not self.close_attempted, "d28_original_cohort_close_once")
        self.close_attempted = True
        self.guard(130)
        require(encode(manifest) == self.custody.files.get(self.caller_reference, "workload-phase-manifest"),
                "d28_original_caller_changed")
        # This is the SAME one terminal request. The new explicit action adds
        # an export reference while preserving the exact legacy close bytes.
        envelope = self.bridge.provider_accounting_with_evidence(manifest, observation, allocation)
        require(type(envelope) is dict and set(envelope) == {"format", "close", "export"}
                and envelope["format"] == "recall-exclusive-cohort-close-envelope/v2", "d28_original_close_envelope")
        self.close_envelope, self.close = envelope, envelope["close"]
        return self.close

    def export(self):
        require(not self.export_attempted and self.begin is not None and self.close_envelope is not None and self.close_envelope["export"] is not None,
                "d28_original_closed_export_missing")
        self.export_attempted = True
        self.guard(1)
        exported = self.bridge.export_cohort_evidence(self.manifest, self.begin, self.close, self.close_envelope["export"])
        require(type(exported) is dict and set(exported) == {"reference", "raw_evidence", "files", "channel"}
                and isinstance(exported["raw_evidence"], Mapping), "d28_original_provider_mapping")
        ref = checked_reference(exported["reference"])
        require(ref["kind"] == "provider-accounting", "d28_original_provider_kind")
        raw = exported["raw_evidence"][ref["sha256"]]
        require(type(raw) is bytes and len(raw) == ref["bytes"] and digest(raw) == ref["sha256"], "d28_original_provider_bundle_bytes")
        value = strict_json(raw)
        require(type(value) is dict and set(value) == {"format", "begin", "close", "baseline", "phase_clock", "sealed", "capture_inventory", "dispatch_records"}
                and value["format"] == "recall-dynamic-provider-accounting/v2", "d28_original_provider_bundle")
        for name, kind in (("begin", "provider-cohort-begin"), ("close", "provider-cohort-close")):
            original_ref = checked_reference(value[name])
            require(original_ref["kind"] == kind, "d28_original_cohort_receipt_kind")
            original_raw = exported["raw_evidence"][original_ref["sha256"]]
            require(type(original_raw) is bytes and len(original_raw) == original_ref["bytes"]
                    and digest(original_raw) == original_ref["sha256"]
                    and strict_json(original_raw) == getattr(self, name), "d28_original_cohort_receipt_changed")
        require(type(exported["channel"]) is dict and set(exported["channel"]) == set(CHANNEL),
                "d28_original_channel_fields")
        phase = self.manifest["phase"]
        require(phase in ("preparation", "measurement"), "d28_original_channel_phase")
        channel = {"format": "recall-provider-accounting-channel/v2",
                   "anchor_sha256": self.custody.context["context"]["anchor_sha256"],
                   "caller": self.caller_reference}
        for name, (kind, maximum) in CHANNEL.items():
            raw = exported["channel"][name]
            require(type(raw) is bytes and 0 < len(raw) <= maximum, "d28_original_channel_raw_bound")
            channel[name] = self.custody.files.save_raw(
                self.custody.output / phase / ("provider-channel-" + name + ".raw"), raw, kind)
        require(len(encode(channel)) <= 16384, "d28_original_channel_carrier_bound")
        self.channel_reference = self.custody.files.save(
            self.custody.output / phase / "provider-channel.json", channel, "provider-accounting-channel")
        return exported, value


def preparation_references(custody, preparation):
    """Index unchanged actual collector files, never reconstructed HTTP bodies."""
    files, root = custody.files, preparation.root
    count = preparation.http_count
    require(type(count) is int and 0 < count <= 4096, "d28_preparation_capture_count")
    rows, configurations, by_path = [], {}, {}
    for index in range(1, count + 1):
        intent = record_file(files, root / "captures" / f"{index:04}.request.json", "preparation-intent", 131072)
        response = record_file(files, root / "captures" / f"{index:04}.json", "preparation-response", 2 * 1024**2)
        row = files.json(response)
        rows.append({"request_index": index, "intent": intent, "response": response})
        by_path.setdefault((row["space"], row["method"], row["path"]), []).append(response)
        if row["method"] == "GET" and row["path"] == "/v1/spaces/" + urllib.parse.quote(row["space"], safe=""):
            configurations.setdefault(row["space"], []).append(response)
    names = set(preparation.loaded["expected_configurations"])
    require(set(configurations) == names and all(len(refs) == 2 for refs in configurations.values()),
            "d28_original_configuration_capture_pair")
    immutable = []
    for source in preparation.published:
        path = "/v1/docs/" + urllib.parse.quote(source["doc_id"], safe="") + "/revisions/" + urllib.parse.quote(source["internal_doc_revision_id"], safe="")
        matches = by_path.get((source["space"], "GET", path), [])
        require(len(matches) == 1, "d28_original_immutable_capture_identity")
        immutable.append({"space": source["space"], "key": source["key"], "revision": source["revision"], "response": matches[0]})
    return {"observation": record_file(files, root / "observation.json", "preparation-observation", 8 * 1024**2),
            "original_intents": files.save(root / "original-intents.json", {"format": "recall-dynamic-preparation-intents/v2", "requests": rows}, "preparation-intents"),
            "configuration_before": files.save(root / "configuration-before.json", {name: configurations[name][0] for name in names}, "preparation-configurations"),
            "configuration_after": files.save(root / "configuration-after.json", {name: configurations[name][1] for name in names}, "preparation-configurations"),
            "immutable_revisions": files.save(root / "immutable-revisions.json", immutable, "preparation-immutable-inventory")}, by_path


async def validate_preparation(custody, preparation, references, exported, provider, channel, label):
    chain, controller, context = await custody.current_chain(label)
    inventory = custody.files.save(custody.output / (label + ".captures.json"),
        {"format": "recall-dynamic-capture-inventory/v2", "files": [
            {"path": path, "ref": ref} for path, ref in sorted(custody.files.files.items())
            if Path(path).is_relative_to(preparation.root)
            or path == str(custody.output / "preparation.manifest.json")]}, "capture-inventory")
    manifest = preparation.manifest
    identity = {**custody.component["identity"], "preparation_nonce": manifest["nonce"],
                "workload_contract_sha256": manifest["contract_sha256"],
                **{name + "_sha256": manifest[name + "_sha256"] for name in
                   ("corpus", "preparation_contract", "typed_delta", "rendered_map", "provider_allocation")}}
    evidence = {"format": "recall-dynamic-preparation-evidence/v2", "identity": identity,
                "runtime_component": chain, **references, "capture_inventory": inventory,
                "provider_cohort_begin": provider["begin"], "provider_accounting": exported["reference"],
                "provider_accounting_channel": channel, "controller": controller}
    originals = EvidenceUnion(custody.files.raw, exported["raw_evidence"], profile_id=custody.files.profile.id)
    component = custody.validator.validate_prepared(custody.reviewed, evidence, custody.component, originals, context)
    require(type(component) is dict and component.get("format") == "recall-prepared-fixture-component/v2"
            and component.get("verdict") == "passed" and component.get("product_acceptance") is False
            and component.get("identity") == identity, "d28_prepared_component_refused")
    custody.live.revalidate_custody()
    evidence_ref = custody.files.save(custody.output / (label + ".prepared-evidence.json"), evidence, "preparation-evidence")
    component_ref = custody.files.save(custody.output / (label + ".prepared-component.json"), component, "prepared-component")
    return component, component_ref, evidence_ref


async def current_heads(preparation, original_paths):
    """Recheck original public config/head/operation before measured admission.

    These are additional read-only preflight requests using the SAME original
    preparation HTTP/capture counters. They do not alter its saved observation.
    """
    first = preparation.http_count + 1
    await preparation.configurations()
    for source, accepted in preparation.accepted:
        for path in (preparation.head_path(source),
                     "/v1/operations/" + urllib.parse.quote(accepted["operation"]["operation_id"], safe="")):
            references = original_paths.get((source["space"], "GET", path), [])
            require(references, "d28_original_preflight_read_missing")
            original = preparation._d28_files.json(references[-1])["response"]["body"]
            actual = await preparation.get(path, source["space"])
            require(encode(actual) == encode(original), "d28_original_head_or_operation_changed")
    return first, preparation.http_count


async def prepare_and_measure(custody, authorization):
    """Perform the retained actual collectors only after original prerequisites."""
    review = custody.reviewed["review"]
    prep_module = load_helper("d28_actual_preparation", PREPARATION, review)
    transport = load_helper("d28_actual_measurement", TRANSPORT, review)
    bridge = load_helper("d28_original_cohort_bridge", BRIDGE, review)
    adapter = load_helper("d28_independent_workload_adapter", ADAPTER, review)
    require(callable(getattr(custody.validator, "validate_prepared", None))
            and callable(getattr(custody.live, "revalidate_worker_recipe", None))
            and callable(getattr(bridge, "provider_accounting_with_evidence", None))
            and callable(getattr(bridge, "export_cohort_evidence", None)), "d28_original_producer_seam_missing")
    manifest, loaded = phase_inputs(custody, phase="preparation", authorization=authorization)
    transport.validate_schedule(loaded["contract"], loaded["corpus"])
    prep_module.validate_sources(loaded["corpus"])
    prep_module.typed_contract(manifest, loaded)
    loaded["rendered_by_source"] = transport.rendered_map_index(loaded["rendered_map"], loaded["corpus"], manifest["corpus_sha256"])
    remaining = custody.live.instance.allocation.remaining()
    require(remaining > 150, "d28_original_preparation_clock_exhausted")
    require(len(encode(manifest)) <= CALLER_LIMIT, "d28_original_caller_bound")
    caller = custody.files.save(custody.output / "preparation.manifest.json", manifest, "workload-phase-manifest")
    cohort = OriginalCohort(custody, bridge, caller)
    preparation = prep_module.Preparation(manifest, loaded, cohort)
    # The inner collector retains 3600/10/130 limits. Cancellation records a
    # failed observation and closes G under the reserved original whole clock.
    async with asyncio.timeout(min(3610, remaining - 140)):
        result = await preparation.run()
    require(result == 0, "d28_original_preparation_failed")
    references, original_paths = preparation_references(custody, preparation)
    exported, provider = cohort.export()
    await validate_preparation(custody, preparation, references, exported, provider, cohort.channel_reference, "prepared")
    preparation._d28_files = custody.files
    custody.live.revalidate_custody()
    first, last = await current_heads(preparation, original_paths)
    preflight = []
    for index in range(first, last + 1):
        preflight.append({"request_index": index,
            "intent": record_file(custody.files, preparation.root / "captures" / f"{index:04}.request.json", "preparation-intent", 131072),
            "response": record_file(custody.files, preparation.root / "captures" / f"{index:04}.json", "preparation-response", 2 * 1024**2)})
    custody.files.save(custody.output / "premeasurement-heads.json", {"format": "recall-d28-current-publication-reads/v2",
        "requests": preflight, "total_preparation_http_requests": preparation.http_count}, "premeasurement-publication-reads")
    component, component_ref, _ = await validate_preparation(custody, preparation, references, exported, provider, cohort.channel_reference, "premeasurement")
    measured_manifest, measured_loaded = phase_inputs(custody, phase="measurement", authorization=authorization)
    measured_manifest["prepared_fixture_sha256"] = component_ref["sha256"]
    measured_loaded["prepared_fixture"] = component
    queries = transport.validate_schedule(measured_loaded["contract"], measured_loaded["corpus"])
    require(custody.live.instance.allocation.remaining() >= 270, "d28_original_measurement_clock_exhausted")
    require(len(encode(measured_manifest)) <= CALLER_LIMIT, "d28_original_caller_bound")
    caller = custody.files.save(custody.output / "measurement.manifest.json", measured_manifest, "workload-phase-manifest")
    measurement_cohort = OriginalCohort(custody, bridge, caller)
    measurement = transport.Workload(measured_manifest, measured_loaded, queries, adapter, measurement_cohort)
    result = await measurement.run()
    if custody.files.profile.id == profiles.SEMANTIC140_ID:
        # Count actual per-request files as well as their combined observation.
        # Hash deduplication never erases physical copies from the byte budget.
        retained = []
        for row in measurement.rows:
            retained.append(record_file(custody.files, measurement.root / "captures" / (row["id"] + ".json"),
                                        "measurement-request-capture", 2 * 1024**2))
        custody.files.save(measurement.root / "original-request-captures.json",
                           {"profile_id": profiles.SEMANTIC140_ID, "requests": retained},
                           "measurement-request-inventory")
    observed = record_file(custody.files, measurement.root / "observation.json", "measurement-observation", file_bound(custody.files.profile.id))
    # Preserve the actual explicit close, even when the independent workload
    # verdict fails. No second backend follows a failed collector or cleanup.
    if measurement_cohort.close_envelope is not None:
        custody.files.save(measurement.root / "original-close-envelope.json", measurement_cohort.close_envelope, "provider-close-envelope")
    require(result == 0, "d28_original_measurement_failed")
    measurement_export, _ = measurement_cohort.export()
    export_ref = custody.files.save(measurement.root / "provider-export.json", {
        "format": "recall-d28-measurement-provider-export/v2", "reference": measurement_export["reference"],
        "files": measurement_export["files"], "provider_accounting_channel": measurement_cohort.channel_reference}, "measurement-provider-export")
    return {"format": "recall-d28-component-run/v2", "runtime_component": custody.component_reference,
            "prepared_component": component_ref, "measurement_observation": observed,
            "measurement_provider_export": export_ref,
            "product_acceptance": False, "provider_request_association": "unmet"}

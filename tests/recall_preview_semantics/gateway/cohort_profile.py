"""Closed qualification allocations. These source pins never authorize a run.

Only the two exact profiles below exist. Manifest values cannot supply limits.
This host-only module is pinned by the complete gateway source inventory.
The v2 child validates its anchor through the existing pinned context helper.
"""
from __future__ import annotations

from typing import NamedTuple


class CohortProfile(NamedTuple):
    profile_id: str
    accounting_policy: str
    manifest_format: str
    count_cap: int
    money_cap: int
    capture_cap: int
    connection_slots: int
    public_queries: int
    measurement: tuple[int, int, int, int]
    allocation_sha256: str
    allocation_content_sha256: str
    contract_sha256: str
    corpus_sha256: str
    preparation_sha256: str
    typed_delta_sha256: str

    @property
    def time_cap(self):
        return 7800

    def phase_limits(self, phase):
        if phase == "preparation":
            return (450, 225, 2, 3600)
        if phase == "measurement":
            return self.measurement
        raise ValueError("unknown_cohort_phase")

    @property
    def typed_pins(self):
        return {"preparation_contract": self.preparation_sha256,
                "typed_delta": self.typed_delta_sha256}


D28 = CohortProfile(
    "d28-v1", "separate-exclusive-workload-embedding-reservation/v1",
    "recall-exclusive-embedding-manifest/v1", 2100, 3_000_000_000,
    20 * 1024**3, 64, 200, (600, 300, 50, 120),
    "0a54fcd4c6359e39611567a06857f2d61975b3f19a80052f6ea49691f6d1633f",
    "4abbb62aba6656d134f183841116fb0af2a09752e899a3088748a5b4f51c966a",
    "696e0fa0f8736306d6e66f5ce26658a21fb013255e5727b5287406008c350a5a",
    "2192437954109fc17ebbea5b3960eb6b398a2ee91a725ce70666b119e86179dc",
    "b1fd4c43e1c8c506ca13e261a29d65a7d06246bf11ef53c02e97945cd3ef56d1",
    "51b34aec951d783a93b903a1468fba9a8833ba2f31f705bd784c66c9b26688f8",
)

SEMANTIC140 = CohortProfile(
    "semantic140-v1", "separate-exclusive-semantic140-embedding-reservation/v1",
    "recall-semantic140-embedding-manifest/v1", 4260, 5_000_000_000,
    40 * 1024**3, 160, 560, (1680, 840, 140, 120),
    "e42df2638349fea37f94eb7f9ea79d2afad170a2d36a655869a40f25f83dcd01",
    "34e0714f0a902e404f0cebd96ae1656f88fdbfe9b5be2d7b8d184a4004deffbe",
    "d334f2c3fcddd134f4765704bbee91e0d917bab9865fc7c27ff19df7942adaa8",
    "e3201eb81bc294391e445a431a847f88603ec0631e23b6068d6f44cf0944ff0f",
    "6129328500aa899d4995c9ff078571176af1eeb8764b78681531581418e0bc4c",
    "e1e9bf38df83d8afdf881924e55847aaffa6174b6ad01ea8e49ebc2a4ff43a5d",
)


def profile_for_id(profile_id):
    if profile_id == D28.profile_id:
        return D28
    if profile_id == SEMANTIC140.profile_id:
        return SEMANTIC140
    raise ValueError("unknown_cohort_profile")

"""Independent closed semantic140 expectations; no dispatch or producer imports.

These literal identities are reviewed before execution. Caller-provided quotas,
helper profile objects, and collected verdicts cannot replace them. The original
D28 constants remain in their original validators.
"""
from __future__ import annotations

PROFILE_ID = "semantic140-v1"
ORIGINAL_CORPUS_SHA256 = "2192437954109fc17ebbea5b3960eb6b398a2ee91a725ce70666b119e86179dc"
FROZEN_INPUTS = {
    "corpus": "e3201eb81bc294391e445a431a847f88603ec0631e23b6068d6f44cf0944ff0f",
    "workload_contract": "d334f2c3fcddd134f4765704bbee91e0d917bab9865fc7c27ff19df7942adaa8",
    "preparation_contract": "6129328500aa899d4995c9ff078571176af1eeb8764b78681531581418e0bc4c",
    "rendered_map": "dff61fd6ecb1cd6a8916df4f1cee7918ad1a106fc7ef5df4a564f8312b9e26fc",
    "provider_allocation": "e42df2638349fea37f94eb7f9ea79d2afad170a2d36a655869a40f25f83dcd01",
    "typed_delta": "e1e9bf38df83d8afdf881924e55847aaffa6174b6ad01ea8e49ebc2a4ff43a5d",
}
CONTRACT_CANONICAL_SHA256 = "6518d9f3d6b46afc28c7a87170de567a81e31f3d754966b5e18c08fe590644a2"
SCHEDULE_SHA256 = "7ac6c3930f63c90c59312a81d131814ef2653fe7f3c51b38f921fbef5739f8b3"
PHYSICAL_ATTEMPTS = 4260
MAXIMUM_NANO_USD = 5_000_000_000
FIRST_BACKEND_ATTEMPTS = 2130
FIRST_BACKEND_ATTEMPTS_PER_SPACE = 1065
GLOBAL_CAPTURE_BYTES = 40 * 1024**3
# A channel exports one cohort. A larger total allocation does not widen it.
PROVIDER_EXPORT_BYTES = 20 * 1024**3
PROVIDER_EXPORT_FILES = 10532
RUNTIME_RAW_BYTES = 2 * 1024**3
OBSERVATION_BYTES = 512 * 1024**2


def is_semantic140(allocation_sha256):
    return type(allocation_sha256) is str and allocation_sha256 == FROZEN_INPUTS["provider_allocation"]


def require_identity(identity):
    """Reject mixing any original or arbitrary input with the new allocation."""
    if type(identity) is not dict or any(identity.get(key + "_sha256") != value
                                        for key, value in FROZEN_INPUTS.items()):
        raise ValueError("dynamic_semantic140_input_identity")


def caps():
    return {"physical_dispatches": PHYSICAL_ATTEMPTS, "maximum_nano_usd": MAXIMUM_NANO_USD}

from __future__ import annotations

import importlib
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _vendor_root() -> Path:
    return _repo_root() / "platform" / "vendor" / "turboquant_plus"


def _ensure_vendor_import_path() -> Path:
    root = _vendor_root()
    root_text = str(root)
    if root_text not in sys.path:
        sys.path.insert(0, root_text)
    return root


def _load_turboquant_module():
    root = _ensure_vendor_import_path()
    if not root.exists():
        return None, "vendor_root_missing", root
    try:
        module = importlib.import_module("turboquant")
    except Exception as exc:  # pragma: no cover - surfaced through diagnostics
        return None, f"{type(exc).__name__}: {exc}", root
    return module, "", root


def build_turboquant_reference_payload():
    module, error, vendor_root = _load_turboquant_module()
    payload = {
        "bundled": vendor_root.exists(),
        "available": False,
        "roundTripVerified": False,
        "library": "turboquant_plus",
        "productRole": "reference_validation",
        "usedForLiveInference": False,
        "requiresCompatibleBackend": True,
        "compatibleBackendFamilies": ["llama_cpp_style"],
        "liveAccelerationStatus": "requires_compatible_backend",
        "bundlePath": str(vendor_root),
        "license": "Apache-2.0",
        "sourceRepo": "https://github.com/TheTom/turboquant_plus",
        "variantIds": ["turbo3", "turbo4"],
        "algorithms": [],
        "summary": (
            "Vendored TurboQuant reference algorithm is available for local verification. "
            "A compatible llama.cpp-style backend is still required for live runtime acceleration, "
            "and the vendored reference is not used directly for live inference."
        ),
        "error": str(error or "").strip(),
    }
    if module is None:
        return payload
    payload["algorithms"] = [
        name
        for name in ("PolarQuant", "QJL", "TurboQuant", "TurboQuantMSE", "KVCacheCompressor")
        if hasattr(module, name)
    ]
    try:
        import numpy as np

        vector = np.linspace(-1.0, 1.0, 8, dtype=np.float64)
        quantizer = module.TurboQuant(d=8, bit_width=3, seed=42)
        compressed = quantizer.quantize(vector)
        restored = quantizer.dequantize(compressed)

        k_cache = np.arange(16, dtype=np.float64).reshape(1, 1, 2, 8)
        v_cache = np.arange(16, dtype=np.float64).reshape(1, 1, 2, 8) / 10.0
        compressor = module.KVCacheCompressor(head_dim=8, k_bits=3, v_bits=3, seed=42)
        kv_compressed = compressor.compress(k_cache, v_cache)
        restored_k, restored_v = compressor.decompress(kv_compressed)
        payload["compressionPreview"] = compressor.memory_stats(seq_len=2, num_layers=1, num_heads=1)
        payload["available"] = True
        payload["roundTripVerified"] = (
            restored.shape == vector.shape
            and restored_k.shape == k_cache.shape
            and restored_v.shape == v_cache.shape
        )
        payload["summary"] = (
            "Vendored TurboQuant reference algorithm is bundled and passed a local round-trip smoke check. "
            "Live inference acceleration still depends on a compatible local backend, "
            "and the vendored reference remains a validation-only asset."
        )
        payload["error"] = ""
    except Exception as exc:  # pragma: no cover - reported into runtime diagnostics
        payload["error"] = f"{type(exc).__name__}: {exc}"
    return payload

from __future__ import annotations

import io
import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Optional


def _normalize_thread_budget(raw_value: str, default: int = 2) -> int:
    try:
        normalized = int(str(raw_value or "").strip() or default)
    except (TypeError, ValueError):
        return default
    return max(1, min(normalized, 8))


SHERPA_ONNX_TTS_THREAD_BUDGET = _normalize_thread_budget(
    os.environ.get("SHERPA_ONNX_TTS_NUM_THREADS") or os.environ.get("SHERPA_ONNX_TTS_MAX_THREADS") or "2"
)
for env_key in (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "NUMEXPR_NUM_THREADS",
):
    os.environ.setdefault(env_key, str(SHERPA_ONNX_TTS_THREAD_BUDGET))
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
os.environ.setdefault("HF_HUB_DISABLE_TELEMETRY", "1")

import numpy as np
import sherpa_onnx
import soundfile as sf
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, Response
from huggingface_hub import snapshot_download
from pydantic import BaseModel, Field


APP = FastAPI(title="Mission Control sherpa-onnx + kokoro", version="1.0.0")
LOGGER = logging.getLogger("mission-control.sherpa-onnx-kokoro")

SHERPA_ONNX_TTS_MODEL_REPO = str(
    os.environ.get("SHERPA_ONNX_TTS_MODEL_REPO") or "csukuangfj/kokoro-int8-multi-lang-v1_1"
).strip()
SHERPA_ONNX_TTS_MODEL_DIR = str(
    os.environ.get("SHERPA_ONNX_TTS_MODEL_DIR") or "/data/openclaw/runtime/sherpa-onnx-tts/kokoro-int8-multi-lang-v1_1"
).strip()
SHERPA_ONNX_TTS_MODEL_ID = str(
    os.environ.get("SHERPA_ONNX_TTS_MODEL_ID") or "kokoro-multi-lang-v1_1"
).strip() or "kokoro-multi-lang-v1_1"
SHERPA_ONNX_TTS_DEFAULT_SPEAKER = str(os.environ.get("SHERPA_ONNX_TTS_DEFAULT_SPEAKER") or "zf_001").strip().lower() or "zf_001"
SHERPA_ONNX_TTS_DEFAULT_SPEED = float(str(os.environ.get("SHERPA_ONNX_TTS_DEFAULT_SPEED") or "1.0").strip() or "1.0")
SHERPA_ONNX_TTS_NUM_THREADS = _normalize_thread_budget(
    os.environ.get("SHERPA_ONNX_TTS_NUM_THREADS") or str(SHERPA_ONNX_TTS_THREAD_BUDGET)
)
SHERPA_ONNX_TTS_PROVIDER = str(os.environ.get("SHERPA_ONNX_TTS_PROVIDER") or "cpu").strip() or "cpu"
SHERPA_ONNX_TTS_LANGUAGE = str(os.environ.get("SHERPA_ONNX_TTS_LANGUAGE") or "").strip()

SHERPA_ONNX_TTS_SPEAKERS = [
    {"id": "zf_001", "sid": 3, "label": "中文女 1 · 温和"},
    {"id": "zf_002", "sid": 4, "label": "中文女 2 · 明亮"},
    {"id": "zf_003", "sid": 5, "label": "中文女 3 · 柔和"},
    {"id": "zf_004", "sid": 6, "label": "中文女 4 · 清晰"},
    {"id": "zf_005", "sid": 7, "label": "中文女 5 · 沉稳"},
    {"id": "zf_006", "sid": 8, "label": "中文女 6 · 轻快"},
    {"id": "zf_017", "sid": 11, "label": "中文女 7 · 亲和"},
    {"id": "zf_018", "sid": 12, "label": "中文女 8 · 活泼"},
    {"id": "zm_009", "sid": 58, "label": "中文男 1 · 稳重"},
    {"id": "zm_010", "sid": 59, "label": "中文男 2 · 自然"},
    {"id": "zm_011", "sid": 60, "label": "中文男 3 · 清晰"},
    {"id": "zm_012", "sid": 61, "label": "中文男 4 · 温厚"},
    {"id": "zm_013", "sid": 62, "label": "中文男 5 · 年轻"},
]
SHERPA_ONNX_TTS_VOICE_ALIASES = {
    "alloy": "zf_001",
    "ash": "zm_009",
    "ballad": "zf_002",
    "cedar": "zf_003",
    "coral": "zf_004",
    "echo": "zm_010",
    "fable": "zf_005",
    "marin": "zf_006",
    "nova": "zf_017",
    "onyx": "zm_011",
    "sage": "zm_012",
    "shimmer": "zf_018",
    "verse": "zm_013",
    "zh": "zf_001",
    "zh-cn": "zf_001",
    "中文女": "zf_001",
    "中文男": "zm_009",
}


class SpeechRequest(BaseModel):
    model: str = Field(default=SHERPA_ONNX_TTS_MODEL_ID)
    input: str = Field(default="")
    voice: str = Field(default=SHERPA_ONNX_TTS_DEFAULT_SPEAKER)
    response_format: str = Field(default="wav")
    speed: Optional[float] = Field(default=None)
    instructions: str = Field(default="")


def _speaker_map():
    return {item["id"]: item for item in SHERPA_ONNX_TTS_SPEAKERS}


def _speaker_labels():
    return {item["id"]: item["label"] for item in SHERPA_ONNX_TTS_SPEAKERS}


def _available_speaker_ids():
    return [item["id"] for item in SHERPA_ONNX_TTS_SPEAKERS]


def resolve_speaker(voice: str) -> dict[str, object]:
    normalized = str(voice or "").strip().lower()
    speaker_id = normalized
    if speaker_id not in _speaker_map():
        speaker_id = SHERPA_ONNX_TTS_VOICE_ALIASES.get(normalized, SHERPA_ONNX_TTS_DEFAULT_SPEAKER)
    return _speaker_map().get(speaker_id, _speaker_map()[SHERPA_ONNX_TTS_DEFAULT_SPEAKER])


def _find_first(base: Path, *patterns: str) -> Path:
    for pattern in patterns:
        matches = sorted(base.glob(pattern))
        if matches:
            return matches[0]
    raise FileNotFoundError(f"Unable to find model asset in {base}: {patterns}")


def _ensure_model_assets() -> Path:
    model_dir = Path(SHERPA_ONNX_TTS_MODEL_DIR)
    model_dir.mkdir(parents=True, exist_ok=True)
    try:
        _find_first(model_dir, "model.int8.onnx", "model.onnx")
        _find_first(model_dir, "voices.bin")
        _find_first(model_dir, "tokens.txt")
        _find_first(model_dir, "lexicon-*.txt")
        espeak_dir = model_dir / "espeak-ng-data"
        if espeak_dir.exists():
            return model_dir
    except FileNotFoundError:
        pass
    LOGGER.info("downloading sherpa-onnx kokoro assets %s -> %s", SHERPA_ONNX_TTS_MODEL_REPO, model_dir)
    snapshot_download(
        repo_id=SHERPA_ONNX_TTS_MODEL_REPO,
        local_dir=str(model_dir),
        allow_patterns=[
            "*.onnx",
            "voices.bin",
            "tokens.txt",
            "lexicon-*.txt",
            "espeak-ng-data/*",
        ],
    )
    return model_dir


def _model_paths() -> dict[str, str]:
    model_dir = _ensure_model_assets()
    espeak_dir = model_dir / "espeak-ng-data"
    lexicons = sorted(str(path) for path in model_dir.glob("lexicon-*.txt"))
    if not lexicons:
        raise FileNotFoundError(f"No lexicon files found in {model_dir}")
    return {
        "model": str(_find_first(model_dir, "model.int8.onnx", "model.onnx")),
        "voices": str(_find_first(model_dir, "voices.bin")),
        "tokens": str(_find_first(model_dir, "tokens.txt")),
        "data_dir": str(espeak_dir if espeak_dir.exists() else model_dir),
        "lexicon": ",".join(lexicons),
    }


@lru_cache(maxsize=1)
def get_tts():
    paths = _model_paths()
    config = sherpa_onnx.OfflineTtsConfig(
        model=sherpa_onnx.OfflineTtsModelConfig(
            kokoro=sherpa_onnx.OfflineTtsKokoroModelConfig(
                model=paths["model"],
                voices=paths["voices"],
                tokens=paths["tokens"],
                data_dir=paths["data_dir"],
                lexicon=paths["lexicon"],
                lang=SHERPA_ONNX_TTS_LANGUAGE,
            ),
            num_threads=SHERPA_ONNX_TTS_NUM_THREADS,
            provider=SHERPA_ONNX_TTS_PROVIDER,
            debug=False,
        ),
        max_num_sentences=1,
    )
    if not config.validate():
        raise RuntimeError("Invalid sherpa-onnx TTS configuration.")
    return sherpa_onnx.OfflineTts(config)


def synthesize_audio(text: str, speaker: dict[str, object], speed: float):
    try:
        generated = get_tts().generate(
            text,
            sid=int(speaker["sid"]),
            speed=float(max(0.75, min(speed, 1.35))),
        )
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    samples = np.asarray(generated.samples, dtype=np.float32).reshape(-1)
    if not samples.size:
        raise HTTPException(status_code=500, detail="sherpa-onnx 未生成音频。")
    return samples, int(generated.sample_rate or 24000)


@APP.get("/healthz")
def healthz():
    try:
        get_tts()
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {
        "ok": True,
        "model": SHERPA_ONNX_TTS_MODEL_ID,
        "ready": True,
        "voices": _available_speaker_ids(),
    }


@APP.get("/models")
def models():
    get_tts()
    return JSONResponse(
        {
            "object": "list",
            "data": [
                {
                    "id": SHERPA_ONNX_TTS_MODEL_ID,
                    "object": "model",
                    "owned_by": "mission-control",
                    "voices": _available_speaker_ids(),
                    "voice_labels": _speaker_labels(),
                    "default_voice": SHERPA_ONNX_TTS_DEFAULT_SPEAKER,
                    "custom_voices": [],
                }
            ],
        }
    )


@APP.get("/v1/models")
def models_v1():
    return models()


@APP.post("/v1/audio/speech")
def audio_speech(payload: SpeechRequest):
    text = str(payload.input or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="input 不能为空。")
    speaker = resolve_speaker(payload.voice)
    LOGGER.info("audio_speech requested_voice=%s resolved_voice=%s", payload.voice, speaker["id"])
    speed = payload.speed if payload.speed and payload.speed > 0 else SHERPA_ONNX_TTS_DEFAULT_SPEED
    audio, sample_rate = synthesize_audio(text, speaker, speed)
    buffer = io.BytesIO()
    try:
        sf.write(buffer, audio, sample_rate, format="WAV")
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return Response(buffer.getvalue(), media_type="audio/wav")

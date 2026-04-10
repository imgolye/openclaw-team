from __future__ import annotations

import io
import json
import logging
import os
from functools import lru_cache
from pathlib import Path


def _normalize_thread_budget(raw_value: str, default: int = 4) -> int:
    try:
        normalized = int(str(raw_value or "").strip() or default)
    except (TypeError, ValueError):
        return default
    return max(1, min(normalized, 8))


QWEN3_TTS_THREAD_BUDGET = _normalize_thread_budget(
    os.environ.get("QWEN3_TTS_NUM_THREADS") or os.environ.get("QWEN3_TTS_MAX_THREADS") or "4"
)
for env_key in (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "NUMEXPR_NUM_THREADS",
):
    os.environ.setdefault(env_key, str(QWEN3_TTS_THREAD_BUDGET))
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

import librosa
import numpy as np
import soundfile as sf
import torch
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, Response
from modelscope import snapshot_download
from pydantic import BaseModel, Field
from qwen_tts import Qwen3TTSModel


APP = FastAPI(title="Mission Control Qwen3-TTS", version="1.0.0")
LOGGER = logging.getLogger("mission-control.qwen3-tts")

QWEN3_TTS_MODEL_REF = str(
    os.environ.get("QWEN3_TTS_MODEL_REF") or "Qwen/Qwen3-TTS-12Hz-0.6B-CustomVoice"
).strip()
QWEN3_TTS_MODEL_DIR = str(
    os.environ.get("QWEN3_TTS_MODEL_DIR") or "/models/Qwen3-TTS-12Hz-0.6B-CustomVoice"
).strip()
QWEN3_TTS_MODEL_ID = str(
    os.environ.get("QWEN3_TTS_MODEL_ID") or "qwen3-tts-12hz-0.6b-customvoice"
).strip() or "qwen3-tts-12hz-0.6b-customvoice"
QWEN3_TTS_BASE_MODEL_REF = str(
    os.environ.get("QWEN3_TTS_BASE_MODEL_REF") or "Qwen/Qwen3-TTS-12Hz-0.6B-Base"
).strip()
QWEN3_TTS_BASE_MODEL_DIR = str(
    os.environ.get("QWEN3_TTS_BASE_MODEL_DIR") or "/models/Qwen3-TTS-12Hz-0.6B-Base"
).strip()
QWEN3_TTS_BASE_MODEL_ID = str(
    os.environ.get("QWEN3_TTS_BASE_MODEL_ID") or "qwen3-tts-12hz-0.6b-base"
).strip() or "qwen3-tts-12hz-0.6b-base"
QWEN3_TTS_TOKENIZER_REF = str(
    os.environ.get("QWEN3_TTS_TOKENIZER_REF") or "Qwen/Qwen3-TTS-Tokenizer-12Hz"
).strip()
QWEN3_TTS_DEFAULT_SPEAKER = str(os.environ.get("QWEN3_TTS_DEFAULT_SPEAKER") or "serena").strip().lower() or "serena"
QWEN3_TTS_DEFAULT_SPEED = float(str(os.environ.get("QWEN3_TTS_DEFAULT_SPEED") or "1.0").strip() or "1.0")
QWEN3_TTS_DEVICE_MAP = str(os.environ.get("QWEN3_TTS_DEVICE_MAP") or "cpu").strip() or "cpu"
QWEN3_TTS_DTYPE = str(os.environ.get("QWEN3_TTS_DTYPE") or "float32").strip().lower() or "float32"
QWEN3_TTS_ATTN_IMPLEMENTATION = str(os.environ.get("QWEN3_TTS_ATTN_IMPLEMENTATION") or "").strip()
QWEN3_TTS_MODELSCOPE_CACHE = str(os.environ.get("QWEN3_TTS_MODELSCOPE_CACHE") or "/models/.cache").strip()
QWEN3_TTS_CUSTOM_SPEAKER_DIR = str(
    os.environ.get("QWEN3_TTS_CUSTOM_SPEAKER_DIR") or "/data/openclaw/runtime/customer-voice-speakers"
).strip()
QWEN3_TTS_CUSTOM_PREFIX = str(os.environ.get("QWEN3_TTS_CUSTOM_PREFIX") or "custom:").strip() or "custom:"
QWEN3_TTS_CUSTOM_PROMPT_FILENAME = str(os.environ.get("QWEN3_TTS_CUSTOM_PROMPT_FILENAME") or "prompt.txt").strip() or "prompt.txt"
QWEN3_TTS_DEFAULT_INSTRUCTIONS = str(
    os.environ.get("QWEN3_TTS_DEFAULT_INSTRUCTIONS")
    or "请用自然、口语化、像真人同事发语音一样的方式说话，保留轻微情绪起伏和停顿，不要像机器播报。"
).strip()
QWEN3_TTS_NUM_THREADS = _normalize_thread_budget(
    os.environ.get("QWEN3_TTS_NUM_THREADS") or str(QWEN3_TTS_THREAD_BUDGET)
)
QWEN3_TTS_INTEROP_THREADS = _normalize_thread_budget(
    os.environ.get("QWEN3_TTS_INTEROP_THREADS") or "1",
    default=1,
)

torch.set_num_threads(QWEN3_TTS_NUM_THREADS)
try:
    torch.set_num_interop_threads(min(QWEN3_TTS_INTEROP_THREADS, QWEN3_TTS_NUM_THREADS))
except RuntimeError:
    pass
torch.set_grad_enabled(False)

QWEN3_TTS_SPEAKER_ORDER = [
    "serena",
    "vivian",
    "uncle_fu",
    "dylan",
    "eric",
    "sohee",
    "ono_anna",
    "aiden",
    "ryan",
]
QWEN3_TTS_SPEAKER_LABELS = {
    "serena": "Serena · 中文温和女声",
    "vivian": "Vivian · 中文明亮女声",
    "uncle_fu": "Uncle_Fu · 中文低沉男声",
    "dylan": "Dylan · 北京男声",
    "eric": "Eric · 成都男声",
    "sohee": "Sohee · 韩语女声",
    "ono_anna": "Ono_Anna · 日语女声",
    "aiden": "Aiden · 英文男声",
    "ryan": "Ryan · 英文动感男声",
}
QWEN3_TTS_VOICE_ALIASES = {
    "alloy": "serena",
    "ash": "uncle_fu",
    "ballad": "serena",
    "cedar": "serena",
    "coral": "serena",
    "echo": "uncle_fu",
    "fable": "vivian",
    "marin": "serena",
    "nova": "serena",
    "onyx": "uncle_fu",
    "sage": "uncle_fu",
    "shimmer": "serena",
    "verse": "vivian",
    "zh": "serena",
    "zh-cn": "serena",
    "中文女": "serena",
    "中文男": "uncle_fu",
    "粤语女": "vivian",
}


class SpeechRequest(BaseModel):
    model: str = Field(default=QWEN3_TTS_MODEL_ID)
    input: str = Field(default="")
    voice: str = Field(default=QWEN3_TTS_DEFAULT_SPEAKER)
    response_format: str = Field(default="wav")
    speed: float | None = Field(default=None)
    instructions: str = Field(default="")


def _torch_dtype():
    mapping = {
        "float16": torch.float16,
        "fp16": torch.float16,
        "bfloat16": torch.bfloat16,
        "bf16": torch.bfloat16,
        "float32": torch.float32,
        "fp32": torch.float32,
    }
    return mapping.get(QWEN3_TTS_DTYPE, torch.float32)


def _model_load_kwargs():
    kwargs = {
        "local_files_only": True,
        "device_map": QWEN3_TTS_DEVICE_MAP,
        "dtype": _torch_dtype(),
    }
    if QWEN3_TTS_ATTN_IMPLEMENTATION:
        kwargs["attn_implementation"] = QWEN3_TTS_ATTN_IMPLEMENTATION
    return kwargs


def _ensure_model_assets(model_ref: str, model_dir_value: str):
    model_dir = Path(model_dir_value)
    tokenizer_dir = model_dir / "speech_tokenizer"
    model_dir.mkdir(parents=True, exist_ok=True)
    tokenizer_dir.mkdir(parents=True, exist_ok=True)
    required_model_files = [
        model_dir / "config.json",
        model_dir / "configuration.json",
        model_dir / "generation_config.json",
        model_dir / "model.safetensors",
    ]
    required_tokenizer_files = [
        tokenizer_dir / "config.json",
        tokenizer_dir / "configuration.json",
        tokenizer_dir / "preprocessor_config.json",
        tokenizer_dir / "model.safetensors",
    ]
    if not all(path.exists() for path in required_model_files):
        LOGGER.info("downloading qwen3-tts model %s -> %s", model_ref, model_dir)
        snapshot_download(
            model_ref,
            cache_dir=QWEN3_TTS_MODELSCOPE_CACHE,
            local_dir=str(model_dir),
        )
    if not all(path.exists() for path in required_tokenizer_files):
        LOGGER.info("downloading qwen3-tts tokenizer %s -> %s", QWEN3_TTS_TOKENIZER_REF, tokenizer_dir)
        snapshot_download(
            QWEN3_TTS_TOKENIZER_REF,
            cache_dir=QWEN3_TTS_MODELSCOPE_CACHE,
            local_dir=str(tokenizer_dir),
        )


@lru_cache(maxsize=1)
def get_custom_voice_model():
    _ensure_model_assets(QWEN3_TTS_MODEL_REF, QWEN3_TTS_MODEL_DIR)
    return Qwen3TTSModel.from_pretrained(
        QWEN3_TTS_MODEL_DIR,
        **_model_load_kwargs(),
    )


@lru_cache(maxsize=1)
def get_base_voice_clone_model():
    _ensure_model_assets(QWEN3_TTS_BASE_MODEL_REF, QWEN3_TTS_BASE_MODEL_DIR)
    return Qwen3TTSModel.from_pretrained(
        QWEN3_TTS_BASE_MODEL_DIR,
        **_model_load_kwargs(),
    )


@lru_cache(maxsize=1)
def available_builtin_speakers():
    model = get_custom_voice_model()
    supported = [str(item or "").strip().lower() for item in (model.get_supported_speakers() or []) if str(item or "").strip()]
    seen = set()
    ordered = []
    for candidate in [*QWEN3_TTS_SPEAKER_ORDER, *supported]:
        normalized = str(candidate or "").strip().lower()
        if not normalized or normalized in seen:
            continue
        if normalized in supported:
            ordered.append(normalized)
            seen.add(normalized)
    return ordered


def _custom_speaker_root():
    return Path(QWEN3_TTS_CUSTOM_SPEAKER_DIR)


def _custom_speaker_agent_id(voice_id: str):
    normalized = str(voice_id or "").strip()
    if not normalized.startswith(QWEN3_TTS_CUSTOM_PREFIX):
        return ""
    return normalized[len(QWEN3_TTS_CUSTOM_PREFIX) :].strip()


def _custom_speaker_payload(voice_id: str):
    agent_id = _custom_speaker_agent_id(voice_id)
    if not agent_id:
        return {}
    sample_dir = _custom_speaker_root() / agent_id
    if not sample_dir.exists():
        return {}
    prompt_path = sample_dir / QWEN3_TTS_CUSTOM_PROMPT_FILENAME
    if not prompt_path.exists():
        return {}
    prompt_text = str(prompt_path.read_text(encoding="utf-8").strip() or "").strip()
    if not prompt_text:
        return {}
    sample_files = [
        item
        for item in sorted(sample_dir.iterdir())
        if item.is_file() and item.name != QWEN3_TTS_CUSTOM_PROMPT_FILENAME
    ]
    if not sample_files:
        return {}
    sample_path = sample_files[0]
    return {
        "voiceId": f"{QWEN3_TTS_CUSTOM_PREFIX}{agent_id}",
        "agentId": agent_id,
        "samplePath": str(sample_path),
        "promptText": prompt_text,
        "sampleMtimeNs": int(sample_path.stat().st_mtime_ns),
        "promptMtimeNs": int(prompt_path.stat().st_mtime_ns),
    }


@lru_cache(maxsize=64)
def _custom_voice_clone_prompt(sample_path: str, prompt_text: str, sample_mtime_ns: int, prompt_mtime_ns: int):
    del sample_mtime_ns, prompt_mtime_ns
    model = get_base_voice_clone_model()
    return model.create_voice_clone_prompt(
        ref_audio=sample_path,
        ref_text=prompt_text,
        x_vector_only_mode=False,
    )


def custom_speaker_ids():
    root = _custom_speaker_root()
    if not root.exists():
        return []
    speakers = []
    for item in sorted(root.iterdir()):
        if not item.is_dir():
            continue
        voice_id = f"{QWEN3_TTS_CUSTOM_PREFIX}{item.name}"
        if _custom_speaker_payload(voice_id):
            speakers.append(voice_id)
    return speakers


def available_speakers():
    return [*available_builtin_speakers(), *custom_speaker_ids()]


@lru_cache(maxsize=1)
def _speaker_labels():
    labels = {speaker: QWEN3_TTS_SPEAKER_LABELS.get(speaker, speaker) for speaker in available_builtin_speakers()}
    for speaker in custom_speaker_ids():
        labels[speaker] = speaker
    return labels


def resolve_speaker(requested_voice: str):
    normalized = str(requested_voice or "").strip().lower()
    if _custom_speaker_payload(normalized):
        return normalized
    speakers = available_builtin_speakers()
    if not speakers:
        raise RuntimeError("Qwen3-TTS 未返回可用 speaker。")
    if normalized in speakers:
        return normalized
    alias = QWEN3_TTS_VOICE_ALIASES.get(normalized)
    if alias and alias in speakers:
        return alias
    if QWEN3_TTS_DEFAULT_SPEAKER in speakers:
        return QWEN3_TTS_DEFAULT_SPEAKER
    return speakers[0]


def detect_language(text: str):
    normalized = str(text or "").strip()
    if not normalized:
        return "Auto"
    if any("\u4e00" <= char <= "\u9fff" for char in normalized):
        return "Chinese"
    if any("\u3040" <= char <= "\u30ff" for char in normalized):
        return "Japanese"
    if any("\uac00" <= char <= "\ud7af" for char in normalized):
        return "Korean"
    return "Auto"


def apply_speed(audio: np.ndarray, speed: float, sample_rate: int):
    normalized_speed = float(speed or 1.0)
    if abs(normalized_speed - 1.0) < 0.01:
        return audio
    try:
        stretched = librosa.effects.time_stretch(audio.astype(np.float32), rate=normalized_speed)
        if isinstance(stretched, np.ndarray) and stretched.size:
            return stretched.astype(np.float32)
    except Exception:
        LOGGER.exception("failed to apply qwen3-tts speed stretch")
    return audio


def synthesize_audio(text: str, speaker: str, speed: float, instructions: str = ""):
    try:
        language = detect_language(text)
        normalized_speaker = resolve_speaker(speaker)
        LOGGER.info(
            "qwen3_tts synthesize speaker=%s language=%s speed=%s chars=%s",
            normalized_speaker,
            language,
            speed,
            len(str(text or "").strip()),
        )
        custom_payload = _custom_speaker_payload(normalized_speaker)
        if custom_payload:
            model = get_base_voice_clone_model()
            clone_prompt = _custom_voice_clone_prompt(
                str(custom_payload.get("samplePath") or ""),
                str(custom_payload.get("promptText") or ""),
                int(custom_payload.get("sampleMtimeNs") or 0),
                int(custom_payload.get("promptMtimeNs") or 0),
            )
            wavs, sample_rate = model.generate_voice_clone(
                text=text,
                language=language,
                voice_clone_prompt=clone_prompt,
                do_sample=True,
                top_p=0.9,
                temperature=0.6,
                repetition_penalty=1.05,
            )
        else:
            model = get_custom_voice_model()
            normalized_instructions = str(instructions or "").strip() or QWEN3_TTS_DEFAULT_INSTRUCTIONS
            wavs, sample_rate = model.generate_custom_voice(
                text=text,
                language=language,
                speaker=normalized_speaker,
                instruct=normalized_instructions,
                do_sample=True,
                top_p=0.9,
                temperature=0.6,
                repetition_penalty=1.05,
            )
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not wavs:
        raise HTTPException(status_code=500, detail="Qwen3-TTS 未生成音频。")
    audio = np.asarray(wavs[0], dtype=np.float32).reshape(-1)
    if not audio.size:
        raise HTTPException(status_code=500, detail="Qwen3-TTS 返回了空音频。")
    return apply_speed(audio, speed, int(sample_rate or 24000)), int(sample_rate or 24000)


@APP.get("/healthz")
def healthz():
    try:
        get_custom_voice_model()
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"ok": True, "model": QWEN3_TTS_MODEL_ID, "ready": True}


@APP.get("/models")
def models():
    try:
        speakers = available_speakers()
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return JSONResponse(
        {
            "object": "list",
            "data": [
                {
                    "id": QWEN3_TTS_MODEL_ID,
                    "object": "model",
                    "owned_by": "mission-control",
                    "voices": speakers,
                    "voice_labels": _speaker_labels(),
                    "default_voice": resolve_speaker(QWEN3_TTS_DEFAULT_SPEAKER),
                    "custom_voices": custom_speaker_ids(),
                    "voice_clone_model": QWEN3_TTS_BASE_MODEL_ID,
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
    LOGGER.info("audio_speech requested_voice=%s resolved_voice=%s", payload.voice, speaker)
    speed = payload.speed if payload.speed and payload.speed > 0 else QWEN3_TTS_DEFAULT_SPEED
    audio, sample_rate = synthesize_audio(text, speaker, speed, instructions=payload.instructions)
    buffer = io.BytesIO()
    try:
        sf.write(buffer, audio, sample_rate, format="WAV")
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return Response(buffer.getvalue(), media_type="audio/wav")


@APP.get("/debug/speakers")
def debug_speakers():
    return {
        "model": QWEN3_TTS_MODEL_ID,
        "modelRef": QWEN3_TTS_MODEL_REF,
        "speakers": available_speakers(),
        "voiceLabels": _speaker_labels(),
    }

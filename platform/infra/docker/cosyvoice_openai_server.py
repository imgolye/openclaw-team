from __future__ import annotations

import io
import logging
import os
import builtins
from functools import lru_cache
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import soundfile as sf
import torch
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field


APP = FastAPI(title="Mission Control CosyVoice", version="1.0.0")
LOGGER = logging.getLogger("mission-control.cosyvoice")

COSYVOICE_MODEL_DIR = str(os.environ.get("COSYVOICE_MODEL_DIR") or "/models/CosyVoice-300M-Instruct").strip()
COSYVOICE_MODEL_ID = str(os.environ.get("COSYVOICE_MODEL_ID") or "cosyvoice-300m-instruct").strip() or "cosyvoice-300m-instruct"
COSYVOICE_SPEED = float(str(os.environ.get("COSYVOICE_SPEED") or "1.0").strip() or "1.0")
COSYVOICE_TEXT_FRONTEND = str(os.environ.get("COSYVOICE_TEXT_FRONTEND") or "0").strip().lower() not in {"0", "false", "no", "off"}
COSYVOICE_CUSTOM_SPEAKERS_DIR = str(os.environ.get("COSYVOICE_CUSTOM_SPEAKERS_DIR") or "/data/openclaw/runtime/customer-voice-speakers").strip()
COSYVOICE_DEFAULT_INSTRUCTIONS = str(
    os.environ.get("COSYVOICE_DEFAULT_INSTRUCTIONS")
    or "请像真人在微信里发语音一样自然说话，语气温和、口语化、有轻微情绪起伏，不要像播报器或机器朗读。"
).strip()
CUSTOM_VOICE_PREFIX = "custom:"

VOICE_ALIASES = {
    "alloy": "中文女",
    "ash": "中文男",
    "ballad": "中文女",
    "cedar": "中文女",
    "coral": "中文女",
    "echo": "中文男",
    "fable": "中文男",
    "marin": "中文女",
    "nova": "中文女",
    "onyx": "中文男",
    "sage": "中文男",
    "shimmer": "中文女",
    "verse": "中文男",
}
CUSTOM_SPEAKER_CACHE = {}


class SpeechRequest(BaseModel):
    model: str = Field(default=COSYVOICE_MODEL_ID)
    input: str = Field(default="")
    voice: str = Field(default="中文女")
    response_format: str = Field(default="wav")
    speed: float | None = Field(default=None)
    instructions: str = Field(default="")


@lru_cache(maxsize=1)
def get_tts_model():
    with disabled_frontend_imports():
        from cosyvoice.cli.cosyvoice import AutoModel

        return AutoModel(model_dir=COSYVOICE_MODEL_DIR)


@lru_cache(maxsize=1)
def speaker_catalog():
    spk2info_path = os.path.join(COSYVOICE_MODEL_DIR, "spk2info.pt")
    if not os.path.exists(spk2info_path):
        return []
    try:
        payload = torch.load(spk2info_path, map_location="cpu", weights_only=True)
    except Exception:
        return []
    if not isinstance(payload, dict):
        return []
    speakers = [str(item).strip() for item in payload.keys() if str(item).strip()]
    seen = set()
    ordered = []
    for item in speakers:
        if item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


@contextmanager
def disabled_frontend_imports():
    if COSYVOICE_TEXT_FRONTEND:
        yield
        return
    original_import = builtins.__import__

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        normalized = str(name or "")
        if normalized == "wetext" or normalized.startswith("wetext.") or normalized == "ttsfrd" or normalized.startswith("ttsfrd."):
            raise ImportError(f"{normalized} disabled by COSYVOICE_TEXT_FRONTEND=0")
        return original_import(name, globals, locals, fromlist, level)

    builtins.__import__ = guarded_import
    try:
        yield
    finally:
        builtins.__import__ = original_import


def available_speakers():
    speakers = builtin_speakers()
    seen = set()
    ordered = []
    for item in speakers:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    for item in custom_speaker_ids():
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def builtin_speakers():
    speakers = speaker_catalog()
    if not speakers:
        model = get_tts_model()
        try:
            speakers = [str(item).strip() for item in model.list_available_spks() if str(item).strip()]
        except Exception:
            speakers = []
    seen = set()
    ordered = []
    for item in speakers:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def custom_speaker_root():
    path = Path(COSYVOICE_CUSTOM_SPEAKERS_DIR).expanduser()
    path.mkdir(parents=True, exist_ok=True)
    return path


def custom_speaker_ids():
    root = custom_speaker_root()
    ids = []
    for candidate in sorted(root.iterdir()):
        if not candidate.is_dir():
            continue
        payload = custom_speaker_payload(f"{CUSTOM_VOICE_PREFIX}{candidate.name}")
        if payload:
            ids.append(f"{CUSTOM_VOICE_PREFIX}{candidate.name}")
    return ids


def custom_speaker_payload(speaker_id: str):
    normalized = str(speaker_id or "").strip()
    if not normalized.startswith(CUSTOM_VOICE_PREFIX):
        return {}
    speaker_name = normalized[len(CUSTOM_VOICE_PREFIX) :].strip()
    if not speaker_name:
        return {}
    sample_dir = custom_speaker_root() / speaker_name
    if not sample_dir.exists():
        return {}
    prompt_path = sample_dir / "prompt.txt"
    if not prompt_path.exists():
        return {}
    try:
        prompt_text = prompt_path.read_text(encoding="utf-8").strip()
    except Exception:
        prompt_text = ""
    sample_path = next(
        (
            item
            for item in sorted(sample_dir.iterdir())
            if item.is_file() and item.name != "prompt.txt"
        ),
        None,
    )
    if not prompt_text or sample_path is None:
        return {}
    return {
        "speakerId": normalized,
        "promptText": prompt_text,
        "promptWav": str(sample_path),
        "cacheKey": f"{sample_path.stat().st_mtime_ns}:{prompt_path.stat().st_mtime_ns}",
    }


def ensure_custom_speaker_loaded(speaker_id: str):
    payload = custom_speaker_payload(speaker_id)
    if not payload:
        raise RuntimeError(f"自定义 speaker {speaker_id} 未配置完成。")
    cache_key = str(payload.get("cacheKey") or "").strip()
    if CUSTOM_SPEAKER_CACHE.get(speaker_id) == cache_key:
        return payload
    model = get_tts_model()
    LOGGER.info("load custom speaker %s from %s", speaker_id, payload["promptWav"])
    model.add_zero_shot_spk(payload["promptText"], payload["promptWav"], speaker_id)
    CUSTOM_SPEAKER_CACHE[speaker_id] = cache_key
    return payload


def resolve_speaker(requested_voice: str):
    speakers = available_speakers()
    builtin = builtin_speakers()
    if not speakers:
        raise RuntimeError("CosyVoice 未返回可用 speaker。")
    normalized = str(requested_voice or "").strip()
    if normalized.startswith(CUSTOM_VOICE_PREFIX) and custom_speaker_payload(normalized):
        return normalized
    if normalized in speakers:
        return normalized
    alias = VOICE_ALIASES.get(normalized.lower())
    if alias and alias in builtin:
        return alias
    preferred = next((item for item in builtin if "中文女" in item), "")
    if preferred:
        return preferred
    return builtin[0] if builtin else speakers[0]


def synthesize_audio(text: str, speaker: str, speed: float, instructions: str = ""):
    model = get_tts_model()
    chunks = []
    try:
        normalized_speaker = str(speaker or "").strip()
        custom_payload = ensure_custom_speaker_loaded(normalized_speaker) if normalized_speaker.startswith(CUSTOM_VOICE_PREFIX) else {}
        normalized_instructions = str(instructions or "").strip() or COSYVOICE_DEFAULT_INSTRUCTIONS
        LOGGER.info(
            "synthesize request speaker=%s custom=%s speed=%s chars=%s instruct=%s",
            speaker,
            bool(custom_payload),
            speed,
            len(str(text or "").strip()),
            bool(normalized_instructions),
        )
        if normalized_instructions and hasattr(model, "inference_instruct"):
            inference_iter = model.inference_instruct(
                text,
                normalized_speaker,
                normalized_instructions,
                stream=False,
                speed=speed,
                text_frontend=COSYVOICE_TEXT_FRONTEND,
            )
        elif custom_payload:
            inference_iter = model.inference_zero_shot(
                text,
                custom_payload["promptText"],
                custom_payload["promptWav"],
                zero_shot_spk_id=normalized_speaker,
                stream=False,
                speed=speed,
                text_frontend=COSYVOICE_TEXT_FRONTEND,
            )
        else:
            inference_iter = model.inference_sft(
                text,
                normalized_speaker,
                stream=False,
                speed=speed,
                text_frontend=COSYVOICE_TEXT_FRONTEND,
            )
        for item in inference_iter:
            speech = item.get("tts_speech")
            if speech is None:
                continue
            if hasattr(speech, "detach"):
                speech = speech.detach().cpu().numpy()
            speech = np.asarray(speech, dtype=np.float32).reshape(-1)
            if speech.size:
                chunks.append(speech)
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not chunks:
        raise HTTPException(status_code=500, detail="CosyVoice 未生成音频。")
    return np.concatenate(chunks), int(getattr(model, "sample_rate", 22050) or 22050)


@APP.get("/healthz")
def healthz():
    try:
        speakers = available_speakers()
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"ok": True, "model": COSYVOICE_MODEL_ID, "voices": speakers}


@APP.get("/models")
def models():
    speakers = builtin_speakers()
    custom_voices = custom_speaker_ids()
    default_voice = resolve_speaker("中文女") if speakers else ""
    return JSONResponse(
        {
            "object": "list",
            "data": [
                {
                    "id": COSYVOICE_MODEL_ID,
                    "object": "model",
                    "owned_by": "mission-control",
                    "voices": speakers,
                    "custom_voices": custom_voices,
                    "default_voice": default_voice,
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
    speed = payload.speed if payload.speed and payload.speed > 0 else COSYVOICE_SPEED
    audio, sample_rate = synthesize_audio(text, speaker, speed, instructions=payload.instructions)
    buffer = io.BytesIO()
    try:
        sf.write(buffer, audio, sample_rate, format="WAV")
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return Response(buffer.getvalue(), media_type="audio/wav")

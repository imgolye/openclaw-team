FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1
ENV QWEN3_TTS_MODEL_REF=Qwen/Qwen3-TTS-12Hz-0.6B-CustomVoice
ENV QWEN3_TTS_MODEL_DIR=/models/Qwen3-TTS-12Hz-0.6B-CustomVoice
ENV QWEN3_TTS_MODEL_ID=qwen3-tts-12hz-0.6b-customvoice
ENV QWEN3_TTS_BASE_MODEL_REF=Qwen/Qwen3-TTS-12Hz-0.6B-Base
ENV QWEN3_TTS_BASE_MODEL_DIR=/models/Qwen3-TTS-12Hz-0.6B-Base
ENV QWEN3_TTS_BASE_MODEL_ID=qwen3-tts-12hz-0.6b-base
ENV QWEN3_TTS_TOKENIZER_REF=Qwen/Qwen3-TTS-Tokenizer-12Hz
ENV QWEN3_TTS_PORT=8080
ENV QWEN3_TTS_DEFAULT_SPEED=1.0
ENV QWEN3_TTS_DEVICE_MAP=cpu
ENV QWEN3_TTS_DTYPE=float32
ENV QWEN3_TTS_ATTN_IMPLEMENTATION=
ENV QWEN3_TTS_MODELSCOPE_CACHE=/models/.cache
ENV QWEN3_TTS_CUSTOM_SPEAKER_DIR=/data/openclaw/runtime/customer-voice-speakers
ENV QWEN3_TTS_CUSTOM_PREFIX=custom:
ENV QWEN3_TTS_CUSTOM_PROMPT_FILENAME=prompt.txt

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    ffmpeg \
    git \
    libgomp1 \
    libsndfile1 \
    sox \
  && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir --upgrade pip "setuptools<81" wheel

RUN pip install --no-cache-dir \
  --extra-index-url https://download.pytorch.org/whl/cpu \
  torch==2.5.1 \
  torchaudio==2.5.1

RUN pip install --no-cache-dir \
  fastapi==0.115.6 \
  librosa==0.11.0 \
  modelscope==1.20.0 \
  numpy==1.26.4 \
  pydantic==2.7.0 \
  qwen-tts==0.1.1 \
  soundfile==0.12.1 \
  uvicorn==0.30.0

COPY platform/infra/docker/qwen3_tts_openai_server.py /app/qwen3_tts_openai_server.py

EXPOSE 8080

HEALTHCHECK --interval=20s --timeout=10s --retries=10 --start-period=600s \
  CMD code="$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8080/healthz || true)"; [ "$code" = "200" ]

CMD ["python3", "-m", "uvicorn", "qwen3_tts_openai_server:APP", "--app-dir", "/app", "--host", "0.0.0.0", "--port", "8080"]

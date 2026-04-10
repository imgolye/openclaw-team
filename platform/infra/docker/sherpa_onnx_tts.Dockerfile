FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1
ENV SHERPA_ONNX_TTS_MODEL_REPO=csukuangfj/kokoro-int8-multi-lang-v1_1
ENV SHERPA_ONNX_TTS_MODEL_DIR=/data/openclaw/runtime/sherpa-onnx-tts/kokoro-int8-multi-lang-v1_1
ENV SHERPA_ONNX_TTS_MODEL_ID=kokoro-multi-lang-v1_1
ENV SHERPA_ONNX_TTS_PORT=8080
ENV SHERPA_ONNX_TTS_DEFAULT_SPEED=1.0
ENV SHERPA_ONNX_TTS_DEFAULT_SPEAKER=zf_001
ENV SHERPA_ONNX_TTS_NUM_THREADS=2
ENV SHERPA_ONNX_TTS_PROVIDER=cpu
ENV SHERPA_ONNX_TTS_LANGUAGE=

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    curl \
    libsndfile1 \
  && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir --upgrade pip "setuptools<81" wheel

RUN pip install --no-cache-dir \
  fastapi==0.115.6 \
  huggingface_hub==0.31.1 \
  numpy==1.26.4 \
  pydantic==2.7.0 \
  sherpa-onnx==1.12.34 \
  soundfile==0.12.1 \
  uvicorn==0.30.0

COPY platform/infra/docker/sherpa_onnx_kokoro_openai_server.py /app/sherpa_onnx_kokoro_openai_server.py

EXPOSE 8080

HEALTHCHECK --interval=20s --timeout=10s --retries=10 --start-period=600s \
  CMD code="$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8080/healthz || true)"; [ "$code" = "200" ]

CMD ["python3", "-m", "uvicorn", "sherpa_onnx_kokoro_openai_server:APP", "--app-dir", "/app", "--host", "0.0.0.0", "--port", "8080"]

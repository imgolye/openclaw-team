FROM python:3.10-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1
ENV COSYVOICE_MODEL_DIR=/models/CosyVoice-300M-Instruct
ENV COSYVOICE_MODEL_ID=cosyvoice-300m-instruct
ENV COSYVOICE_PORT=8080
ENV COSYVOICE_SPEED=1.0
ENV COSYVOICE_TEXT_FRONTEND=0

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    ffmpeg \
    git \
    libsndfile1 \
    libsox-dev \
    sox \
  && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir --upgrade pip "setuptools<81" wheel

RUN pip install --no-cache-dir \
  --no-build-isolation \
  --extra-index-url https://download.pytorch.org/whl/cpu \
  torch==2.3.1 \
  torchaudio==2.3.1

RUN pip install --no-cache-dir \
  conformer==0.3.2 \
  diffusers==0.29.0 \
  fastapi==0.115.6 \
  fastapi-cli==0.0.4 \
  gdown==5.1.0 \
  grpcio==1.57.0 \
  grpcio-tools==1.57.0 \
  hydra-core==1.3.2 \
  HyperPyYAML==1.2.3 \
  inflect==7.3.1 \
  librosa==0.10.2 \
  matplotlib==3.7.5 \
  lightning==2.2.4 \
  modelscope==1.20.0 \
  networkx==3.1 \
  numpy==1.26.4 \
  omegaconf==2.3.0 \
  onnx==1.16.0 \
  onnxruntime==1.18.0 \
  protobuf==4.25 \
  pyarrow==18.1.0 \
  pydantic==2.7.0 \
  pyworld==0.3.4 \
  rich==13.7.1 \
  soundfile==0.12.1 \
  tensorboard==2.14.0 \
  transformers==4.51.3 \
  uvicorn==0.30.0 \
  wetext==0.0.4 \
  wget==3.2 \
  x-transformers==2.11.24

# Matcha-TTS imports `lightning` directly during CosyVoice model boot.
RUN pip install --no-cache-dir \
  lightning==2.2.4 \
  lightning-utilities==0.11.3 \
  pytorch-lightning==2.2.4

RUN cat > /tmp/pkg_resources.py <<'PY'
from packaging.requirements import Requirement


def parse_requirements(lines):
    for raw in lines:
        line = str(raw).strip()
        if not line or line.startswith("#"):
            continue
        yield Requirement(line)
PY

RUN PYTHONPATH=/tmp pip install --no-cache-dir --no-build-isolation openai-whisper==20231117

RUN git clone --depth=1 --recursive https://github.com/FunAudioLLM/CosyVoice.git /opt/CosyVoice

ENV PYTHONPATH="/opt/CosyVoice:/opt/CosyVoice/third_party/Matcha-TTS"

RUN python3 - <<'PY'
from modelscope import snapshot_download

snapshot_download(
    'iic/CosyVoice-300M-Instruct',
    cache_dir='/tmp/modelscope-cache',
    local_dir='/models/CosyVoice-300M-Instruct',
)
print('CosyVoice-300M-Instruct model ready')
PY

COPY platform/infra/docker/cosyvoice_openai_server.py /app/cosyvoice_openai_server.py

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=5s --retries=8 --start-period=60s \
  CMD code="$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8080/models || true)"; [ "$code" = "200" ]

CMD ["python3", "-m", "uvicorn", "cosyvoice_openai_server:APP", "--app-dir", "/app", "--host", "0.0.0.0", "--port", "8080"]

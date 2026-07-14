FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Europe/Kyiv

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

# supercronic: cron for containers (scheduler service)
ARG TARGETARCH=amd64
ARG SUPERCRONIC_VERSION=v0.2.47
RUN case "${TARGETARCH}" in \
        amd64) SHA1SUM=712d2ece75da6f6e530192a151488578153e4e96 ;; \
        arm64) SHA1SUM=93323899ddca3f1198f1796a4bf4418ed1e7982e ;; \
        *) echo "unsupported arch: ${TARGETARCH}" && exit 1 ;; \
    esac \
    && curl -fsSLo /usr/local/bin/supercronic \
        "https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-${TARGETARCH}" \
    && echo "${SHA1SUM}  /usr/local/bin/supercronic" | sha1sum -c - \
    && chmod +x /usr/local/bin/supercronic

WORKDIR /srv/ubki-uploader

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY crontab ./crontab

# api service; the scheduler service overrides this with supercronic
CMD ["uvicorn", "app.api:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]

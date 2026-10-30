# Lab 6 demo image: python:3.13-slim + a non-root user.
# No HEALTHCHECK: Kubernetes probes (see the demo manifests) are the source of
# truth, and a Docker-level healthcheck would only duplicate /readyz.
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py ui.html .

RUN useradd --uid 10001 --create-home appuser

USER 10001

EXPOSE 8080

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]

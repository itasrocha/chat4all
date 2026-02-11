from fastapi import FastAPI
from prometheus_client import start_http_server, Counter, Gauge
from prometheus_fastapi_instrumentator import Instrumentator

MESSAGES_PROCESSED = Counter('worker_messages_processed_total', 'Total de mensagens processadas pelo worker', ['topic'])
WORKER_ERRORS = Counter('worker_errors_total', 'Total de erros no processamento', ['type'])
ACTIVE_JOBS = Gauge('worker_active_jobs', 'Número de jobs sendo processados agora')

def start_worker_metrics(port=8000):
    """Inicia um servidor HTTP leve para expor métricas em background"""
    try:
        start_http_server(port)
        print(f"📊 Métricas do Worker rodando na porta {port}")
    except Exception as e:
        print(f"⚠️ Falha ao iniciar métricas: {e}")

def setup_metrics(app: FastAPI):
    """
    Adiciona endpoints /metrics e instrumentação automática de latência/erros.
    """
    Instrumentator().instrument(app).expose(app)
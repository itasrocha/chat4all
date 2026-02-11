# services/api-service/src/main.py
import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from dotenv import load_dotenv

from common.utils.kafka_client import ProducerWrapper
from common.database.scylla_client import scylla_client
from common.database.s3_client import s3_client
from common.observability.metrics import setup_metrics

from . import dependencies
from .routers import conversations, messages, users, sync, files

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api-service")
load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Inicializando API Service...")
    
    # 1. Init Kafka Producer
    dependencies.kafka_producer = ProducerWrapper(bootstrap_servers=KAFKA_BOOTSTRAP)
    
    # 2. Init Scylla (Lazy connection)
    scylla_client.connect()

    # 3. S3 client
    s3_client.create_bucket_if_not_exists()

    yield
    
    logger.info("🛑 Encerrando conexões...")
    if dependencies.kafka_producer:
        dependencies.kafka_producer.close()
    if scylla_client.session:
        scylla_client.close()

app = FastAPI(
    title="Chat4All Message Service", 
    version="2.0.0",
    lifespan=lifespan
)

setup_metrics(app)

# Registra as rotas
app.include_router(users.router, prefix="/v1/users", tags=["Users"])
app.include_router(conversations.router, prefix="/v1/conversations", tags=["Conversations"])
app.include_router(messages.router, prefix="/v1/messages", tags=["Messages"])
app.include_router(sync.router, prefix="/v1/sync", tags=["Sync"])
app.include_router(files.router, prefix="/v1/files", tags=["Files"])

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "api-service"}
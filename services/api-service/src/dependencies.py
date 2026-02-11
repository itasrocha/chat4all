# services/api-service/src/dependencies.py
import os
import logging
from typing import Annotated
from fastapi import Header, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from common.auth.security import decode_access_token

# Imports da Common
from common.utils.kafka_client import ProducerWrapper
from common.database.scylla_client import scylla_client

logger = logging.getLogger("api-dependencies")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="http://localhost:8002/token")

# --- KAFKA SINGLETON ---
# Será inicializado no lifespan do main.py
kafka_producer: ProducerWrapper = None

def get_kafka_producer() -> ProducerWrapper:
    if not kafka_producer:
        raise HTTPException(status_code=503, detail="Kafka unavailable")
    return kafka_producer

# --- AUTH MOCK ---
async def get_current_user_id(token: str = Depends(oauth2_scheme)) -> str:
    """
    Valida o Token JWT e extrai o User ID.
    Agora é seguro e stateless.
    """
    payload = decode_access_token(token)
    if payload is None:
        raise HTTPException(
            status_code=401,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    user_id: str = payload.get("sub")
    if user_id is None:
        raise HTTPException(status_code=401, detail="Token missing user_id")
        
    return user_id

# --- SCYLLA DB ---
def get_scylla():
    """Garante que o cliente Scylla está conectado"""
    if not scylla_client.session:
        scylla_client.connect()
    return scylla_client
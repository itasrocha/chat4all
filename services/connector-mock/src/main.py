# services/connector-mock/src/main.py
import os
import logging
import threading
import uuid
import time
import json
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel

# --- IMPORTS DA LIB COMPARTILHADA ---
from common.utils.kafka_client import ConsumerWrapper, ProducerWrapper
from common.schemas.events import MessageSentEvent, MessageStatusEvent
from common.clients.auth_client import auth_client

# --- CONFIGURAÇÕES ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')

# Nome do canal (Ex: "whatsapp", "instagram", "telegram")
# Definido no docker-compose
CHANNEL_NAME = os.getenv("CHANNEL_NAME", "generic")

logger = logging.getLogger(f"connector-{CHANNEL_NAME}")

# Tópicos Kafka
# Onde publicar novas mensagens que chegaram de fora
TOPIC_INBOUND = os.getenv("TOPIC_INBOUND", "chat.message.created.v1")

# Onde ler mensagens que o sistema quer enviar para fora
# Ex: connector.whatsapp.outbound.v1
TOPIC_CONSUME = os.getenv("TOPIC_CONSUME", f"connector.{CHANNEL_NAME}.outbound.v1")

# Onde publicar atualizações de status (Lido/Entregue)
TOPIC_STATUS = os.getenv("TOPIC_STATUS", "chat.message.status.updated.v1")

# Variável global para o Producer Kafka
producer: ProducerWrapper = None

# --- LÓGICA DE CONSUMER (OUTBOUND) ---

def simulate_delivery_callbacks(event: MessageSentEvent):
    """
    Simula o ciclo de vida de uma mensagem enviada para o usuário externo.
    1. Recebe a mensagem do Kafka.
    2. Simula delay de rede.
    3. Envia status DELIVERED.
    4. Simula delay de leitura.
    5. Envia status READ.
    """
    try:
        logger.info(f"📤 [OUTBOUND] Enviando para {event.metadata.get('external_id')}: {event.content}")
        
        # Simula tempo de entrega (Rede 4G/5G)
        time.sleep(1.5)
        
        status_event = MessageStatusEvent(
            event_id=str(uuid.uuid4()),
            message_id=event.message_id,
            conversation_id=event.conversation_id,
            sequence_number=event.sequence_number,
            user_id=event.sender_id,
            sender_id=event.sender_id,
            status="DELIVERED",
            timestamp=datetime.now(timezone.utc).isoformat(),
            channel=CHANNEL_NAME
        )
        
        producer.send_event(TOPIC_STATUS, status_event, key=event.message_id)
        logger.info(f"✅ [Callback] Msg {event.message_id} -> DELIVERED")

        # Simula tempo até o usuário abrir o app
        time.sleep(3.0)

        status_event.status = "READ"
        status_event.timestamp = datetime.now(timezone.utc).isoformat()
        status_event.event_id = str(uuid.uuid4())
        
        producer.send_event(TOPIC_STATUS, status_event, key=event.message_id)
        logger.info(f"👀 [Callback] Msg {event.message_id} -> READ")

    except Exception as e:
        logger.error(f"❌ Erro ao enviar callbacks: {e}")

def outbound_consumer_loop():
    """
    Thread que fica escutando o tópico de saída (do Fanout Service).
    """
    logger.info(f"🎧 Iniciando Consumer Outbound no tópico: {TOPIC_CONSUME}")
    
    # Usa Group ID específico para este conector garantir que recebe a msg
    consumer = ConsumerWrapper(
        topics=[TOPIC_CONSUME],
        group_id=f"connector_{CHANNEL_NAME}_group",
        auto_offset_reset='latest'
    )
    
    for msg_dict in consumer.consume():
        try:
            # Converte dict do Kafka para o Schema
            event = MessageSentEvent(**msg_dict)
            
            # Dispara a simulação (Síncrono aqui para simplificar a thread, 
            # em prod seria jogado para uma Task Queue tipo Celery/Arq)
            simulate_delivery_callbacks(event)
            
        except Exception as e:
            logger.error(f"Erro processando mensagem outbound: {e}")

# --- API FASTAPI (INBOUND) ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = ProducerWrapper()
    t = threading.Thread(target=outbound_consumer_loop, daemon=True)
    t.start()
    
    yield

    if producer:
        producer.close()
    logger.info("🛑 Connector finalizado.")

app = FastAPI(title=f"Mock Connector - {CHANNEL_NAME}", lifespan=lifespan)

class WebhookRequest(BaseModel):
    external_id: str
    name: str
    content: str
    conversation_id: str | None = None 

@app.post("/webhook")
async def receive_external_message(payload: WebhookRequest, background_tasks: BackgroundTasks):
    """
    Simula o recebimento de uma mensagem vinda da API Oficial (Meta/Twilio).
    """
    logger.info(f"📥 [INBOUND] Webhook recebido de {payload.external_id}")
    
    try:
        # ---------------------------------------------------------
        # RESOLUÇÃO DE IDENTIDADE (Auth Service gRPC)
        # ---------------------------------------------------------
        # Converte o ID externo (Telefone) em UUID interno.
        # Se não existir, o Auth cria um "Shadow User" automaticamente.
        auth_response = auth_client.get_or_create_external_user(
            provider=CHANNEL_NAME,
            provider_id=payload.external_id,
            name=payload.name
        )
        
        internal_user_id = auth_response.user.id
        is_new_user = auth_response.created_new
        
        if is_new_user:
            logger.info(f"🆕 Novo usuário criado automaticamente: {internal_user_id}")
        else:
            logger.info(f"👤 Usuário identificado: {internal_user_id}")

        # ---------------------------------------------------------
        # GERAÇÃO DE CONTEXTO (Conversation ID)
        # ---------------------------------------------------------
        # Para o MVP, usamos um UUID determinístico (v5) baseado no ID externo.
        # Isso garante que todas as mensagens desse cliente caiam no mesmo chat.
        if not payload.conversation_id:
            unique_string = f"{CHANNEL_NAME}:{payload.external_id}"
            conv_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_string))
        else:
            conv_id = payload.conversation_id

        # ---------------------------------------------------------
        # CRIAÇÃO E ENVIO DO EVENTO
        # ---------------------------------------------------------
        message_id = str(uuid.uuid4())
        
        event = MessageSentEvent(
            event_id=str(uuid.uuid4()),
            message_id=message_id,
            conversation_id=conv_id,
            sequence_number=0, # O Ingestion Service vai atribuir o real
            sender_id=internal_user_id, # UUID interno (Fundamental!)
            content=payload.content,
            message_type="text",
            status="SENT",
            timestamp=datetime.now(timezone.utc).isoformat(),
            metadata={
                "source_channel": CHANNEL_NAME,
                "external_id": payload.external_id,
                "sender_name": payload.name,
                "is_inbound": "true"
            }
        )

        # Publica no Kafka (Tópico de entrada do sistema)
        # Usamos conversation_id como chave para garantir ordem de processamento
        producer.send_event(TOPIC_INBOUND, event, key=conv_id)
        
        return {
            "status": "received",
            "platform": CHANNEL_NAME,
            "internal_user_id": internal_user_id,
            "conversation_id": conv_id,
            "message_id": message_id
        }

    except Exception as e:
        logger.error(f"❌ Erro crítico no Webhook: {e}", exc_info=True)
        # Retorna 500 para o Webhook tentar reenviar (Retry policy)
        raise HTTPException(status_code=500, detail="Internal processing error")

@app.get("/health")
def health_check():
    return {"status": "ok", "channel": CHANNEL_NAME}
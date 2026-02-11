# services/delivery-worker/src/main.py
import os
import asyncio
import json
import logging
import redis.asyncio as redis
from uuid import uuid4
from datetime import datetime, timezone
from redis.asyncio.cluster import RedisCluster

from common.utils.kafka_client import AsyncConsumerWrapper, AsyncProducerWrapper
from common.database.scylla_client import scylla_client
from common.utils.proto_converter import ProtoConverter
from common.generated import events_pb2
from common.schemas.events import DeliveryJobEvent, PushNotificationEvent

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("delivery-worker")

# Configs
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC_CONSUME = os.getenv("TOPIC_CONSUME", "chat.message.delivery.v1")
TOPIC_PUSH = os.getenv("TOPIC_PRODUCE", "chat.message.notification.v1")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "delivery_worker_group")

REDIS_HOST = os.getenv('REDIS_HOST', 'redis-cluster')
REDIS_URL = f"redis://{REDIS_HOST}:7000"

# Clientes Globais
redis_client = None
kafka_producer: AsyncProducerWrapper = None

async def process_delivery_job(msg_bytes: bytes):
    """
    Lógica de Negócio: Kafka -> Scylla -> Redis
    """
    try:
        # 1. Parse e Validação (Protobuf -> Pydantic)
        proto = events_pb2.DeliveryJobEventProto()
        proto.ParseFromString(msg_bytes)
        event = ProtoConverter.delivery_job_from_proto(proto)
        payload = event.payload
        
        # Tratamento para campos opcionais
        seq_num = payload.sequence_number if payload.sequence_number is not None else -1
        content = payload.content or ""
        
        # Persistência na User Inbox (ScyllaDB) - Write-Ahead
        try:
            query = """
                INSERT INTO chat_history.user_inbox 
                (user_id, created_at, conversation_id, message_id, sequence_number, content, sender_id, status)
                VALUES (%s, toTimestamp(now()), %s, %s, %s, %s, %s, 'PENDING')
            """
            scylla_client.session.execute(query, (
                scylla_client._to_uuid(event.recipient_id),
                scylla_client._to_uuid(event.conversation_id),
                scylla_client._to_uuid(event.message_id),
                seq_num,
                content,
                scylla_client._to_uuid(payload.sender_id)
            ))
        except Exception as e:
            logger.error(f"❌ Erro crítico Scylla (Inbox): {e}")
            raise e

        # Publicação no Redis (Fire-and-Forget)
        redis_channel = f"user:{event.recipient_id}"
        
        subscribers_count = await redis_client.execute_command(
            "PUBLISH",
            redis_channel, 
            payload.model_dump_json()
        )
        
        if subscribers_count > 0:
            logger.info(f"⚡ Entregue via Socket: {event.recipient_id} (Subs: {subscribers_count})")
        else:
            # Fallback: Usuário Offline -> Enviar Push Notification
            logger.info(f"zzz Usuário {event.recipient_id} offline. Enfileirando Push...")
            
            push_event = PushNotificationEvent(
                notification_id=str(uuid4()),
                user_id=event.recipient_id,
                title=f"Nova mensagem de {payload.sender_id}",
                body=payload.content[:100] if payload.content else "Novo arquivo",
                data={"conversation_id": event.conversation_id, "message_id": event.message_id},
                timestamp=datetime.now(timezone.utc).isoformat()
            )

            push_proto = ProtoConverter.push_notification_to_proto(push_event)

            await kafka_producer.send_proto(
                topic=TOPIC_PUSH, 
                proto_object=push_proto,
                key=push_event.user_id
            )

    except Exception as e:
        logger.error(f"❌ Erro de processamento: {e}", exc_info=True)
        raise e

async def run():
    global redis_client, kafka_producer
    
    logger.info("🚀 Delivery Worker Iniciado (Protobuf + Redis Cluster)...")
    
    scylla_client.connect()
    
    logger.info(f"🔌 Conectando ao Redis Cluster em: {REDIS_URL}")
    redis_client = RedisCluster.from_url(REDIS_URL, decode_responses=True)
    # Ping para garantir conexão antes de iniciar
    await redis_client.ping()

    kafka_producer = AsyncProducerWrapper(bootstrap_servers=KAFKA_BOOTSTRAP)
    await kafka_producer.start()
    
    consumer = AsyncConsumerWrapper(
        topic=TOPIC_CONSUME,
        group_id=GROUP_ID,
        bootstrap_servers=KAFKA_BOOTSTRAP
    )
    
    try:
        await consumer.consume(process_delivery_job)
    finally:
        logger.info("🛑 Shutting down...")
        await kafka_producer.stop()
        await redis_client.close()
        scylla_client.close()

if __name__ == "__main__":
    asyncio.run(run())
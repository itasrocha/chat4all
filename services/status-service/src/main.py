# services/status-service/src/main.py
import os
import json
import asyncio
import logging
from redis.asyncio.cluster import RedisCluster


from common.database.scylla_client import scylla_client
from common.utils.kafka_client import AsyncConsumerWrapper
from common.generated import events_pb2 

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("status-service")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC_STATUS = os.getenv("TOPIC_CONSUME_STATUS") 
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "status_service_group")

REDIS_HOST = os.getenv('REDIS_HOST', 'redis-cluster')
REDIS_URL = f"redis://{REDIS_HOST}:7000"

async def process_event(proto_event: events_pb2.MessageStatusEventProto, redis_client):
    """
    Processamento Otimizado (Pure Protobuf):
    1. Update Scylla (IO Disco Async)
    2. Publish Redis Cluster (IO Rede Async)
    """
    try:
        query = """
            UPDATE chat_history.messages 
            SET status = %s 
            WHERE conversation_id = %s AND sequence_number = %s
        """
        
        scylla_client.session.execute(query, (
            proto_event.status,
            scylla_client._to_uuid(proto_event.conversation_id),
            proto_event.sequence_number 
        ))

        if proto_event.sender_id != proto_event.user_id:
            notification = {
                "type": "STATUS_UPDATE",
                "conversation_id": proto_event.conversation_id,
                "message_id": proto_event.message_id,
                "status": proto_event.status,
                "read_by": proto_event.user_id,
                "timestamp": proto_event.timestamp
            }
            
            redis_channel = f"user:{proto_event.sender_id}"
            
            await redis_client.execute_command(
                "PUBLISH", 
                redis_channel, 
                json.dumps(notification)
            )
            
            logger.info(f"✔ Status {proto_event.status} processado. Sender {proto_event.sender_id} notificado.")
            
    except Exception as e:
        logger.error(f"❌ Erro processando status: {e}")

async def run():
    logger.info(f"🚀 Status Service Iniciado (Protobuf + Redis Cluster)...")
    
    scylla_client.connect()
    
    logger.info(f"🔌 Conectando ao Redis Cluster em: {REDIS_URL}")
    redis_client = RedisCluster.from_url(REDIS_URL, decode_responses=True)
    
    consumer = AsyncConsumerWrapper(
        topic=TOPIC_STATUS,
        group_id=GROUP_ID,
        bootstrap_servers=KAFKA_BOOTSTRAP
    )

    async def handler(msg_bytes: bytes):
        try:
            proto = events_pb2.MessageStatusEventProto()
            proto.ParseFromString(msg_bytes)
            await process_event(proto, redis_client)
        except Exception as e:
            logger.error(f"⚠️ Erro de deserialização ou lógica: {e}")

    try:
        await consumer.consume(handler)
    finally:
        await redis_client.close()
        scylla_client.close()

if __name__ == "__main__":
    asyncio.run(run())
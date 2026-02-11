# services/notification-service/src/main.py
import os
import asyncio
import logging
import json

from common.utils.kafka_client import AsyncConsumerWrapper
from common.schemas.events import PushNotificationEvent

from common.generated import events_pb2
from common.utils.proto_converter import ProtoConverter

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("notification-service")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC_CONSUME = os.getenv("TOPIC_CONSUME", "chat.message.notification.v1")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "notification_push_group")

async def send_to_fcm_mock(event: PushNotificationEvent):
    """
    Simula chamada HTTP para Firebase Cloud Messaging (FCM) ou Apple APNS.
    Geralmente tem latência de 200ms a 500ms.
    """
    # Simula latência de I/O de rede
    await asyncio.sleep(0.2)
    
    logger.info("📲 [PUSH SENT] -----------------------------------------")
    logger.info(f"   To User: {event.user_id}")
    logger.info(f"   Title:   {event.title}")
    logger.info(f"   Body:    {event.body}")
    logger.info(f"   Data:    {event.data}")
    logger.info("-------------------------------------------------------")

async def run():
    logger.info(f"🚀 Notification Service ouvindo (Protobuf): {TOPIC_CONSUME}")
    
    consumer = AsyncConsumerWrapper(
        topic=TOPIC_CONSUME,
        group_id=GROUP_ID,
        bootstrap_servers=KAFKA_BOOTSTRAP
    )
    
    async def handler(msg_bytes: bytes):
        try:
            proto = events_pb2.PushNotificationEventProto()
            proto.ParseFromString(msg_bytes)
            
            event = ProtoConverter.push_notification_from_proto(proto)
            
            await send_to_fcm_mock(event)
            
        except Exception as e:
            logger.error(f"❌ Erro ao processar Push Notification: {e}", exc_info=True)
            # Em produção, poderíamos enviariamos para uma DLQ (Dead Letter Queue)

    await consumer.consume(handler)

if __name__ == "__main__":
    asyncio.run(run())
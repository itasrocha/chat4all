# services/metadata-service/src/consumer.py
import logging
import asyncio
import os
from common.utils.kafka_client import AsyncConsumerWrapper
from common.schemas.events import UserCreatedEvent
from common.utils.proto_converter import ProtoConverter
from common.generated import events_pb2
from .database import get_connection

logger = logging.getLogger("metadata-sync")

TOPIC_USER_CREATED = os.getenv("TOPIC_USER_CREATED", "user.account.created.v1")
GROUP_ID = "metadata_query_sync_v2"

async def process_user_created(msg_bytes: bytes):
    """
    Callback de negócio.
    Recebe BYTES (Protobuf), converte para Pydantic e persiste.
    """
    try:
        try:
            proto_event = events_pb2.UserCreatedEventProto()
            proto_event.ParseFromString(msg_bytes)
            event = ProtoConverter.user_created_from_proto(proto_event)
            
        except Exception as e:
            logger.error(f"❌ Erro fatal de deserialização Protobuf (Msg ignorada): {e}")
            return

        # 2. Persistência (Idempotente com ON CONFLICT)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO users_directory (user_id, username, name, avatar_url)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE 
                    SET name = EXCLUDED.name, avatar_url = EXCLUDED.avatar_url
                """, (
                    event.user_id, 
                    event.username, 
                    event.name, 
                    f"https://api.dicebear.com/7.x/initials/svg?seed={event.username}"
                ))
                
                logger.info(f"➕ Vinculando canal 'delivery' para {event.username}")
                
                cur.execute("""
                    INSERT INTO user_identities (user_id, channel, external_id)
                    VALUES (%s, 'delivery', %s)
                    ON CONFLICT (user_id, channel) DO NOTHING
                """, (event.user_id, event.user_id))

                if event.username.startswith("whatsapp:"):
                    phone = event.username.split(":")[1]
                    cur.execute("""
                        INSERT INTO user_identities (user_id, channel, external_id)
                        VALUES (%s, 'whatsapp', %s)
                        ON CONFLICT (user_id, channel) DO NOTHING
                    """, (event.user_id, phone))
                
                elif event.username.startswith("instagram:"):
                    handle = event.username.split(":")[1]
                    cur.execute("""
                        INSERT INTO user_identities (user_id, channel, external_id)
                        VALUES (%s, 'instagram', %s)
                        ON CONFLICT (user_id, channel) DO NOTHING
                    """, (event.user_id, handle))

                conn.commit()

        logger.info(f"✅ UserSync Completo: {event.username} (Diretório + Identidades)")

    except Exception as e:
        logger.error(f"❌ Erro de banco de dados no UserSync: {e}")

async def start_user_sync_consumer():
    consumer = AsyncConsumerWrapper(
        topic=TOPIC_USER_CREATED,
        group_id=GROUP_ID
    )
    
    await consumer.consume(process_user_created)
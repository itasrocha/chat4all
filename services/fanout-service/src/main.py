# services/fanout-service/src/main.py
import os
import logging
import time
import uuid

from common.utils.kafka_client import ConsumerWrapper, ProducerWrapper
from common.schemas.events import MessageSentEvent, DeliveryJobEvent
from common.utils.proto_converter import ProtoConverter
from common.generated import events_pb2
from common.clients.metadata_client import metadata_client
from common.observability.metrics import start_worker_metrics, MESSAGES_PROCESSED

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("fanout-service")

TOPIC_CONSUME = os.getenv("TOPIC_CONSUME")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "fanout_group")

CHANNEL_TOPIC_MAP = {
    "delivery": os.getenv("TOPIC_PRODUCE_DELIVERY", "connector.delivery.outbound.v1"),
    "whatsapp": os.getenv("TOPIC_PRODUCE_WHATSAPP", "connector.whatsapp.outbound.v1"),
    "instagram": os.getenv("TOPIC_PRODUCE_INSTAGRAM", "connector.instagram.outbound.v1"),
}

def resolve_target_channels(requested_channels: list, available_identities: dict) -> dict:
    """
    Retorna: {'canal': 'external_id'} apenas para canais válidos e solicitados.
    """
    final_routing = {}
    
    if "all" in requested_channels:
        target_set = set(available_identities.keys())
    else:
        target_set = set(requested_channels)

    for channel in target_set:
        if channel in available_identities:
            final_routing[channel] = available_identities[channel]
        else:
            # TODO: Logar que tentou mandar pro Instagram mas o usuário não tem Instagram vinculado
            pass
            
    return final_routing

def run():
    logger.info("🚀 Dispatcher Service Iniciado...")
    start_worker_metrics(8000)

    consumer = ConsumerWrapper(
        topics=[TOPIC_CONSUME],
        group_id=GROUP_ID,
        auto_offset_reset='latest' # Em dev, latest é melhor para não reprocessar msg velha
    )
    
    producer = ProducerWrapper()

    logger.info(f"🎧 Ouvindo mensagens persistidas em: {TOPIC_CONSUME}")

    for msg_bytes in consumer.consume():
        try:
            MESSAGES_PROCESSED.labels(topic=TOPIC_CONSUME).inc()
            proto_event = events_pb2.MessageSentEventProto()
            proto_event.ParseFromString(msg_bytes)
            logger.info(f"⚡ Dispatching Msg: {proto_event.message_id} (Conv: {proto_event.conversation_id})")

            members = metadata_client.get_conversation_members(proto_event.conversation_id)
            
            if not members:
                logger.warning(f"⚠️ Conversa {proto_event.conversation_id} sem membros! Nada a fazer.")
                continue

            logger.info(f"👥 Fan-out para {len(members)} membros...")

            count_sent = 0
            for member_id in members:
                # Não enviar para quem enviou a mensagem (Echo suppression)
                if member_id == proto_event.sender_id:
                    continue

                identities = metadata_client.get_user_identities(member_id)

                if not identities:
                    logger.warning(f"⚠️ Usuário {member_id} sem canais configurados.")
                    continue

                routes = resolve_target_channels(proto_event.target_channels, identities)
                
                if not routes:
                    logger.warning(f"🚫 Nenhuma rota compatível para {member_id}. Pedido: {proto_event.target_channels}, Disp: {identities.keys()}")
                    continue

                for channel, external_id in routes.items():
                    target_topic = CHANNEL_TOPIC_MAP.get(channel)
                    if not target_topic: continue

                    job_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, f"{proto_event.message_id}:{member_id}:{channel}")

                    job_proto = events_pb2.DeliveryJobEventProto()
                    job_proto.job_id = str(job_uuid)
                    job_proto.message_id = proto_event.message_id
                    job_proto.conversation_id = proto_event.conversation_id
                    job_proto.recipient_id = member_id
                    job_proto.channel = channel
                    job_proto.payload.CopyFrom(proto_event)

                    producer.send_proto(
                        topic=target_topic,
                        proto_object=job_proto,
                        key=member_id 
                    )
                    
                    logger.info(f"   -> Roteado para {member_id} via [{channel}] (ID: {external_id})")

        except Exception as e:
            logger.error(f"Erro Fanout: {e}")

if __name__ == "__main__":
    run()
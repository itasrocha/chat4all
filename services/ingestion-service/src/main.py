# services/ingestion-service/src/main.py
import os
import logging
from collections import deque

from common.utils.kafka_client import ConsumerWrapper, ProducerWrapper
from common.schemas.events import MessageSentEvent
from common.clients.metadata_client import metadata_client
from common.utils.proto_converter import ProtoConverter
from common.generated import events_pb2
from common.database.scylla_client import scylla_client 
from common.observability.metrics import start_worker_metrics, MESSAGES_PROCESSED

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("ingestion-worker")

TOPIC_CONSUME = os.getenv("TOPIC_CONSUME")
TOPIC_PRODUCE = os.getenv("TOPIC_PRODUCE")
GROUP_ID = os.getenv("KAFKA_GROUP_ID")

processed_ids_cache = deque(maxlen=10000)

def run():
    logger.info("🚀 Iniciando Ingestion Worker (Persistence Only)...")
    start_worker_metrics(8000)
    
    scylla_client.connect()

    consumer = ConsumerWrapper(
        topics=[TOPIC_CONSUME],
        group_id=GROUP_ID,
        auto_offset_reset='earliest'
    )
    
    producer = ProducerWrapper()

    logger.info(f"🎧 Aguardando mensagens em {TOPIC_CONSUME}...")

    for msg_bytes in consumer.consume():
        try:
            MESSAGES_PROCESSED.labels(topic=TOPIC_CONSUME).inc()
            
            event_proto = events_pb2.MessageSentEventProto()
            event_proto.ParseFromString(msg_bytes)
            
            # Deduplicação (Cache Local simples)
            # Obs: Em produção real, usaríamos o Redis para isso ser distribuído
            if event_proto.message_id in processed_ids_cache:
                logger.warning(f"♻️ Msg duplicada ignorada: {event_proto.message_id}")
                continue
            
            logger.info(f"📥 Processando: {event_proto.message_id} | Conv: {event_proto.conversation_id}")

            # Sequenciamento (gRPC Metadata)
            # Obtém o próximo ID sequencial para garantir ordem de leitura
            seq_num = metadata_client.get_next_sequence(
                conversation_id=event_proto.conversation_id,
                message_id=event_proto.message_id 
            )
            
            scylla_client.save_message(event_proto, seq_num)
            
            processed_ids_cache.append(event_proto.message_id)

            event_proto.sequence_number = seq_num

            producer.send_proto(
                topic=TOPIC_PRODUCE,
                proto_object=event_proto,
                key=event_proto.conversation_id
            )
            
            logger.info(f"✅ Msg {event_proto.message_id} (Seq: {seq_num}) persistida e encaminhada.")

        except Exception as e:
            logger.error(f"❌ Erro no Ingestion: {e}", exc_info=True)
            # Em produção, isso iria para um Dead Letter Queue (DLQ)

if __name__ == "__main__":
    run()
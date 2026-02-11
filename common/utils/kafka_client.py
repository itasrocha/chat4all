# common/util/kafka_client.py
import os
import json
import logging
from typing import Optional, Generator, Any, Dict, Type, Union, Callable
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaTimeoutError
from pydantic import BaseModel
from functools import partial

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

# Configuração de Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração padrão via Variáveis de Ambiente
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

class KafkaClient:
    """
    Wrapper base para configurações compartilhadas (opcional, mas bom para organização).
    """
    pass

class ProducerWrapper(KafkaClient):
    def __init__(self, bootstrap_servers: str = BOOTSTRAP_SERVERS):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                # Serializer para a CHAVE da mensagem (String -> Bytes)
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                # Configurações de durabilidade (RNF03)
                acks='all',  # Espera confirmação de todas as réplicas
                retries=3,    # Tenta reenviar em caso de falha temporária
                max_block_ms=5000
            )
            logger.info(f"Kafka Producer conectado em: {bootstrap_servers}")
        except Exception as e:
            logger.error(f"Falha ao conectar Producer: {e}")
            raise e

    def send_proto(self, topic: str, proto_object, key: Optional[str] = None):
        """
        Envia um objeto Protobuf compilado de forma síncrona.
        
        Args:
            topic: Nome do tópico.
            proto_object: Objeto gerado pelo protoc (deve ter método .SerializeToString()).
            key: Chave de particionamento.
        """
        try:
            # 1. Serialização: Protobuf -> Bytes
            payload_bytes = proto_object.SerializeToString()
            
            future = self.producer.send(
                topic=topic,
                key=key,
                value=payload_bytes
            )
            
            # Callbacks
            future.add_callback(partial(self._on_success, topic, key))
            future.add_errback(partial(self._on_error, topic, key))
            
            # Nota: O KafkaProducer é assíncrono por natureza e faz batching.
            # Se quiser garantir que saiu da máquina AGORA, chame self.producer.flush()
            # mas isso reduz performance.
            
        except Exception as e:
            logger.error(f"Erro ao publicar no tópico {topic}: {e}")
            raise e

    def _on_success(self, topic, key, record_metadata):
        """
        Loga o sucesso do envio com metadados do broker.
        """
        logger.info(f"✅ Msg enviada: Tópico={topic} Partição={record_metadata.partition} Offset={record_metadata.offset} Key={key}")

    def _on_error(self, topic, key, exc):
        """
        Loga erros de envio assíncronos.
        """
        logger.error(f"❌ Erro assíncrono Kafka: Tópico={topic} Key={key} Erro={exc}")
    
    def close(self):
        self.producer.close()

class ConsumerWrapper(KafkaClient):
    def __init__(self, 
                 topics: list[str], 
                 group_id: str, 
                 bootstrap_servers: str = BOOTSTRAP_SERVERS,
                 auto_offset_reset: str = 'latest'):
        """
        Inicializa o consumidor.
        
        Args:
            topics: Lista de tópicos para escutar.
            group_id: ID do grupo de consumidores (essencial para escalar horizontalmente).
        """
        try:
            self.consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=False, # Controle manual para garantir processamento (at-least-once)
            )
            logger.info(f"Kafka Consumer (Grupo: {group_id}) ouvindo: {topics}")
        except Exception as e:
            logger.error(f"Falha ao iniciar Consumer: {e}")
            raise e

    def consume(self) -> Generator[Dict[str, Any], None, None]:
        """
        Generator que entrega mensagens deserializadas (Dict).
        O serviço deve converter esse Dict para o Pydantic específico.
        """
        try:
            for message in self.consumer:
                try:
                    data = message.value
                    logger.info(f"Mensagem recebida do tópico {message.topic}")
                    
                    yield data # Entrega o dicionário para a lógica de negócio
                    
                    # Se o código acima não der erro, commitamos o offset
                    self.consumer.commit()
                    
                except Exception as e:
                    logger.error(f"Erro ao processar mensagem offset {message.offset}: {e}")
                    # Aqui você poderia implementar lógica de DLQ (Dead Letter Queue)
        except KeyboardInterrupt:
            logger.info("Consumer interrompido.")
        finally:
            self.consumer.close()

class AsyncConsumerWrapper:
    """
    Wrapper para AIOKafkaConsumer.
    Gerencia conexão, loop de leitura e deserialização.
    """
    def __init__(self, topic: str, group_id: str, bootstrap_servers: str = None):
        self.topic = topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.consumer = None
        self.running = False

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False, 
            auto_offset_reset='latest' # Em realtime, geralmente queremos o 'agora', mas 'earliest' seria mais seguro para não perder nada.
        )
        await self.consumer.start()
        self.running = True
        logger.info(f"🎧 Async Consumer iniciado: {self.topic} (Group: {self.group_id})")

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()
            self.running = False
            logger.info("🛑 Async Consumer parado.")

    async def consume(self, callback: Callable):
        """
        Loop infinito de consumo.
        :param callback: Função async que recebe a mensagem (dict) processada.
        """
        if not self.consumer:
            await self.start()

        try:
            async for msg in self.consumer:
                try:
                    # msg.value já é dict devido ao deserializer acima
                    await callback(msg.value)
                    await self.consumer.commit()
                except Exception as e:
                    logger.error(f"Erro processando mensagem Async: {e}")
                    # DECISÃO DE ARQUITETURA:
                    # Aqui você tem duas opções:
                    # A) Não fazer nada: O offset não é comitado. Se o container reiniciar, a mensagem volta.
                    #    Porém, se o loop continuar rodando, o commit da PRÓXIMA mensagem bem sucedida 
                    #    vai comitar esta aqui implicitamente (Kafka offsets são cumulativos).
                    # B) Parar o loop/Crashar: Força o Kubernetes a reiniciar o Pod para tentar de novo limpo.
                    
                    # Para este MVP, vamos apenas logar. No mundo real, usaríamos DLQ ou Pause.
        finally:
            await self.stop()

class AsyncProducerWrapper:
    """
    Wrapper para AIOKafkaProducer (Assíncrono).
    Ideal para Workers que não podem bloquear o Event Loop (ex: Delivery Worker).
    """
    def __init__(self, bootstrap_servers: str = None):
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.producer = None

    async def start(self):
        """Inicializa a conexão com o Broker."""
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            # Key Serializer: String -> Bytes
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        await self.producer.start()
        logger.info(f"🚀 Async Producer conectado em: {self.bootstrap_servers}")

    async def stop(self):
        """Fecha a conexão."""
        if self.producer:
            await self.producer.stop()
            logger.info("🛑 Async Producer encerrado.")

    async def send_proto(self, topic: str, proto_object, key: Optional[str] = None):
        """
        Envia um objeto Protobuf compilado.
        """
        if not self.producer:
            raise Exception("Producer not started")

        try:
            # SERIALIZAÇÃO AQUI: Protobuf -> Bytes
            payload_bytes = proto_object.SerializeToString()
            
            await self.producer.send_and_wait(
                topic=topic, 
                value=payload_bytes, 
                key=key
            )
        except Exception as e:
            logger.error(f"❌ Erro envio Protobuf: {e}")
            raise e
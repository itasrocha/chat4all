# common/database/scylla_client.py
import os
import time
import logging
import json
import uuid
from uuid import UUID
from datetime import datetime
from typing import Optional, List, Dict, Any

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import dict_factory

# Import schemas
from common.schemas.events import MessageSentEvent

logger = logging.getLogger(__name__)

class ScyllaClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ScyllaClient, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
            
        self.cluster = None
        self.session = None
        self.keyspace = "chat_history"
        # Pega a config do ambiente ou usa defaults
        self.node = os.getenv("SCYLLA_HOST", "message-store")
        self._initialized = True

    def connect(self):
        if self.session:
            return

        logger.info(f"💾 [ScyllaClient] Conectando em {self.node}...")
        
        lb_policy = TokenAwarePolicy(DCAwareRoundRobinPolicy())

        while not self.session:
            try:
                self.cluster = Cluster(
                    contact_points=[self.node],
                    load_balancing_policy=lb_policy,
                    protocol_version=4,
                    connect_timeout=10
                )
                self.session = self.cluster.connect()
                # Configura row_factory para retornar dicionários (facilita para JSON)
                self.session.row_factory = dict_factory 
                logger.info("✅ [ScyllaClient] Conectado!")
                self._init_schema()
            except Exception as e:
                logger.warning(f"⏳ [ScyllaClient] Falha na conexão. Retentando em 5s... Erro: {str(e)}")
                time.sleep(5)

    def _init_schema(self):
        # 1. Keyspace
        self.session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)

        # 2. Tabela: Global Message Log (Fonte da verdade para Histórico)
        # Partition Key: conversation_id (agrupa msgs da mesma conversa)
        # Clustering Key: sequence_number (ordena cronologicamente)
        self.session.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.messages (
                conversation_id UUID,
                sequence_number BIGINT,
                message_id UUID,
                sender_id UUID,
                content TEXT,
                message_type TEXT,
                status TEXT,
                timestamp TIMESTAMP,
                attachments TEXT, 
                PRIMARY KEY ((conversation_id), sequence_number)
            ) WITH CLUSTERING ORDER BY (sequence_number ASC)
        """)

        # 3. Tabela: User Inbox (Usada pelo Realtime Service / Sync)
        # Partition Key: user_id (agrupa msgs de UM usuário)
        # Clustering Key: created_at (ordena cronologicamente)
        self.session.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.user_inbox (
                user_id UUID,
                created_at TIMESTAMP,
                conversation_id UUID,
                message_id UUID,
                sequence_number BIGINT,
                content TEXT,
                sender_id UUID,
                status TEXT,
                PRIMARY KEY ((user_id), created_at, message_id)
            ) WITH CLUSTERING ORDER BY (created_at DESC)
        """)
        
        logger.info("✅ [ScyllaClient] Schema verificado (Tables: messages, user_inbox).")

    def close(self):
        if self.cluster:
            self.cluster.shutdown()
            self.session = None

    # --- Métodos de Escrita ---

    def save_message(self, event_obj, sequence_number: int) -> bool:
        """
        Aceita tanto Pydantic quanto Protobuf wrapper.
        """
        # Detecta se é Protobuf (pela existência de campos específicos ou tipo)
        is_proto = hasattr(event_obj, "attachments_json")

        # Extração de dados agnóstica
        msg_id = getattr(event_obj, "message_id")
        conv_id = getattr(event_obj, "conversation_id")
        sender_id = getattr(event_obj, "sender_id")
        timestamp_str = getattr(event_obj, "timestamp")
        content = getattr(event_obj, "content")
        msg_type = getattr(event_obj, "message_type")
        status = getattr(event_obj, "status")

        # Tratamento especial para Anexos
        if is_proto:
            # Já é string JSON no Protobuf, perfeito para o banco!
            # Zero custo de CPU (sem json.dumps)
            att_str = event_obj.attachments_json or None
        else:
            # É Pydantic (Dict), precisa serializar
            attachments = getattr(event_obj, "attachments", None)
            att_str = json.dumps(attachments) if attachments else None

        query = f"""
            INSERT INTO {self.keyspace}.messages 
            (conversation_id, sequence_number, message_id, sender_id, content, message_type, status, timestamp, attachments)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            USING TTL 31536000
        """
        
        try:
            uuid_conv = self._to_uuid(conv_id)
            uuid_msg = self._to_uuid(msg_id)
            uuid_sender = self._to_uuid(sender_id)
            ts_obj = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))

            self.session.execute(query, (
                uuid_conv,
                sequence_number,
                uuid_msg,
                uuid_sender,
                content,
                msg_type,
                status,
                ts_obj,
                att_str # Salva a string direto
            ))
            return True
        except Exception as e:
            logger.error(f"❌ Erro Scylla: {e}")
            raise e

    def save_to_inbox(self, user_id: str, event: dict, status: str = 'PENDING'):
        """
        Salva na 'user_inbox'.
        Usado pelo Realtime Service.
        """
        query = f"""
            INSERT INTO {self.keyspace}.user_inbox 
            (user_id, created_at, conversation_id, message_id, sequence_number, content, sender_id, status)
            VALUES (%s, toTimestamp(now()), %s, %s, %s, %s, %s, %s)
        """
        # Implementar lógica similar de conversão se necessário
        pass

    # --- Helpers ---
    
    def _to_uuid(self, val):
        if isinstance(val, UUID):
            return val
        try:
            return UUID(val)
        except ValueError:
            # Fallback determinístico para strings que não são UUIDs (ex: IDs legados)
            return uuid.uuid5(uuid.NAMESPACE_DNS, str(val))

# Instância global para ser importada
scylla_client = ScyllaClient()
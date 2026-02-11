#common/clients/metadata_client.py
import os
import grpc
import json
import logging
from typing import List

# Importa os stubs gerados automaticamente
from common.generated import metadata_pb2, metadata_pb2_grpc

logger = logging.getLogger(__name__)

class MetadataClient:
    _instance = None

    def __new__(cls):
        # Implementação Singleton para garantir apenas 1 canal gRPC por processo
        if cls._instance is None:
            cls._instance = super(MetadataClient, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
            
        # Pega a variável de ambiente ou usa o default do docker-compose
        self.host = os.getenv("METADATA_HOST", "metadata-service:50051")
        self.channel = None
        self.stub = None
        self._initialized = True

    def _connect(self):
        """Estabelece a conexão Lazy (apenas quando necessária)"""
        if not self.channel:
            logger.info(f"🔌 Conectando gRPC ao Metadata Service em {self.host}...")
            # Cria canal inseguro (interno ao cluster)
            # Em produção, usaria grpc.secure_channel com credenciais SSL
            self.channel = grpc.insecure_channel(self.host)
            self.stub = metadata_pb2_grpc.MetadataServiceStub(self.channel)

    def create_conversation(self, type: str, members: list, metadata: dict = None) -> str:
        self._connect()
        try:
            req = metadata_pb2.CreateConversationRequest(
                type=type,
                member_ids=members,
                metadata=metadata or {}
            )
            response = self.stub.CreateConversation(req)
            return response.conversation_id
        except grpc.RpcError as e:
            logger.error(f"❌ Erro gRPC (CreateConversation): {e.details()}")
            raise e

    def get_conversation_members(self, conversation_id: str) -> List[str]:
        self._connect()
        try:
            req = metadata_pb2.GetConversationMembersRequest(conversation_id=conversation_id)
            response = self.stub.GetConversationMembers(req)
            return response.member_ids
        except grpc.RpcError as e:
            logger.error(f"❌ Erro gRPC (GetMembers): {e.details()}")
            raise e

    def get_user_conversations(self, user_id: str) -> List[dict]:
        self._connect()
        try:
            req = metadata_pb2.GetUserConversationsRequest(user_id=user_id)
            resp = self.stub.GetUserConversations(req)
            
            # Converte Protobuf de volta para Lista de Dicionários Python
            result = []
            for c in resp.conversations:
                result.append({
                    "conversation_id": c.conversation_id,
                    "type": c.type,
                    "metadata": json.loads(c.metadata_json) if c.metadata_json else {},
                    "last_sequence_number": c.last_sequence_number
                })
            return result
            
        except grpc.RpcError as e:
            logger.error(f"❌ Erro gRPC (GetUserConversations): {e.details()}")
            raise e

    def get_next_sequence(self, conversation_id: str, message_id: str) -> int:
        """
        Obtém o próximo ID sequencial.
        """
        self._connect()
        try:
            request = metadata_pb2.GetNextSequenceNumberRequest(
                conversation_id=conversation_id,
                message_id=message_id
            )
            response = self.stub.GetNextSequenceNumber(request)
            return response.sequence_number
        except grpc.RpcError as e:
            logger.error(f"❌ Erro gRPC (GetNextSequence): {e.details()}")
            raise e

    def list_users(self, limit: int = 100, offset: int = 0):
        """
        Consulta o diretório.
        Retorna: Lista de UserProfile (desempacotada)
        """
        self._connect()
        try:
            request = metadata_pb2.ListUsersRequest(limit=limit, offset=offset)
            response = self.stub.ListUsers(request)
            return response.users
        except grpc.RpcError as e:
            logger.error(f"❌ Erro gRPC (ListUsers): {e.details()}")
            raise e

    def get_user_profile(self, user_id: str):
        """
        Retorna: UserProfile (O próprio objeto)
        """
        self._connect()
        try:
            request = metadata_pb2.GetUserProfileRequest(user_id=user_id)
            return self.stub.GetUserProfile(request) # Já é o objeto final
        except grpc.RpcError as e:
            logger.error(f"❌ Erro gRPC (GetUserProfile): {e.details()}")
            raise e

    def get_user_identities(self, user_id: str) -> dict:
        self._connect()
        try:
            req = metadata_pb2.GetUserIdentitiesRequest(user_id=user_id)
            resp = self.stub.GetUserIdentities(req)
            return {id.channel: id.external_id for id in resp.identities}
        except grpc.RpcError as e:
            logger.error(f"❌ Erro gRPC (GetUserIdentities): {e.details()}")
            raise e

    def add_user_identity(self, user_id: str, channel: str, external_id: str):
        self._connect()
        try:
            req = metadata_pb2.AddUserIdentityRequest(
                user_id=user_id,
                channel=channel,
                external_id=external_id
            )
            self.stub.AddUserIdentity(req)
        except grpc.RpcError as e:
            logger.error(f"❌ Erro gRPC (AddUserIdentity): {e.details()}")
            raise e

# Instância global pronta para uso
metadata_client = MetadataClient()
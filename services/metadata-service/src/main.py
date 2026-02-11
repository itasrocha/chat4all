# services/metadata-service/src/main.py
import os
import logging
import uuid
import grpc
import json
import threading
import asyncio
from concurrent import futures
from pathlib import Path
from dotenv import load_dotenv

from common.generated import metadata_pb2, metadata_pb2_grpc

from .database import Database, get_connection
from .consumer import start_user_sync_consumer
from .repository import ConversationRepository, ConversationNotFoundError, RepositoryError

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("metadata-service")

load_dotenv()

GRPC_PORT = os.getenv("GRPC_PORT")

class MetadataService(metadata_pb2_grpc.MetadataServiceServicer):
    
    def __init__(self):
        self.repo = ConversationRepository()

    def CreateConversation(self, request, context):
        generated_id = str(uuid.uuid4())
        
        try:
            if not request.member_ids:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("A conversa deve ter pelo menos 1 membro")
                return metadata_pb2.CreateConversationResponse()

            final_id = self.repo.create_conversation(
                conversation_id=generated_id,
                type=request.type,
                members=request.member_ids,
                metadata=dict(request.metadata)
            )

            logger.info(f"🚀 Conversa criada: {final_id}")

            return metadata_pb2.CreateConversationResponse(
                conversation_id=final_id
            )
        except Exception as e:
            logger.error(f"Erro create: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return metadata_pb2.CreateConversationResponse()

    def GetConversationMembers(self, request, context):
        try:
            members = self.repo.get_members(request.conversation_id)
            return metadata_pb2.GetConversationMembersResponse(member_ids=members)
        except RepositoryError:
            context.set_code(grpc.StatusCode.INTERNAL)
            return metadata_pb2.GetConversationMembersResponse()

    def GetUserConversations(self, request, context):
        try:
            conversations = self.repo.get_user_conversations(request.user_id)
            
            proto_list = []
            for conv in conversations:
                meta_json = json.dumps(conv.get('metadata') or {})
                
                proto_list.append(metadata_pb2.ConversationSummaryProto(
                    conversation_id=conv['conversation_id'],
                    type=conv['type'],
                    metadata_json=meta_json,
                    last_sequence_number=conv['last_sequence_number']
                ))
            
            return metadata_pb2.GetUserConversationsResponse(conversations=proto_list)

        except Exception as e:
            logger.error(f"Erro ao buscar conversas: {e}")
            context.abort(grpc.StatusCode.INTERNAL, str(e))

    def GetNextSequenceNumber(self, request, context):
        conversation_id = request.conversation_id
        message_id = request.message_id
        try:
            new_sequence = self.repo.get_or_create_sequence(conversation_id, message_id)
            
            logger.info(f"🔢 Seq: {new_sequence} | Conv: {conversation_id} | Msg: {message_id}")
            
            return metadata_pb2.GetNextSequenceNumberResponse(
                sequence_number=new_sequence
            )

        except ConversationNotFoundError:
            logger.warning(f"⚠️ Conversa não encontrada: {conversation_id}")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Conversa não encontrada")
            return metadata_pb2.GetNextSequenceNumberResponse()
            
        except RepositoryError:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Erro ao acessar banco de dados")
            return metadata_pb2.GetNextSequenceNumberResponse()

    def ListUsers(self, request, context):
        """
        Lê da tabela local users_directory (Muito rápido!)
        """
        limit = request.limit if request.limit > 0 else 100
        offset = request.offset
        
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT user_id, name, username, avatar_url FROM users_directory LIMIT %s OFFSET %s",
                        (limit, offset)
                    )
                    rows = cur.fetchall()
                    
                    users_proto = []
                    for r in rows:
                        users_proto.append(metadata_pb2.UserProfile(
                            user_id=r[0],
                            name=r[1],
                            username=r[2],
                            avatar_url=r[3] or ""
                        ))
                    
                    return metadata_pb2.ListUsersResponse(users=users_proto)
        except Exception as e:
            logger.error(f"Erro ListUsers: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return metadata_pb2.ListUsersResponse()
    
    def GetUserIdentities(self, request, context):
        identities_dict = self.repo.get_user_identities(request.user_id)
        
        proto_identities = [
            metadata_pb2.UserIdentity(channel=k, external_id=v) 
            for k, v in identities_dict.items()
        ]
        
        return metadata_pb2.GetUserIdentitiesResponse(identities=proto_identities)

    def AddUserIdentity(self, request, context):
        try:
            self.repo.add_identity(
                user_id=request.user_id,
                channel=request.channel,
                external_id=request.external_id
            )
            logger.info(f"🔗 Identidade vinculada: {request.user_id} -> {request.channel}:{request.external_id}")
            return metadata_pb2.AddUserIdentityResponse(success=True)
        except Exception as e:
            logger.error(f"Erro ao adicionar identidade: {e}")
            context.abort(grpc.StatusCode.INTERNAL, str(e))

def run_async_consumer():
    """Wrapper para rodar o loop asyncio em uma thread separada"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start_user_sync_consumer())

def serve():
    consumer_thread = threading.Thread(target=run_async_consumer, daemon=True)
    consumer_thread.start()

    Database.initialize()
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    metadata_pb2_grpc.add_MetadataServiceServicer_to_server(MetadataService(), server)
    
    try:
        from grpc_reflection.v1alpha import reflection
        SERVICE_NAMES = (
            metadata_pb2.DESCRIPTOR.services_by_name['MetadataService'].full_name,
            reflection.SERVICE_NAME,
        )
        reflection.enable_server_reflection(SERVICE_NAMES, server)
    except ImportError:
        pass

    address = f"[::]:{GRPC_PORT}"
    server.add_insecure_port(address)
    logger.info(f"🚀 Metadata Service rodando gRPC em {address}")
    
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
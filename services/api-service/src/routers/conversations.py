# services/api-service/src/routers/conversations.py
import logging
from typing import Annotated, List
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException

from ..schemas import (
    CreateConversationRequest, 
    CreateConversationResponse, 
    ConversationSummary
)
from ..dependencies import get_current_user_id
from common.clients.metadata_client import metadata_client

router = APIRouter()
logger = logging.getLogger("router-conversations")

@router.post("", response_model=CreateConversationResponse, status_code=201)
async def create_conversation(
    request: CreateConversationRequest,
    user_id: Annotated[str, Depends(get_current_user_id)]
):
    try:
        # Garante que quem cria está na lista de membros
        members = list(set(request.members + [user_id]))
        
        conversation_id = metadata_client.create_conversation(
            type=request.type,
            members=members,
            metadata=request.metadata
        )

        return CreateConversationResponse(
            conversation_id=conversation_id,
            created_at=datetime.now(timezone.utc).isoformat()
        )
    except Exception as e:
        logger.error(f"Erro criando conversa: {e}")
        raise HTTPException(status_code=500, detail="Failed to create conversation")

@router.get("", response_model=List[ConversationSummary])
async def list_my_conversations(
    user_id: Annotated[str, Depends(get_current_user_id)]
):
    """
    Retorna as conversas (Grupos e Privadas) do usuário logado.
    """
    try:
        # Chamada real ao Metadata Service via gRPC
        conversations = metadata_client.get_user_conversations(user_id)
        return conversations
        
    except Exception as e:
        logger.error(f"Erro listando conversas: {e}")
        raise HTTPException(status_code=500, detail="Internal Error")
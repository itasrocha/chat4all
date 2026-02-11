# services/api-service/src/routers/sync.py
from typing import Annotated, List, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, Query, HTTPException
from ..schemas import MessageResponse
from ..dependencies import get_current_user_id, get_scylla

# Logger para debug
import logging
logger = logging.getLogger("router-sync")

router = APIRouter()

@router.get("", response_model=List[MessageResponse])
async def sync_offline_messages(
    user_id: Annotated[str, Depends(get_current_user_id)],
    since: Optional[datetime] = Query(None),
    scylla = Depends(get_scylla)
):
    """
    Busca na 'user_inbox' mensagens recebidas.
    """
    try:
        # 1. Preparação da Query
        cql = "SELECT * FROM chat_history.user_inbox WHERE user_id = %s"
        
        params = [scylla._to_uuid(user_id)]
        
        if since:
            cql += " AND created_at > %s"
            params.append(since)
            
        cql += " LIMIT 100"
        
        # 2. Execução
        logger.info(f"Syncing for user UUID: {params[0]}")
        rows = scylla.session.execute(cql, tuple(params))
        
        # 3. Conversão (Row -> Pydantic)
        results = []
        for row in rows:
            # Atenção: user_inbox tem campos que podem ser nulos dependendo da sua inserção
            # O get() ajuda a evitar KeyError, e o 'or' trata None
            results.append(MessageResponse(
                message_id=str(row['message_id']),
                conversation_id=str(row['conversation_id']),
                sender_id=str(row['sender_id']),
                content=row['content'] or "",
                message_type="text", # user_inbox simplificada não tem message_type no schema atual, assumimos text
                sequence_number=row['sequence_number'] or 0,
                created_at=row['created_at'], # Driver já retorna datetime
                status=row['status'] or "DELIVERED"
            ))
        
        return results

    except Exception as e:
        logger.error(f"Erro no Sync: {e}")
        raise HTTPException(status_code=500, detail="Failed to sync messages")
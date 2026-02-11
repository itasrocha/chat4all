# services/api-service/src/routers/users.py
import logging

from typing import List, Annotated
from fastapi import APIRouter, Depends, HTTPException
from ..schemas import UserResponse, IdentityRequest

from common.clients.metadata_client import metadata_client
from ..dependencies import get_current_user_id

router = APIRouter()
logger = logging.getLogger("router-users")

@router.get("", response_model=List[UserResponse])
async def list_users(
    current_user_id: Annotated[str, Depends(get_current_user_id)]
):
    try:
        users = metadata_client.list_users(limit=50) 
        
        results = []
        for user in users:
            results.append(UserResponse(
                user_id=user.user_id,
                name=user.name,
                avatar_url=user.avatar_url
            ))
        return results
        
    except Exception as e:
        logger.error(f"Erro fetching users directory: {e}")
        raise HTTPException(status_code=500, detail="Directory unavailable")

@router.post("/me/identities", status_code=201)
def link_identity(
    request: IdentityRequest,
    current_user_id = Depends(get_current_user_id)
):
    """
    Permite que o usuário logado vincule um canal externo à sua conta.
    """
    # Validação básica
    if request.channel not in ["whatsapp", "instagram", "telegram"]:
        raise HTTPException(status_code=400, detail="Canal inválido")

    try:
        metadata_client.add_user_identity(
            user_id=str(current_user_id),
            channel=request.channel,
            external_id=request.external_id
        )
        return {"status": "linked", "channel": request.channel}
    except Exception as e:
        logger.error(f"❌ Falha ao vincular identidade: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/me/identities")
def get_my_identities(current_user_id = Depends(get_current_user_id)):
    """
    Lista os canais vinculados do usuário logado.
    """
    return metadata_client.get_user_identities(str(current_user_id))
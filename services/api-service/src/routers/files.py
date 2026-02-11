# services/api-service/src/routers/files.py
import uuid
import logging
from fastapi import APIRouter, Depends, HTTPException
from typing import Annotated

from ..schemas import FileUploadRequest, FileUploadResponse
from ..dependencies import get_current_user_id
from common.database.s3_client import s3_client

router = APIRouter()
logger = logging.getLogger("router-files")

@router.post("/presigned-url", response_model=FileUploadResponse)
async def get_presigned_url(
    request: FileUploadRequest,
    user_id: Annotated[str, Depends(get_current_user_id)]
):
    """
    Etapa 1 do Upload: Cliente pede permissão.
    Retorna URL para o cliente fazer o upload direto pro MinIO.
    """
    # 1. Gera uma chave única (Key) para o arquivo
    # Estrutura: uploads/{user_id}/{uuid}/{filename}
    file_uuid = str(uuid.uuid4())
    object_key = f"uploads/{user_id}/{file_uuid}/{request.filename}"

    try:
        # 2. Gera a URL assinada (S3 Presigned Post)
        presigned_data = s3_client.generate_presigned_post(object_key)
        
        if not presigned_data:
            raise HTTPException(status_code=500, detail="Could not generate upload URL")

        return FileUploadResponse(
            file_key=object_key,
            upload_url=presigned_data['url'],
            fields=presigned_data['fields']
        )

    except Exception as e:
        logger.error(f"Erro upload: {e}")
        raise HTTPException(status_code=500, detail="Internal Error")

@router.get("/view/{file_key:path}") 
def get_download_url(
    file_key: str, 
    user_id: Annotated[str, Depends(get_current_user_id)]
):
    """
    Gera uma URL temporária para visualizar/baixar o arquivo.
    """
    logger.info(f"Gerando link para: {file_key}")
    
    try:
        url = s3_client.generate_presigned_get(file_key)
        
        if not url:
             raise Exception("URL gerada vazia")

        return {"url": url}

    except Exception as e:
        logger.error(f"❌ Erro ao gerar link S3: {e}", exc_info=True)
        raise HTTPException(status_code=404, detail="Arquivo não encontrado ou erro interno")
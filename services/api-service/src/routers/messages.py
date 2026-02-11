# services/api-service/src/routers/messages.py
import os
import logging
import json
from typing import Annotated, List, Optional
from datetime import datetime, timezone
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query

from ..schemas import CreateMessageRequest, CreateMessageResponse, MessageResponse, UpdateStatusRequest
from ..dependencies import get_current_user_id, get_kafka_producer, get_scylla
from common.schemas.events import MessageSentEvent, MessageStatusEvent
from common.utils.proto_converter import ProtoConverter
from common.generated import events_pb2

router = APIRouter()
logger = logging.getLogger("router-messages")

TOPIC_PRODUCE_MESSAGE = os.getenv("TOPIC_PRODUCE_MESSAGE", "chat.message.created.v1")
TOPIC_PRODUCE_STATUS = os.getenv("TOPIC_PRODUCE_STATUS", "chat.message.status.v1")

@router.post("", status_code=202, response_model=CreateMessageResponse)
async def send_message(
    request: CreateMessageRequest,
    user_id: Annotated[str, Depends(get_current_user_id)],
    producer = Depends(get_kafka_producer)
):
    channels = request.target_channels if request.target_channels else ["delivery"]

    internal_event = MessageSentEvent(
        message_id=str(request.idempotency_key),
        conversation_id=str(request.conversation_id),
        sender_id=user_id,
        timestamp=datetime.now(timezone.utc).isoformat(),
        message_type=request.message_type,
        content=request.content,
        attachments=request.attachments,
        target_channels=channels,
        status="SENT"
    )

    event_proto = ProtoConverter.message_sent_to_proto(internal_event)

    try:
        producer.send_proto(
            topic=TOPIC_PRODUCE_MESSAGE,
            proto_object=event_proto,
            key=internal_event.conversation_id
        )

        return CreateMessageResponse(
            status="queued",
            message_id=internal_event.message_id,
            timestamp=internal_event.timestamp
        )
    except Exception as e:
        logger.error(f"Erro envio Kafka: {e}")
        raise HTTPException(status_code=500, detail="Failed to queue message")

@router.get("/{conversation_id}", response_model=List[MessageResponse])
async def get_history(
    conversation_id: str, 
    limit: int = Query(50, le=100),
    before_sequence: Optional[int] = None,
    scylla = Depends(get_scylla)
):
    try:
        cql = "SELECT * FROM chat_history.messages WHERE conversation_id = %s"
        
        uuid_conv_id = scylla._to_uuid(conversation_id)
        params = [uuid_conv_id]

        if before_sequence:
            cql += " AND sequence_number < %s"
            params.append(before_sequence)
        
        cql += " ORDER BY sequence_number DESC LIMIT %s"
        params.append(limit)

        rows = scylla.session.execute(cql, tuple(params))
        
        results = []
        for row in rows:
            # --- Tratamento do JSON de Anexos ---
            attachments_obj = None
            
            # Verifica se a coluna existe no resultado e se tem valor
            # row pode ser acessado como dict ou atributo dependendo do RowFactory
            raw_attachments = getattr(row, 'attachments', None) or row.get('attachments')
            
            if raw_attachments:
                try:
                    # Converte de String '{"key": "val"}' para Dict Python
                    attachments_obj = json.loads(raw_attachments)
                except Exception as e:
                    logger.warning(f"Erro ao parsear anexo da msg {row['message_id']}: {e}")

            results.append(MessageResponse(
                message_id=str(row['message_id']),
                conversation_id=str(row['conversation_id']),
                sender_id=str(row['sender_id']),
                content=row['content'],
                message_type=row['message_type'] or "text",
                sequence_number=row['sequence_number'],
                created_at=row['timestamp'], 
                status=row['status'],
                attachments=attachments_obj 
            ))
        
        return list(results)

    except Exception as e:
        logger.error(f"Erro lendo histórico: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch history")

@router.patch("/status", status_code=202)
async def update_message_status(
    request: UpdateStatusRequest,
    user_id: Annotated[str, Depends(get_current_user_id)],
    producer = Depends(get_kafka_producer)
):
    """
    Atualiza o status de uma mensagem (Feedback Loop).
    Ex: Bob chama isso com status='READ' para avisar que leu a msg de Alice.
    """

    internal_event = MessageStatusEvent(
        event_id=str(uuid4()),
        message_id=str(request.message_id),
        conversation_id=str(request.conversation_id),
        sequence_number=request.sequence_number,
        user_id=user_id,
        sender_id=str(request.sender_id),
        status=request.status,
        timestamp=datetime.now(timezone.utc).isoformat()
    )

    event_proto = ProtoConverter.message_status_to_proto(internal_event)

    try:
        # Publica no Kafka
        # Key = conversation_id (Mantém ordem)
        producer.send_proto(
            topic=TOPIC_PRODUCE_STATUS,
            proto_object=event_proto,
            key=internal_event.conversation_id 
        )
        
        return {"status": "processing", "updated_to": request.status}

    except Exception as e:
        logger.error(f"Erro ao atualizar status: {e}")
        raise HTTPException(status_code=500, detail="Failed to update status")
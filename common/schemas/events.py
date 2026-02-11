# common/schemas/events.py
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime

# Evento: Quando uma mensagem foi aceita e salva no banco
class MessageSentEvent(BaseModel):
    message_id: str
    conversation_id: str
    sender_id: str
    timestamp: str 
    message_type: str
    content: str
    attachments: Optional[Dict[str, Any]] = None
    status: str
    sequence_number: Optional[int] = None # Enriquecimento no ingestion
    target_channels: List[str] = ["delivery"]
    
    class Config:
        from_attributes = True

# Evento: Ordem para entregar mensagem a alguém
class DeliveryJobEvent(BaseModel):
    job_id: str
    message_id: str
    conversation_id: str
    recipient_id: str
    channel: str # "internal", "whatsapp", "telegram"
    payload: MessageSentEvent # Dados da mensagem original
class MessageStatusEvent(BaseModel):
    """
    Evento disparado quando o destinatário recebe ou lê uma mensagem.
    Fluxo: API -> Kafka (Status Topic) -> Ingestion (DB) -> Realtime (Notificar Remetente)
    """
    event_id: str # UUID para rastrear este evento específico
    message_id: str
    sequence_number: int
    conversation_id: str
    user_id: str # Quem atualizou o status (o leitor)
    sender_id: str # Quem enviou (Remetente original - para notificar)
    status: str = Field(..., pattern="^(DELIVERED|READ)$")
    timestamp: str 
    
    class Config:
        from_attributes = True

class UserCreatedEvent(BaseModel):
    """
    Evento disparado pelo Auth Service quando um registro ocorre.
    O Metadata Service vai escutar isso para criar a cópia pública (Query Model).
    """
    event_id: str
    user_id: str
    username: str
    email: str
    name: str
    timestamp: str
    
    class Config:
        from_attributes = True

class PushNotificationEvent(BaseModel):
    notification_id: str
    user_id: str
    title: str
    body: str
    data: Optional[Dict[str, Any]] = None # Payload oculto (ex: conversation_id para abrir a tela certa)
    timestamp: str

    class Config:
        from_attributes = True
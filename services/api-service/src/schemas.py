# services/api-service/src/schemas.py
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from uuid import UUID
from datetime import datetime

# --- SHARED MODELS ---
class UserResponse(BaseModel):
    user_id: str
    name: str
    avatar_url: Optional[str] = None

# --- MESSAGING (WRITE) ---
class CreateMessageRequest(BaseModel):
    idempotency_key: UUID = Field(..., description="UUID v4 para garantir idempotência")
    conversation_id: UUID
    message_type: str = Field(default="text", pattern="^(text|file|location)$")
    content: str
    attachments: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    target_channels: Optional[List[str]] = None

class CreateMessageResponse(BaseModel):
    status: str
    message_id: UUID
    timestamp: str

# --- MESSAGING (READ) ---
class MessageResponse(BaseModel):
    message_id: str
    conversation_id: str
    sender_id: str
    content: str
    message_type: str
    sequence_number: int
    created_at: datetime
    status: str
    attachments: Optional[Dict[str, Any]] = None

# --- CONVERSATIONS (WRITE) ---
class CreateConversationRequest(BaseModel):
    type: str = Field(..., pattern="^(private|group)$")
    members: List[str] = Field(..., min_length=1)
    metadata: Optional[Dict[str, Any]] = None

class CreateConversationResponse(BaseModel):
    conversation_id: str
    created_at: str

# --- CONVERSATIONS (READ) ---
class ConversationSummary(BaseModel):
    conversation_id: str
    type: str
    metadata: Optional[Dict[str, Any]] = {}
    last_sequence_number: int = 0
    # Em um app real, aqui viriam também 'unread_count' e 'last_message_preview'

# --- STATUS ---

class UpdateStatusRequest(BaseModel):
    """
    Cliente avisa: "Eu li a mensagem X da conversa Y".
    """
    conversation_id: UUID
    message_id: UUID
    sequence_number: int
    sender_id: UUID = Field(..., description="ID do remetente original da mensagem (para notificação)")
    status: str = Field(..., pattern="^(DELIVERED|READ)$", description="Novo status da mensagem")

    class Config:
        json_schema_extra = {
            "example": {
                "conversation_id": "c56a4180-65aa-42ec-a945-5fd21dec0538",
                "message_id": "123e4567-e89b-12d3-a456-426614174000",
                "sender_id": "9500ae63-7697-4b11-9084-c793d4dd0dfc",
                "status": "READ"
            }
        }

class FileUploadRequest(BaseModel):
    filename: str
    content_type: str # ex: "image/jpeg", "video/mp4"
    size_bytes: int

class FileUploadResponse(BaseModel):
    file_key: str # O ID do arquivo no sistema
    upload_url: str # A URL do MinIO
    fields: Dict[str, Any] # Campos de autenticação AWS

class IdentityRequest(BaseModel):
    channel: str      # "whatsapp", "instagram"
    external_id: str  # "5511999..."
# services/auth-service/src/schemas.py
from pydantic import BaseModel, EmailStr, Field
from uuid import UUID

class UserCreate(BaseModel):
    # Adicionamos 'pattern' (Regex) para permitir explicitamente:
    # - Letras e números (a-z, 0-9)
    # - Underline (_) e Hífen (-)
    # - Dois pontos (:) para o prefixo 'whatsapp:'
    # - Sinal de mais (+) para DDI de telefone '+55'
    username: str = Field(
        ..., 
        min_length=3, 
        pattern=r"^[a-zA-Z0-9_\-:+]+$",
        description="Aceita alfanuméricos e os caracteres especiais: - _ : +"
    )
    password: str
    email: EmailStr
    name: str

class UserResponse(BaseModel):
    id: UUID
    username: str
    email: str
    name: str
    
    class Config:
        from_attributes = True

class Token(BaseModel):
    access_token: str
    token_type: str
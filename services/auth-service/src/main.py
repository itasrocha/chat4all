# services/auth-service/src/main.py
import os
import logging
import uuid
import grpc
from datetime import datetime, timezone
import threading
from concurrent import futures
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from contextlib import asynccontextmanager

from .database import engine, Base, get_db
from . import models, schemas
from common.auth.security import verify_password, get_password_hash, create_access_token

from common.utils.kafka_client import ProducerWrapper
from common.schemas.events import UserCreatedEvent

from common.generated import auth_pb2, auth_pb2_grpc
from common.schemas.events import UserCreatedEvent

from common.utils.proto_converter import ProtoConverter

from common.observability.metrics import setup_metrics

# Configuração gRPC
GRPC_PORT = 50052

producer: ProducerWrapper = None

# --- Implementação do Servidor gRPC ---
class AuthGRPCImplementation(auth_pb2_grpc.AuthServiceServicer):
    
    def _get_db(self):
        return SessionLocal()

    def GetOrCreateExternalUser(self, request, context):
        """
        Cria um Shadow User e dispara evento para o Metadata Service indexar.
        """
        db = self._get_db()
        provider_key = f"{request.provider}:{request.provider_id}"
        
        try:
            user = db.query(models.User).filter(models.User.username == provider_key).first()
            created = False
            
            if not user:
                logger.info(f"👤 Criando Shadow User para: {provider_key}")
                new_user_id = uuid.uuid4()
                user = models.User(
                    id=new_user_id,
                    username=provider_key,
                    email=f"{provider_key}@external.chat4all",
                    name=request.name or f"User {request.provider_id}",
                    hashed_password=get_password_hash(str(uuid.uuid4())) # Senha inútil
                )
                db.add(user)
                db.commit()
                db.refresh(user)
                created = True
                
                # Dispara evento CQRS (Shadow User também precisa ir pro Metadata)
                if producer:
                    try:
                        event = UserCreatedEvent(
                            event_id=str(uuid.uuid4()),
                            user_id=str(user.id),
                            username=user.username,
                            email=user.email,
                            name=user.name,
                            timestamp=datetime.now(timezone.utc).isoformat()
                        )
                        
                        # Conversão Pydantic -> Proto
                        event_proto = ProtoConverter.user_created_to_proto(event)
                        
                        producer.send_proto(
                            topic=TOPIC_USER_CREATED,
                            proto_object=event_proto,
                            key=event.user_id
                        )
                        logger.info(f"📢 Shadow User sincronizado via Kafka: {user.username}")
                    except Exception as e:
                        logger.error(f"❌ Erro enviando evento Shadow User: {e}")

            return auth_pb2.ExternalUserResponse(
                user=auth_pb2.User(
                    id=str(user.id),
                    username=user.username,
                    email=user.email,
                    name=user.name
                ),
                created_new=created
            )
        except Exception as e:
            logger.error(f"Erro gRPC ExternalUser: {e}")
            context.abort(grpc.StatusCode.INTERNAL, str(e))
        finally:
            db.close()

# --- Startup do gRPC em Thread ---
def start_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    auth_pb2_grpc.add_AuthServiceServicer_to_server(AuthGRPCImplementation(), server)
    server.add_insecure_port(f'[::]:{GRPC_PORT}')
    logger.info(f"🔐 Auth gRPC rodando na porta {GRPC_PORT}")
    server.start()
    server.wait_for_termination()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    try:
        producer = ProducerWrapper()
    except Exception as e:
        logger.warning(f"⚠️ Kafka não disponível no startup: {e}")

    t = threading.Thread(target=start_grpc_server, daemon=True)
    t.start()
    
    yield
    
    if producer:
        producer.close()
    logger.info("🛑 Auth Service finalizado.")

# Cria tabelas no startup (Simples para MVP)
Base.metadata.create_all(bind=engine)

producer = ProducerWrapper() 
TOPIC_USER_CREATED = os.getenv("TOPIC_USER_CREATED", "user.account.created.v1")

app = FastAPI(title="Chat4All Auth Service", lifespan=lifespan)
setup_metrics(app)
logger = logging.getLogger("auth-service")

@app.post("/register", response_model=schemas.UserResponse, status_code=201)
def register(user: schemas.UserCreate, db: Session = Depends(get_db)):
    # Verifica duplicidade
    existing = db.query(models.User).filter(models.User.username == user.username).first()
    if existing:
        raise HTTPException(status_code=400, detail="Username already registered")
    
    # Cria usuário no Banco (Write Model)
    db_user = models.User(
        username=user.username,
        email=user.email,
        name=user.name,
        hashed_password=get_password_hash(user.password)
    )
    
    try:
        db.add(db_user)
        db.commit()
        db.refresh(db_user) # Recarrega do banco para pegar o ID (UUID) gerado
    except Exception as e:
        db.rollback()
        logger.error(f"Erro de banco: {e}")
        raise HTTPException(status_code=500, detail="Database error")

    try:
        event = UserCreatedEvent(
            event_id=str(uuid.uuid4()),
            user_id=str(db_user.id),
            username=db_user.username,
            email=db_user.email,
            name=db_user.name,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
        
        # Converte Pydantic -> Protobuf
        event_proto = ProtoConverter.user_created_to_proto(event)
        
        if producer:
            producer.send_proto(
                topic=TOPIC_USER_CREATED,
                proto_object=event_proto,
                key=event.user_id
            )
            logger.info(f"📢 Evento UserCreated disparado (Proto): {db_user.username}")
        else:
            logger.error("❌ Producer Kafka não inicializado. Evento perdido.")

    except Exception as e:
        logger.error(f"❌ Falha crítica no Kafka (CQRS): {e}")
        # Não falhamos o request HTTP, apenas logamos o erro de consistência eventual

    return db_user

@app.post("/token", response_model=schemas.Token)
def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(), 
    db: Session = Depends(get_db)
):
    """
    Endpoint padrão OAuth2. Recebe 'username' e 'password' via Form-Data.
    """
    user = db.query(models.User).filter(models.User.username == form_data.username).first()
    
    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Gera JWT
    # O 'sub' (subject) do token será o UUID do usuário
    access_token = create_access_token(data={"sub": str(user.id), "username": user.username})
    
    return {"access_token": access_token, "token_type": "bearer"}
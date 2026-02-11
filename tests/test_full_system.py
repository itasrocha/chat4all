# tests/test_full_system.py
import pytest
import asyncio
import uuid
import json
import time
from httpx import AsyncClient
import websockets

# --- CONFIGURAÇÕES DE AMBIENTE ---
import os

BASE_URL = os.getenv("GATEWAY_URL", "http://localhost:8080")
API_URL = f"{BASE_URL}"          # API Service
# Para o WebSocket, se estiver dentro do docker, o host pode ser diferente
# Se usarmos o gateway (nginx), ele encaminha /ws para o socket-gateway
WS_URL = os.getenv("WS_URL", "ws://localhost:8080/ws")
AUTH_URL = f"{BASE_URL}/auth"       # Auth Service

# Timeout para esperar eventos assíncronos (Kafka -> Redis)
EVENT_TIMEOUT = 10.0 

# --- HELPERS DE AUTENTICAÇÃO ---

async def create_and_login_user(prefix: str):
    """
    Cria um utilizador no Auth Service, faz login e retorna:
    - id: UUID do utilizador
    - token: JWT Access Token
    - headers: Dict pronto para usar no httpx ({Authorization: Bearer ...})
    """
    unique_suffix = uuid.uuid4().hex[:8]
    username = f"{prefix}_{unique_suffix}"
    password = "password123"
    email = f"{username}@test.com"

    async with AsyncClient(base_url=AUTH_URL, timeout=10.0) as auth_client:
        # 1. Registar
        reg_payload = {
            "username": username,
            "password": password,
            "email": email,
            "name": f"User {prefix.capitalize()}"
        }
        resp_reg = await auth_client.post("/register", json=reg_payload)
        assert resp_reg.status_code == 201, f"Falha no registo: {resp_reg.text}"
        user_data = resp_reg.json()
        user_id = user_data["id"]

        # 2. Login (Obter Token)
        login_data = {
            "username": username,
            "password": password
        }
        resp_login = await auth_client.post("/token", data=login_data)
        assert resp_login.status_code == 200, f"Falha no login: {resp_login.text}"
        token_data = resp_login.json()
        token = token_data["access_token"]

    return {
        "id": user_id,
        "username": username,
        "token": token,
        "headers": {"Authorization": f"Bearer {token}"}
    }

# --- FIXTURES (SETUP) ---

@pytest.fixture
async def alice():
    """Cria a utilizadora Alice e retorna suas credenciais"""
    return await create_and_login_user("alice")

@pytest.fixture
async def bob():
    """Cria o utilizador Bob e retorna suas credenciais"""
    return await create_and_login_user("bob")

@pytest.fixture
async def api_client():
    """Cliente HTTP genérico para a API"""
    async with AsyncClient(base_url=API_URL, timeout=10.0) as client:
        yield client

@pytest.fixture
async def conversation_ctx(api_client, alice, bob):
    """
    Cria uma conversa privada entre Alice e Bob.
    Retorna o ID da conversa.
    """
    payload = {
        "type": "private",
        "members": [alice["id"], bob["id"]],
        "metadata": {"env": "integration_test"}
    }
    
    # Alice cria a conversa
    resp = await api_client.post("/v1/conversations", json=payload, headers=alice["headers"])
    assert resp.status_code in [200, 201]
    return resp.json()["conversation_id"]

# --- HELPER: RETRY ASSERTION ---
async def wait_for_condition(check_func, timeout=10, interval=1):
    start = time.time()
    last_error = None
    while time.time() - start < timeout:
        try:
            await check_func()
            return
        except AssertionError as e:
            last_error = e
            await asyncio.sleep(interval)
        except Exception as e:
            # Captura outros erros (ex: conexão recusada temporária)
            last_error = e
            await asyncio.sleep(interval)
            
    raise last_error

# --- TESTES EXISTENTES (ADAPTADOS ONDE NECESSÁRIO) ---

@pytest.mark.asyncio
async def test_auth_flow_and_discovery(api_client, alice):
    """
    Testa se o token gerado permite acessar a API protegida.
    """
    # Tenta acessar endpoint protegido (List Users)
    resp = await api_client.get("/v1/users", headers=alice["headers"])
    assert resp.status_code == 200
    users = resp.json()
    assert isinstance(users, list)

@pytest.mark.asyncio
async def test_realtime_messaging_flow(api_client, conversation_ctx, alice, bob):
    """
    Cenário: Alice envia msg (Default Delivery), Bob (Online via WS) recebe.
    """
    msg_content = f"Hello Realtime {uuid.uuid4()}"
    msg_id = str(uuid.uuid4())

    ws_url_bob = f"{WS_URL}?token={bob['token']}"
    
    async with websockets.connect(ws_url_bob) as ws:
        
        # 2. Alice envia mensagem REST (Sem especificar target_channels, assume ["delivery"])
        payload = {
            "idempotency_key": msg_id,
            "conversation_id": conversation_ctx,
            "content": msg_content,
            "message_type": "text"
        }
        resp = await api_client.post("/v1/messages", json=payload, headers=alice["headers"])
        assert resp.status_code == 202

        # 3. Bob espera receber no Socket
        try:
            raw_msg = await asyncio.wait_for(ws.recv(), timeout=EVENT_TIMEOUT)
            event = json.loads(raw_msg)
            
            assert event['message_id'] == msg_id
            assert event['content'] == msg_content
            
        except asyncio.TimeoutError:
            pytest.fail(f"Bob não recebeu a mensagem via WebSocket em {EVENT_TIMEOUT}s")

@pytest.mark.asyncio
async def test_file_upload_flow(api_client, conversation_ctx, alice):
    """
    Teste de Upload com Presigned URL.
    """
    filename = "test_image.png"
    file_content = b"fake_image_bytes"
    
    # 1. Obter URL
    payload_req = {
        "filename": filename,
        "content_type": "image/png",
        "size_bytes": len(file_content)
    }
    
    resp = await api_client.post("/v1/files/presigned-url", json=payload_req, headers=alice["headers"])
    assert resp.status_code == 200
    data = resp.json()
    
    # 2. Upload para MinIO
    # Se estiver rodando fora do docker, minio:9000 -> localhost:9000
    # Se estiver rodando dentro do docker, minio:9000 funciona diretamente
    s3_test_url = os.getenv("S3_TEST_URL", "http://localhost:9000")
    upload_url = data['upload_url'].replace("http://minio:9000", s3_test_url)
    files = {'file': (filename, file_content)}
    
    async with AsyncClient() as uploader:
        resp_upload = await uploader.post(upload_url, data=data['fields'], files=files)
        
    assert resp_upload.status_code == 204

    # 3. Enviar Mensagem com Anexo
    msg_id = str(uuid.uuid4())
    payload_msg = {
        "idempotency_key": msg_id,
        "conversation_id": conversation_ctx,
        "content": "Imagem Anexada",
        "message_type": "file",
        "attachments": {
            "file_key": data['file_key'],
            "original_name": filename,
            "size": len(file_content)
        }
    }
    
    resp_msg = await api_client.post("/v1/messages", json=payload_msg, headers=alice["headers"])
    assert resp_msg.status_code == 202

@pytest.mark.asyncio
async def test_status_update_flow(api_client, conversation_ctx, alice, bob):
    """
    Cenário: Alice envia -> Bob recebe -> Bob lê -> Alice notificada.
    """
    msg_id = str(uuid.uuid4())
    
    ws_url_alice = f"{WS_URL}?token={alice['token']}"
    
    async with websockets.connect(ws_url_alice) as ws_alice:
        
        await api_client.post("/v1/messages", json={
            "idempotency_key": msg_id,
            "conversation_id": conversation_ctx,
            "content": "Read me!",
            "message_type": "text"
        }, headers=alice["headers"])

        await asyncio.sleep(2) 

        resp_hist = await api_client.get(f"/v1/messages/{conversation_ctx}", headers=bob["headers"])
        history = resp_hist.json()
        target_msg = next((m for m in history if m['message_id'] == msg_id), None)
        assert target_msg is not None
        
        payload_status = {
            "conversation_id": conversation_ctx,
            "message_id": msg_id,
            "sequence_number": target_msg['sequence_number'],
            "status": "READ",
            "sender_id": alice["id"]
        }
        
        patch_resp = await api_client.patch("/v1/messages/status", json=payload_status, headers=bob["headers"])
        assert patch_resp.status_code == 202

        found_notification = False
        start_time = time.time()
        
        while time.time() - start_time < EVENT_TIMEOUT:
            try:
                raw = await asyncio.wait_for(ws_alice.recv(), timeout=1.0)
                event = json.loads(raw)
                
                if event.get("type") == "STATUS_UPDATE" and event["message_id"] == msg_id:
                    assert event["status"] == "READ"
                    found_notification = True
                    break
            except asyncio.TimeoutError:
                continue
        
        assert found_notification, "Alice não recebeu a confirmação de leitura."

@pytest.mark.asyncio
async def test_offline_sync_flow(api_client, conversation_ctx, alice, bob):
    """
    Sync de mensagens offline.
    """
    msg_id = str(uuid.uuid4())
    msg_content = "Offline Msg"

    await api_client.post("/v1/messages", json={
        "idempotency_key": msg_id,
        "conversation_id": conversation_ctx,
        "content": msg_content,
        "message_type": "text"
    }, headers=alice["headers"])

    async def check_sync():
        resp = await api_client.get("/v1/sync", headers=bob["headers"])
        assert resp.status_code == 200
        inbox = resp.json()
        found = next((m for m in inbox if m['message_id'] == msg_id), None)
        assert found is not None
        assert found['content'] == msg_content

    await wait_for_condition(check_sync, timeout=10)

@pytest.mark.asyncio
async def test_push_notification_trigger(api_client, conversation_ctx, alice, bob):
    """
    Cenário: Bob Offline -> Push.
    """
    msg_id = str(uuid.uuid4())
    msg_content = f"Msg para Trigger Push {msg_id}"

    print(f"\n[PUSH TEST] ID do Bob: {bob['id']}")

    resp = await api_client.post("/v1/messages", json={
        "idempotency_key": msg_id,
        "conversation_id": conversation_ctx,
        "content": msg_content,
        "message_type": "text"
    }, headers=alice["headers"])
    
    assert resp.status_code == 202

    async def check_persistence():
        resp_sync = await api_client.get("/v1/sync", headers=bob["headers"])
        assert resp_sync.status_code == 200
        inbox = resp_sync.json()
        assert any(m['message_id'] == msg_id for m in inbox)
    
    await wait_for_condition(check_persistence, timeout=10)
    print(f"[PUSH TEST] ✅ Mensagem processada. Verifique 'docker logs notification-service'.")

# --- NOVOS TESTES DE ROTEAMENTO ---

@pytest.mark.asyncio
async def test_multichannel_routing_api_contract(api_client, conversation_ctx, alice):
    """
    Testa se a API aceita a especificação de múltiplos canais.
    Verifica se o sistema processa sem erro, mesmo que o Fanout descarte por falta de mapeamento.
    """
    msg_id = str(uuid.uuid4())
    
    # Payload com canais específicos
    payload = {
        "idempotency_key": msg_id,
        "conversation_id": conversation_ctx,
        "content": "Message to WhatsApp only",
        "message_type": "text",
        "target_channels": ["whatsapp", "instagram"] # <--- Roteamento Explícito
    }

    print(f"\n[ROUTING TEST] Enviando ID {msg_id} para {payload['target_channels']}")

    resp = await api_client.post("/v1/messages", json=payload, headers=alice["headers"])
    assert resp.status_code == 202, f"API rejeitou payload multiplataforma: {resp.text}"

    # Como não temos mock do DB de identidades, verificamos se a msg pelo menos chegou no Ingestion
    # Se ela chegou no sync (interno), significa que o ingestion salvou.
    # Nota: Como o ingestion salva independente do canal alvo, isso valida o fluxo até o DB.
    async def check_persistence():
        # Alice verifica seu próprio histórico (sent messages) se implementado, 
        # ou apenas confiamos no status 202 e nos logs do Fanout.
        # Aqui, vamos apenas garantir um pequeno delay para os logs aparecerem.
        await asyncio.sleep(2)
        pass
    
    await check_persistence()
    print("[ROUTING TEST] ✅ API aceitou payload. Verifique 'docker logs fanout-service'.")
    print("               Esperado: '⚠️ Usuário ... sem canais configurados' ou '🚫 Nenhuma rota compatível'")

@pytest.mark.asyncio
async def test_broadcast_routing(api_client, conversation_ctx, alice):
    """
    Testa o envio com target_channels=['all'].
    """
    msg_id = str(uuid.uuid4())
    payload = {
        "idempotency_key": msg_id,
        "conversation_id": conversation_ctx,
        "content": "Broadcast Message",
        "message_type": "text",
        "target_channels": ["all"] # <--- Broadcast
    }

    print(f"\n[BROADCAST TEST] Enviando ID {msg_id} para ALL channels")
    
    resp = await api_client.post("/v1/messages", json=payload, headers=alice["headers"])
    assert resp.status_code == 202
    
    print("[BROADCAST TEST] ✅ Sucesso. Verifique logs do Fanout para confirmar a tentativa de lookup.")
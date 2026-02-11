# tests/test_routing.py
import pytest
import asyncio
import uuid
import json
import time
import base64
from httpx import AsyncClient
import websockets

# --- CONFIGURAÇÕES ---
import os

BASE_URL = os.getenv("GATEWAY_URL", "http://localhost:8080")
WS_URL = os.getenv("WS_URL", "ws://localhost:8080/ws")
AUTH_URL = f"{BASE_URL}/auth"

EVENT_TIMEOUT = 5.0

# --- HELPERS ---

def get_id_from_token(token):
    """
    Decodifica o JWT (sem validar assinatura) para extrair o 'sub' (user_id).
    Evita depender da lista /v1/users que tem delay de sincronização.
    """
    try:
        # JWT é Header.Payload.Signature
        payload_part = token.split(".")[1]
        # Corrige padding do base64 se necessário
        payload_part += "=" * ((4 - len(payload_part) % 4) % 4)
        decoded_bytes = base64.b64decode(payload_part)
        payload = json.loads(decoded_bytes)
        return payload["sub"]
    except Exception as e:
        print(f"Erro decodificando token: {e}")
        return None

async def create_user_custom(username, password="password123"):
    """
    Cria ou Loga um usuário. 
    Obtém o ID de forma robusta (via resposta de registro ou Token).
    """
    async with AsyncClient(base_url=AUTH_URL, timeout=10.0) as auth_client:
        user_id = None
        
        # 1. Tenta Registrar
        reg_payload = {
            "username": username,
            "password": password,
            "email": f"{username}@test.com",
            "name": f"User {username}"
        }
        resp = await auth_client.post("/register", json=reg_payload)
        
        if resp.status_code == 201:
            # Se criou agora, o ID vem no corpo da resposta
            user_id = resp.json()["id"]
        
        # 2. Login (Necessário para pegar o Token)
        login_data = {"username": username, "password": password}
        resp_login = await auth_client.post("/token", data=login_data)
        assert resp_login.status_code == 200, f"Falha login: {resp_login.text}"
        
        token = resp_login.json()["access_token"]
        
        # Se não pegamos o ID no registro (pq usuário já existia), pegamos do token
        if not user_id:
            user_id = get_id_from_token(token)

        assert user_id is not None, "Não foi possível obter o User ID"

        return {
            "id": user_id,
            "username": username,
            "token": token,
            "headers": {"Authorization": f"Bearer {token}"}
        }

@pytest.fixture
async def api_client():
    async with AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        yield client

# --- TESTES DE ROTEAMENTO ---

@pytest.mark.asyncio
async def test_routing_whatsapp_isolation(api_client):
    """
    CENÁRIO: Roteamento Exclusivo.
    Alice envia mensagem para Bob APENAS via WhatsApp.
    Bob (mesmo conectado no WebSocket) NÃO deve receber a mensagem na tela.
    """
    # 1. Setup
    alice = await create_user_custom(f"alice_{uuid.uuid4().hex[:6]}")
    bob = await create_user_custom(f"bob_{uuid.uuid4().hex[:6]}")
    
    # Aguarda Sync do Metadata (criação de identidades no banco)
    # Isso é necessário para o Fanout saber que o Bob existe e tem canais
    await asyncio.sleep(2)

    # Cria conversa
    resp_conv = await api_client.post("/v1/conversations", json={
        "type": "private", "members": [alice["id"], bob["id"]]
    }, headers=alice["headers"])
    assert resp_conv.status_code in [200, 201]
    conv_id = resp_conv.json()["conversation_id"]

    # 2. Bob conecta no WebSocket
    ws_url = f"{WS_URL}?token={bob['token']}"
    async with websockets.connect(ws_url) as ws_bob:
        
        # 3. Alice envia mensagem EXCLUSIVA para WhatsApp
        # Bob tem canal 'delivery' (padrão), mas Alice forçou 'whatsapp'.
        msg_id = str(uuid.uuid4())
        await api_client.post("/v1/messages", json={
            "idempotency_key": msg_id,
            "conversation_id": conv_id,
            "content": "Secret WhatsApp Message",
            "message_type": "text",
            "target_channels": ["whatsapp"] # <--- TARGET ESPECÍFICO
        }, headers=alice["headers"])

        # 4. Verificação Negativa
        # Bob NÃO deve receber essa mensagem no Socket
        try:
            # Esperamos um pouco para ver se chega algo (Timeout curto proposital)
            msg = await asyncio.wait_for(ws_bob.recv(), timeout=3.0)
            event = json.loads(msg)
            
            # Se recebermos algo, verificamos se é a mensagem proibida
            if event.get('message_id') == msg_id:
                pytest.fail("FALHA DE ROTEAMENTO: Mensagem destinada ao WhatsApp vazou para o WebSocket!")
            else:
                print(f"Info: Recebido evento irrelevante no socket: {event.get('type')}")
                
        except asyncio.TimeoutError:
            # Sucesso! O timeout significa que nada chegou.
            print("\n✅ Sucesso: Mensagem de WhatsApp não apareceu no WebSocket.")

@pytest.mark.asyncio
async def test_routing_whatsapp_isolation(api_client):
    """
    CENÁRIO: Roteamento Exclusivo e Vínculo de Canal.
    1. Bob cria conta.
    2. Bob VINCULA seu número de WhatsApp.
    3. Alice envia mensagem APENAS para WhatsApp.
    4. Bob NÃO deve receber no WebSocket (mas a mensagem deve ser aceita).
    """
    # 1. Setup
    alice = await create_user_custom(f"alice_{uuid.uuid4().hex[:6]}")
    bob = await create_user_custom(f"bob_{uuid.uuid4().hex[:6]}")
    
    # 2. Bob vincula o WhatsApp (AGORA TEMOS UMA ROTA PARA ISSO)
    wa_number = "5511988887777"
    resp_link = await api_client.post(
        "/v1/users/me/identities",
        json={"channel": "whatsapp", "external_id": wa_number},
        headers=bob["headers"]
    )
    assert resp_link.status_code == 201
    print(f"\n[SETUP] Bob vinculou WhatsApp: {wa_number}")

    # Cria conversa
    resp_conv = await api_client.post("/v1/conversations", json={
        "type": "private", "members": [alice["id"], bob["id"]]
    }, headers=alice["headers"])
    conv_id = resp_conv.json()["conversation_id"]

    # 3. Conecta WebSocket
    ws_url = f"{WS_URL}?token={bob['token']}"
    async with websockets.connect(ws_url) as ws_bob:
        
        # 4. Alice envia EXCLUSIVA para WhatsApp
        msg_id = str(uuid.uuid4())
        
        # Payload pedindo WhatsApp
        payload = {
            "idempotency_key": msg_id,
            "conversation_id": conv_id,
            "content": "Secret WhatsApp Message",
            "message_type": "text",
            "target_channels": ["whatsapp"] 
        }

        # A API deve aceitar (202), pois Bob AGORA TEM o canal 'whatsapp' configurado!
        # Antes falharia ou o Fanout descartaria silenciosamente.
        resp = await api_client.post("/v1/messages", json=payload, headers=alice["headers"])
        assert resp.status_code == 202

        # 5. Verificação Negativa no Socket
        try:
            msg = await asyncio.wait_for(ws_bob.recv(), timeout=3.0)
            event = json.loads(msg)
            if event.get('message_id') == msg_id:
                pytest.fail("FALHA: Mensagem vazou para o WebSocket!")
        except asyncio.TimeoutError:
            print("✅ Sucesso: Roteamento respeitou o canal e ignorou o WebSocket.")

async def wait_for_condition(check_func, timeout=5):
    start = time.time()
    last_err = None
    while time.time() - start < timeout:
        try:
            await check_func()
            return
        except Exception as e:
            last_err = e
            await asyncio.sleep(1)
    raise last_err
import os
import logging
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, status

from redis.asyncio.cluster import RedisCluster
import redis.asyncio as redis

from common.auth.security import decode_access_token

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("socket-gateway")

app = FastAPI(title="Chat4All Socket Gateway")

REDIS_HOST = os.getenv('REDIS_HOST', 'redis-cluster')
REDIS_URL = f"redis://{REDIS_HOST}:7000"

class ConnectionManager:
    def __init__(self):
        self.active_connections = {}

    async def connect(self, user_id: str, websocket: WebSocket):
        await websocket.accept()
        if user_id not in self.active_connections:
            self.active_connections[user_id] = []
        self.active_connections[user_id].append(websocket)
        logger.info(f"🔌 Conectado: {user_id} (Total: {len(self.active_connections[user_id])})")

    def disconnect(self, user_id: str, websocket: WebSocket):
        if user_id in self.active_connections:
            if websocket in self.active_connections[user_id]:
                self.active_connections[user_id].remove(websocket)
            if not self.active_connections[user_id]:
                del self.active_connections[user_id]
        logger.info(f"👋 Desconectado: {user_id}")

manager = ConnectionManager()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: str = Query(...)):
    payload = decode_access_token(token)
    if not payload:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    user_id = payload.get("sub")
    await manager.connect(user_id, websocket)

    rc = None
    node_client = None
    pubsub = None
    channel = f"user:{user_id}"

    try:
        rc = RedisCluster.from_url(REDIS_URL, decode_responses=True)
        await rc.initialize()
        
        node = rc.get_node_from_key(channel)
        
        if not node:
            node = rc.get_nodes()[0]

        logger.info(f"📍 Conectando PubSub no nó {node.host}:{node.port}")

        node_client = redis.Redis(
            host=node.host, 
            port=node.port, 
            decode_responses=True
        )

        pubsub = node_client.pubsub()
        
        await pubsub.subscribe(channel)
        
        logger.info(f"👂 Ouvindo canal standard: {channel}")

        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            
            if message:
                data = message["data"]
                await websocket.send_text(data)

    except WebSocketDisconnect:
        manager.disconnect(user_id, websocket)
    except Exception as e:
        logger.error(f"Erro WS: {e}", exc_info=True)
        manager.disconnect(user_id, websocket)
    finally:
        try:
            if pubsub:
                await pubsub.unsubscribe(channel)
            if node_client:
                await node_client.close()
            if rc:
                await rc.close()
        except Exception as e:
            logger.error(f"Erro cleanup Redis: {e}")
            
@app.get("/health")
def health():
    return {"status": "ok", "service": "socket-gateway", "connections": len(manager.active_connections)}
# common/database/redis_client.py
import os
from redis.asyncio.cluster import RedisCluster

REDIS_URL = os.getenv("REDIS_CLUSTER_URL", "redis://redis-cluster:7000")

async def get_redis_cluster_client():
    """
    Retorna um cliente ciente da topologia do cluster.
    Ele descobre automaticamente onde estão os slots.
    """
    return RedisCluster.from_url(
        REDIS_URL, 
        decode_responses=True,
        # Importante para remapeamento automático em caso de failover
        recluster_timeout=5  
    )
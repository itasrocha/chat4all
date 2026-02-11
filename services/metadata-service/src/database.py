# services/metadata-service/src/database.py
import os
import logging
from contextlib import contextmanager
from psycopg2 import pool

logger = logging.getLogger("metadata-db")

class Database:
    _pool = None

    @classmethod
    def initialize(cls):
        if cls._pool is None:
            try:
                cls._pool = pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=20,
                    host=os.getenv("PG_HOST"),
                    database=os.getenv("PG_DB"),
                    user=os.getenv("PG_USER"),
                    password=os.getenv("PG_PASS"),
                    port=os.getenv("PG_PORT")
                )
                logger.info("✅ Pool PostgreSQL iniciado.")
            except Exception as e:
                logger.critical(f"❌ Falha no pool: {e}")
                raise e

    @classmethod
    @contextmanager
    def get_connection(cls):
        if cls._pool is None:
            cls.initialize()
            
        conn = cls._pool.getconn()
        try:
            yield conn
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cls._pool.putconn(conn)

get_connection = Database.get_connection
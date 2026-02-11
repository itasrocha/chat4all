# services/metadata-service/src/repository.py
import logging
import json
import psycopg2
from .database import get_connection

logger = logging.getLogger("metadata-repo")

class RepositoryError(Exception): pass
class ConversationNotFoundError(RepositoryError): pass

class ConversationRepository:
    
    def find_private_conversation(self, member_a: str, member_b: str) -> str | None:
        """
        Verifica se já existe um chat privado entre dois usuários.
        Retorna o conversation_id se existir, ou None.
        """
        query = """
        SELECT cm1.conversation_id
        FROM conversation_members cm1
        JOIN conversation_members cm2 ON cm1.conversation_id = cm2.conversation_id
        JOIN conversations c ON c.conversation_id = cm1.conversation_id
        WHERE cm1.user_id = %s 
          AND cm2.user_id = %s
          AND c.type = 'private'
        LIMIT 1;
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (member_a, member_b))
                result = cur.fetchone()
                return result[0] if result else None

    def create_conversation(self, conversation_id: str, type: str, members: list, metadata: dict) -> str:
        """
        Cria conversa. Se for 'private' e já existir, retorna o ID da existente (Idempotência).
        """
        if type == 'private' and len(members) == 2:
            existing_id = self.find_private_conversation(members[0], members[1])
            if existing_id:
                logger.info(f"♻️ Chat privado já existe: {existing_id}")
                return existing_id

        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO conversations (conversation_id, type, last_sequence_number, metadata) 
                        VALUES (%s, %s, 0, %s)
                        """,
                        (conversation_id, type, json.dumps(metadata))
                    )
                    
                    unique_members = list(set(members))
                    member_data = [(conversation_id, uid) for uid in unique_members]
                    
                    cur.executemany(
                        "INSERT INTO conversation_members (conversation_id, user_id) VALUES (%s, %s)",
                        member_data
                    )
                
                conn.commit()
            return conversation_id
            
        except psycopg2.Error as e:
            logger.error(f"Erro criando conversa: {e}")
            raise RepositoryError(f"Erro DB: {e}")

    def get_members(self, conversation_id: str) -> list:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id FROM conversation_members WHERE conversation_id = %s", (conversation_id,))
                return [row[0] for row in cur.fetchall()]

    def get_or_create_sequence(self, conversation_id: str, message_id: str) -> int:
        """
        Gera um sequence number de forma IDEMPOTENTE.
        1. Se message_id já existe no log -> Retorna o valor antigo.
        2. Se não existe -> Incrementa conversa, salva no log e retorna novo valor.
        """
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT sequence_number FROM message_sequences_log WHERE message_id = %s",
                        (message_id,)
                    )
                    existing = cur.fetchone()
                    
                    if existing:
                        logger.info(f"♻️ Idempotência: Seq {existing[0]} reutilizada para Msg {message_id}")
                        return existing[0]

                    cur.execute(
                        """
                        UPDATE conversations 
                        SET last_sequence_number = last_sequence_number + 1 
                        WHERE conversation_id = %s 
                        RETURNING last_sequence_number
                        """,
                        (conversation_id,)
                    )
                    row = cur.fetchone()
                    
                    if row is None:
                        raise ConversationNotFoundError(f"Conversa {conversation_id} inexistente")
                    
                    new_sequence = row[0]

                    cur.execute(
                        """
                        INSERT INTO message_sequences_log (message_id, conversation_id, sequence_number)
                        VALUES (%s, %s, %s)
                        """,
                        (message_id, conversation_id, new_sequence)
                    )
                
                conn.commit()
                return new_sequence

        except psycopg2.Error as e:
            logger.error(f"Erro sequence idempotente: {e}")
            raise RepositoryError(f"Erro DB: {e}")
            
    def get_user_conversations(self, user_id: str) -> list:
        """
        Retorna lista de conversas de um usuário (Para a Home do App).
        Necessário para o 'Sync Service' no futuro.
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.conversation_id, c.type, c.metadata, c.last_sequence_number
                    FROM conversation_members cm
                    JOIN conversations c ON cm.conversation_id = c.conversation_id
                    WHERE cm.user_id = %s
                """, (user_id,))
                
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_user_identities(self, user_id: str) -> dict:
        """
        Retorna dicionário: {'whatsapp': '5511...', 'instagram': '@user'}
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT channel, external_id FROM user_identities WHERE user_id = %s",
                    (user_id,)
                )
                rows = cur.fetchall()
                return {row[0]: row[1] for row in rows}

    def add_identity(self, user_id: str, channel: str, external_id: str):
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO user_identities (user_id, channel, external_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_id, channel) 
                    DO UPDATE SET external_id = EXCLUDED.external_id
                """, (user_id, channel, external_id))
                conn.commit()
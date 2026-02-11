-- services/metadata-service/database/init_db.sql

CREATE TABLE IF NOT EXISTS conversations (
    conversation_id VARCHAR(36) PRIMARY KEY,
    type VARCHAR(20) NOT NULL, -- 'private', 'group'
    last_sequence_number BIGINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB -- { "title": "...", "photo": "..." }
);

CREATE TABLE IF NOT EXISTS conversation_members (
    conversation_id VARCHAR(36) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (conversation_id, user_id),
    FOREIGN KEY (conversation_id) REFERENCES conversations(conversation_id) ON DELETE CASCADE
);

-- Tabela otimizada para LEITURA (Query Model)
CREATE TABLE IF NOT EXISTS users_directory (
    user_id VARCHAR(50) PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    name VARCHAR(100) NOT NULL,
    avatar_url VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS message_sequences_log (
    message_id VARCHAR(36) PRIMARY KEY,
    conversation_id VARCHAR(36) NOT NULL,
    sequence_number BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_identities (
    user_id VARCHAR(36) NOT NULL,
    channel VARCHAR(50) NOT NULL, -- 'whatsapp', 'instagram', 'telegram', 'delivery'
    external_id VARCHAR(255) NOT NULL, -- O número de telefone, @handle ou token push
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, channel)
);

CREATE INDEX IF NOT EXISTS idx_msg_seq_log_date ON message_sequences_log(created_at);
CREATE INDEX IF NOT EXISTS idx_members_conv ON conversation_members(conversation_id);
CREATE INDEX IF NOT EXISTS idx_members_user ON conversation_members(user_id);
CREATE INDEX IF NOT EXISTS idx_users_name ON users_directory(name);
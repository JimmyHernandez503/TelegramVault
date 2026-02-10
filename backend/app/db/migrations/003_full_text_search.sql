-- Full-Text Search indexes for TelegramVault

-- Add tsvector columns for faster FTS
ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS search_vector tsvector;
ALTER TABLE telegram_users ADD COLUMN IF NOT EXISTS search_vector tsvector;
ALTER TABLE detections ADD COLUMN IF NOT EXISTS search_vector tsvector;

-- Create GIN indexes for full-text search
CREATE INDEX IF NOT EXISTS idx_messages_fts ON telegram_messages USING GIN(search_vector);
CREATE INDEX IF NOT EXISTS idx_users_fts ON telegram_users USING GIN(search_vector);
CREATE INDEX IF NOT EXISTS idx_detections_fts ON detections USING GIN(search_vector);

-- Update existing messages with search vectors
UPDATE telegram_messages 
SET search_vector = to_tsvector('spanish', COALESCE(text, ''))
WHERE search_vector IS NULL AND text IS NOT NULL;

-- Update existing users with search vectors
UPDATE telegram_users 
SET search_vector = to_tsvector('spanish', 
    COALESCE(first_name, '') || ' ' || 
    COALESCE(last_name, '') || ' ' || 
    COALESCE(username, '') || ' ' ||
    COALESCE(bio, '')
)
WHERE search_vector IS NULL;

-- Update existing detections with search vectors
UPDATE detections 
SET search_vector = to_tsvector('spanish', 
    COALESCE(matched_text, '') || ' ' || 
    COALESCE(context_before, '') || ' ' || 
    COALESCE(context_after, '')
)
WHERE search_vector IS NULL;

-- Create trigger function for messages
CREATE OR REPLACE FUNCTION update_message_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector := to_tsvector('spanish', COALESCE(NEW.text, ''));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger function for users
CREATE OR REPLACE FUNCTION update_user_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector := to_tsvector('spanish', 
        COALESCE(NEW.first_name, '') || ' ' || 
        COALESCE(NEW.last_name, '') || ' ' || 
        COALESCE(NEW.username, '') || ' ' ||
        COALESCE(NEW.bio, '')
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger function for detections
CREATE OR REPLACE FUNCTION update_detection_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector := to_tsvector('spanish', 
        COALESCE(NEW.matched_text, '') || ' ' || 
        COALESCE(NEW.context_before, '') || ' ' || 
        COALESCE(NEW.context_after, '')
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers (drop first to avoid duplicates)
DROP TRIGGER IF EXISTS trg_message_search ON telegram_messages;
CREATE TRIGGER trg_message_search
    BEFORE INSERT OR UPDATE ON telegram_messages
    FOR EACH ROW EXECUTE FUNCTION update_message_search_vector();

DROP TRIGGER IF EXISTS trg_user_search ON telegram_users;
CREATE TRIGGER trg_user_search
    BEFORE INSERT OR UPDATE ON telegram_users
    FOR EACH ROW EXECUTE FUNCTION update_user_search_vector();

DROP TRIGGER IF EXISTS trg_detection_search ON detections;
CREATE TRIGGER trg_detection_search
    BEFORE INSERT OR UPDATE ON detections
    FOR EACH ROW EXECUTE FUNCTION update_detection_search_vector();

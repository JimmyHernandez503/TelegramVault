-- Migration 008: Add Full-Text Search Support
-- This migration adds search_vector columns and triggers for FTS

-- Add search_vector column to telegram_messages
ALTER TABLE telegram_messages 
ADD COLUMN IF NOT EXISTS search_vector tsvector;

-- Add search_vector column to telegram_users
ALTER TABLE telegram_users 
ADD COLUMN IF NOT EXISTS search_vector tsvector;

-- Add search_vector column to detections
ALTER TABLE detections 
ADD COLUMN IF NOT EXISTS search_vector tsvector;

-- Create function to update message search_vector
CREATE OR REPLACE FUNCTION update_message_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector := to_tsvector('spanish', COALESCE(NEW.text, ''));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for messages
DROP TRIGGER IF EXISTS message_search_vector_update ON telegram_messages;
CREATE TRIGGER message_search_vector_update
BEFORE INSERT OR UPDATE ON telegram_messages
FOR EACH ROW EXECUTE FUNCTION update_message_search_vector();

-- Create function to update user search_vector
CREATE OR REPLACE FUNCTION update_user_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector := to_tsvector('spanish', 
        COALESCE(NEW.username, '') || ' ' ||
        COALESCE(NEW.first_name, '') || ' ' ||
        COALESCE(NEW.last_name, '') || ' ' ||
        COALESCE(NEW.phone, '') || ' ' ||
        COALESCE(NEW.bio, '')
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for users
DROP TRIGGER IF EXISTS user_search_vector_update ON telegram_users;
CREATE TRIGGER user_search_vector_update
BEFORE INSERT OR UPDATE ON telegram_users
FOR EACH ROW EXECUTE FUNCTION update_user_search_vector();

-- Create function to update detection search_vector
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

-- Create trigger for detections
DROP TRIGGER IF EXISTS detection_search_vector_update ON detections;
CREATE TRIGGER detection_search_vector_update
BEFORE INSERT OR UPDATE ON detections
FOR EACH ROW EXECUTE FUNCTION update_detection_search_vector();

-- Create GIN indexes for fast FTS queries
CREATE INDEX IF NOT EXISTS idx_messages_search_vector ON telegram_messages USING GIN(search_vector);
CREATE INDEX IF NOT EXISTS idx_users_search_vector ON telegram_users USING GIN(search_vector);
CREATE INDEX IF NOT EXISTS idx_detections_search_vector ON detections USING GIN(search_vector);

-- Populate existing records with search vectors
UPDATE telegram_messages 
SET search_vector = to_tsvector('spanish', COALESCE(text, ''))
WHERE search_vector IS NULL;

UPDATE telegram_users 
SET search_vector = to_tsvector('spanish',
    COALESCE(username, '') || ' ' ||
    COALESCE(first_name, '') || ' ' ||
    COALESCE(last_name, '') || ' ' ||
    COALESCE(phone, '') || ' ' ||
    COALESCE(bio, '')
)
WHERE search_vector IS NULL;

UPDATE detections 
SET search_vector = to_tsvector('spanish',
    COALESCE(matched_text, '') || ' ' ||
    COALESCE(context_before, '') || ' ' ||
    COALESCE(context_after, '')
)
WHERE search_vector IS NULL;

-- Add unique constraint for telegram_id + group_id on messages table
-- This is required for ON CONFLICT to work in batch inserts

CREATE UNIQUE INDEX IF NOT EXISTS idx_telegram_messages_unique 
ON telegram_messages (telegram_id, group_id);

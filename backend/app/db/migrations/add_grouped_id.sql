-- Migration to add grouped_id column to telegram_messages table
-- This column stores the Telegram grouped_id for messages that are part of a media album

ALTER TABLE telegram_messages ADD COLUMN IF NOT EXISTS grouped_id BIGINT;
CREATE INDEX IF NOT EXISTS ix_telegram_messages_grouped_id ON telegram_messages(grouped_id);

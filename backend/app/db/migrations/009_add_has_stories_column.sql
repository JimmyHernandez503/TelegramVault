-- Migration 009: Add has_stories column to telegram_users table
-- This column tracks whether a user has active stories

-- Add has_stories column with default value FALSE
ALTER TABLE telegram_users 
ADD COLUMN IF NOT EXISTS has_stories BOOLEAN DEFAULT FALSE NOT NULL;

-- Update existing rows to have the default value
UPDATE telegram_users 
SET has_stories = FALSE 
WHERE has_stories IS NULL;

-- Add comment for documentation
COMMENT ON COLUMN telegram_users.has_stories IS 'Indicates whether the user currently has active stories';

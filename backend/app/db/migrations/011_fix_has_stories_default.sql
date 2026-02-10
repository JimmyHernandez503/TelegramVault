-- Migration 011: Fix column default values for telegram_users
-- This migration adds proper default values for all NOT NULL columns
-- to prevent constraint violations during user creation

-- Step 1: Update existing NULL values to defaults (if any exist)
UPDATE telegram_users SET has_stories = FALSE WHERE has_stories IS NULL;
UPDATE telegram_users SET is_watchlist = FALSE WHERE is_watchlist IS NULL;
UPDATE telegram_users SET is_favorite = FALSE WHERE is_favorite IS NULL;
UPDATE telegram_users SET is_bot = FALSE WHERE is_bot IS NULL;
UPDATE telegram_users SET is_deleted = FALSE WHERE is_deleted IS NULL;
UPDATE telegram_users SET is_fake = FALSE WHERE is_fake IS NULL;
UPDATE telegram_users SET is_premium = FALSE WHERE is_premium IS NULL;
UPDATE telegram_users SET is_restricted = FALSE WHERE is_restricted IS NULL;
UPDATE telegram_users SET is_scam = FALSE WHERE is_scam IS NULL;
UPDATE telegram_users SET is_verified = FALSE WHERE is_verified IS NULL;
UPDATE telegram_users SET messages_count = 0 WHERE messages_count IS NULL;
UPDATE telegram_users SET groups_count = 0 WHERE groups_count IS NULL;
UPDATE telegram_users SET media_count = 0 WHERE media_count IS NULL;
UPDATE telegram_users SET attachments_count = 0 WHERE attachments_count IS NULL;
UPDATE telegram_users SET created_at = NOW() WHERE created_at IS NULL;
UPDATE telegram_users SET updated_at = NOW() WHERE updated_at IS NULL;

-- Step 2: Set default values for all NOT NULL columns
ALTER TABLE telegram_users 
ALTER COLUMN has_stories SET DEFAULT FALSE,
ALTER COLUMN is_watchlist SET DEFAULT FALSE,
ALTER COLUMN is_favorite SET DEFAULT FALSE,
ALTER COLUMN is_bot SET DEFAULT FALSE,
ALTER COLUMN is_deleted SET DEFAULT FALSE,
ALTER COLUMN is_fake SET DEFAULT FALSE,
ALTER COLUMN is_premium SET DEFAULT FALSE,
ALTER COLUMN is_restricted SET DEFAULT FALSE,
ALTER COLUMN is_scam SET DEFAULT FALSE,
ALTER COLUMN is_verified SET DEFAULT FALSE,
ALTER COLUMN messages_count SET DEFAULT 0,
ALTER COLUMN groups_count SET DEFAULT 0,
ALTER COLUMN media_count SET DEFAULT 0,
ALTER COLUMN attachments_count SET DEFAULT 0,
ALTER COLUMN created_at SET DEFAULT NOW(),
ALTER COLUMN updated_at SET DEFAULT NOW();

-- Add comments for documentation
COMMENT ON COLUMN telegram_users.has_stories IS 'Indicates whether the user currently has active stories. Default is FALSE.';
COMMENT ON COLUMN telegram_users.is_watchlist IS 'Indicates whether the user is on the watchlist. Default is FALSE.';
COMMENT ON COLUMN telegram_users.is_favorite IS 'Indicates whether the user is marked as favorite. Default is FALSE.';
COMMENT ON COLUMN telegram_users.is_bot IS 'Indicates whether the user is a bot. Default is FALSE.';
COMMENT ON COLUMN telegram_users.is_deleted IS 'Indicates whether the user account is deleted. Default is FALSE.';
COMMENT ON COLUMN telegram_users.is_fake IS 'Indicates whether the user is marked as fake. Default is FALSE.';
COMMENT ON COLUMN telegram_users.is_premium IS 'Indicates whether the user has Telegram Premium. Default is FALSE.';
COMMENT ON COLUMN telegram_users.is_restricted IS 'Indicates whether the user is restricted. Default is FALSE.';
COMMENT ON COLUMN telegram_users.is_scam IS 'Indicates whether the user is marked as scam. Default is FALSE.';
COMMENT ON COLUMN telegram_users.is_verified IS 'Indicates whether the user is verified. Default is FALSE.';
COMMENT ON COLUMN telegram_users.messages_count IS 'Count of messages from this user. Default is 0.';
COMMENT ON COLUMN telegram_users.groups_count IS 'Count of groups this user is in. Default is 0.';
COMMENT ON COLUMN telegram_users.media_count IS 'Count of media items from this user. Default is 0.';
COMMENT ON COLUMN telegram_users.attachments_count IS 'Count of attachments from this user. Default is 0.';
COMMENT ON COLUMN telegram_users.created_at IS 'Timestamp when the user record was created. Default is NOW().';
COMMENT ON COLUMN telegram_users.updated_at IS 'Timestamp when the user record was last updated. Default is NOW().';

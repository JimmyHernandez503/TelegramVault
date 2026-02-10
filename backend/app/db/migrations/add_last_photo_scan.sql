-- Add last_photo_scan column to telegram_users
ALTER TABLE telegram_users ADD COLUMN IF NOT EXISTS last_photo_scan TIMESTAMP;

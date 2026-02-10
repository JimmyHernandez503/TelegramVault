ALTER TABLE user_profile_photos ADD COLUMN IF NOT EXISTS telegram_photo_id BIGINT;
ALTER TABLE user_profile_photos ADD COLUMN IF NOT EXISTS file_hash VARCHAR(64);
ALTER TABLE user_profile_photos ADD COLUMN IF NOT EXISTS is_video BOOLEAN DEFAULT FALSE;
ALTER TABLE user_profile_photos ADD COLUMN IF NOT EXISTS captured_at TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_profile_photos_telegram_id ON user_profile_photos(telegram_photo_id);
CREATE INDEX IF NOT EXISTS idx_profile_photos_user_current ON user_profile_photos(user_id, is_current);

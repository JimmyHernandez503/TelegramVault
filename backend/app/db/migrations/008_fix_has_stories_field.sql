-- Migration 008: Fix has_stories field in telegram_users table
-- This migration ensures the has_stories column has proper defaults and constraints
-- to prevent NOT NULL constraint violations during user creation

-- Step 1: Add has_stories column if it doesn't exist with default value FALSE
ALTER TABLE telegram_users 
ADD COLUMN IF NOT EXISTS has_stories BOOLEAN DEFAULT FALSE;

-- Step 2: Update existing NULL values to FALSE
UPDATE telegram_users 
SET has_stories = FALSE 
WHERE has_stories IS NULL;

-- Step 3: Add NOT NULL constraint
ALTER TABLE telegram_users 
ALTER COLUMN has_stories SET NOT NULL;

-- Step 4: Ensure default value is set for future inserts
ALTER TABLE telegram_users 
ALTER COLUMN has_stories SET DEFAULT FALSE;

-- Add comment for documentation
COMMENT ON COLUMN telegram_users.has_stories IS 'Indicates whether the user currently has active stories. Default is FALSE.';

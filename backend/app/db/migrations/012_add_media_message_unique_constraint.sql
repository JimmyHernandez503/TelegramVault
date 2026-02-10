-- Migration 012: Add Unique Constraint on media_files.message_id
-- This migration adds a unique constraint to ensure one media file per message
-- Required for proper UPSERT operations on media files
-- Requirements: 8.1, 8.2, 8.3

-- Add unique constraint on media_files.message_id if not exists
DO $$
BEGIN
    -- Check if constraint already exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'uq_media_files_message_id'
        AND table_name = 'media_files'
    ) THEN
        -- Create unique constraint
        ALTER TABLE media_files 
        ADD CONSTRAINT uq_media_files_message_id 
        UNIQUE (message_id);
        
        RAISE NOTICE 'Created unique constraint: uq_media_files_message_id';
    ELSE
        RAISE NOTICE 'Constraint uq_media_files_message_id already exists';
    END IF;
END $$;

-- Verify constraint was created successfully
DO $$
DECLARE
    constraint_count INTEGER;
BEGIN
    -- Count unique constraints on media_files.message_id
    SELECT COUNT(*) INTO constraint_count
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu 
        ON tc.constraint_name = kcu.constraint_name
    WHERE tc.constraint_type = 'UNIQUE'
        AND tc.table_name = 'media_files'
        AND kcu.column_name = 'message_id';
    
    IF constraint_count >= 1 THEN
        RAISE NOTICE 'SUCCESS: media_files has proper unique constraint on message_id';
    ELSE
        RAISE WARNING 'WARNING: media_files may not have proper unique constraint on message_id';
    END IF;
END $$;

-- Add comment for documentation
COMMENT ON CONSTRAINT uq_media_files_message_id ON media_files IS 
'Ensures one media file per message for proper UPSERT operations';

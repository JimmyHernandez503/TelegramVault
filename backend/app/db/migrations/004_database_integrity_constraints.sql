-- Database Integrity Constraints Migration
-- This migration ensures all necessary unique constraints exist for proper UPSERT operations
-- Requirements: 1.1, 1.2

-- Add unique constraint on telegram_messages (telegram_id, group_id) if not exists
-- Note: This may already exist from previous migration, so we use IF NOT EXISTS pattern
DO $$
BEGIN
    -- Check if constraint already exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'uq_telegram_messages_telegram_id_group_id'
        AND table_name = 'telegram_messages'
    ) THEN
        -- Check if unique index already exists (from previous migration)
        IF EXISTS (
            SELECT 1 FROM pg_indexes 
            WHERE indexname = 'idx_telegram_messages_unique'
            AND tablename = 'telegram_messages'
        ) THEN
            -- Convert existing unique index to named constraint
            ALTER TABLE telegram_messages 
            ADD CONSTRAINT uq_telegram_messages_telegram_id_group_id 
            UNIQUE USING INDEX idx_telegram_messages_unique;
            
            RAISE NOTICE 'Converted existing unique index to named constraint: uq_telegram_messages_telegram_id_group_id';
        ELSE
            -- Create new unique constraint
            ALTER TABLE telegram_messages 
            ADD CONSTRAINT uq_telegram_messages_telegram_id_group_id 
            UNIQUE (telegram_id, group_id);
            
            RAISE NOTICE 'Created new unique constraint: uq_telegram_messages_telegram_id_group_id';
        END IF;
    ELSE
        RAISE NOTICE 'Constraint uq_telegram_messages_telegram_id_group_id already exists';
    END IF;
END $$;

-- Ensure telegram_users has proper unique constraint on telegram_id
-- This should already exist from the model definition, but we verify it
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_type = 'UNIQUE'
        AND table_name = 'telegram_users'
        AND constraint_name LIKE '%telegram_id%'
    ) THEN
        ALTER TABLE telegram_users 
        ADD CONSTRAINT uq_telegram_users_telegram_id 
        UNIQUE (telegram_id);
        
        RAISE NOTICE 'Created unique constraint: uq_telegram_users_telegram_id';
    ELSE
        RAISE NOTICE 'Unique constraint on telegram_users.telegram_id already exists';
    END IF;
END $$;

-- Create performance indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_telegram_messages_timestamp 
ON telegram_messages(date);

CREATE INDEX IF NOT EXISTS idx_telegram_messages_sender_id 
ON telegram_messages(sender_id);

CREATE INDEX IF NOT EXISTS idx_telegram_messages_group_id 
ON telegram_messages(group_id);

-- Create index for message content search (if not exists)
CREATE INDEX IF NOT EXISTS idx_telegram_messages_text_gin 
ON telegram_messages USING gin(to_tsvector('english', text));

-- Verify constraints were created successfully
DO $$
DECLARE
    constraint_count INTEGER;
BEGIN
    -- Count unique constraints on telegram_messages
    SELECT COUNT(*) INTO constraint_count
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu 
        ON tc.constraint_name = kcu.constraint_name
    WHERE tc.constraint_type = 'UNIQUE'
        AND tc.table_name = 'telegram_messages'
        AND kcu.column_name IN ('telegram_id', 'group_id');
    
    IF constraint_count >= 2 THEN
        RAISE NOTICE 'SUCCESS: telegram_messages has proper unique constraint on (telegram_id, group_id)';
    ELSE
        RAISE WARNING 'WARNING: telegram_messages may not have proper unique constraint';
    END IF;
    
    -- Verify telegram_users constraint
    SELECT COUNT(*) INTO constraint_count
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu 
        ON tc.constraint_name = kcu.constraint_name
    WHERE tc.constraint_type = 'UNIQUE'
        AND tc.table_name = 'telegram_users'
        AND kcu.column_name = 'telegram_id';
    
    IF constraint_count >= 1 THEN
        RAISE NOTICE 'SUCCESS: telegram_users has proper unique constraint on telegram_id';
    ELSE
        RAISE WARNING 'WARNING: telegram_users may not have proper unique constraint';
    END IF;
END $$;
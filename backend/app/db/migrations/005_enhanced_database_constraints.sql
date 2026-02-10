-- Enhanced Database Constraints Migration
-- This migration ensures all necessary constraints and indexes exist for proper UPSERT operations
-- and addresses the specific issues identified in the TelegramVault Database Fixes spec
-- Requirements: 1.1, 1.2, 1.3

-- Function to safely add constraints and indexes
CREATE OR REPLACE FUNCTION add_constraint_if_not_exists(
    table_name TEXT,
    constraint_name TEXT,
    constraint_definition TEXT
) RETURNS VOID AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = add_constraint_if_not_exists.constraint_name
        AND table_name = add_constraint_if_not_exists.table_name
    ) THEN
        EXECUTE format('ALTER TABLE %I ADD CONSTRAINT %I %s', 
                      table_name, constraint_name, constraint_definition);
        RAISE NOTICE 'Added constraint % to table %', constraint_name, table_name;
    ELSE
        RAISE NOTICE 'Constraint % already exists on table %', constraint_name, table_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to safely add indexes
CREATE OR REPLACE FUNCTION add_index_if_not_exists(
    index_name TEXT,
    table_name TEXT,
    index_definition TEXT
) RETURNS VOID AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE indexname = add_index_if_not_exists.index_name
        AND tablename = add_index_if_not_exists.table_name
    ) THEN
        EXECUTE format('CREATE INDEX %I ON %I %s', 
                      index_name, table_name, index_definition);
        RAISE NOTICE 'Created index % on table %', index_name, table_name;
    ELSE
        RAISE NOTICE 'Index % already exists on table %', index_name, table_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- 1. CRITICAL: Ensure telegram_messages has proper unique constraint for UPSERT operations
-- This is essential for ON CONFLICT (telegram_id, group_id) DO NOTHING/UPDATE to work

-- First, handle any existing unique index that might conflict
DO $$
BEGIN
    -- If the old unique index exists without a proper constraint name, drop it
    IF EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE indexname = 'idx_telegram_messages_unique'
        AND tablename = 'telegram_messages'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'uq_telegram_messages_telegram_id_group_id'
        AND table_name = 'telegram_messages'
    ) THEN
        DROP INDEX IF EXISTS idx_telegram_messages_unique;
        RAISE NOTICE 'Dropped old unique index to replace with named constraint';
    END IF;
END $$;

-- Add the properly named unique constraint
SELECT add_constraint_if_not_exists(
    'telegram_messages',
    'uq_telegram_messages_telegram_id_group_id',
    'UNIQUE (telegram_id, group_id)'
);

-- 2. Ensure telegram_users has proper unique constraint on telegram_id
-- This should exist from the model, but we verify and create if missing
SELECT add_constraint_if_not_exists(
    'telegram_users',
    'uq_telegram_users_telegram_id',
    'UNIQUE (telegram_id)'
);

-- 3. Create performance indexes for common query patterns
-- These indexes improve query performance for message retrieval and filtering

-- Index for timestamp-based queries (message history, date ranges)
SELECT add_index_if_not_exists(
    'idx_telegram_messages_date',
    'telegram_messages',
    '(date)'
);

-- Index for sender-based queries (user message history)
SELECT add_index_if_not_exists(
    'idx_telegram_messages_sender_id',
    'telegram_messages',
    '(sender_id)'
);

-- Index for group-based queries (group message retrieval)
SELECT add_index_if_not_exists(
    'idx_telegram_messages_group_id',
    'telegram_messages',
    '(group_id)'
);

-- Composite index for group + date queries (common pattern)
SELECT add_index_if_not_exists(
    'idx_telegram_messages_group_date',
    'telegram_messages',
    '(group_id, date)'
);

-- Index for reply message lookups
SELECT add_index_if_not_exists(
    'idx_telegram_messages_reply_to',
    'telegram_messages',
    '(reply_to_msg_id)'
);

-- Index for grouped messages (media albums)
SELECT add_index_if_not_exists(
    'idx_telegram_messages_grouped_id',
    'telegram_messages',
    '(grouped_id)'
);

-- 4. Create indexes for telegram_users table performance
SELECT add_index_if_not_exists(
    'idx_telegram_users_username',
    'telegram_users',
    '(username)'
);

SELECT add_index_if_not_exists(
    'idx_telegram_users_last_seen',
    'telegram_users',
    '(last_seen)'
);

-- 5. Full-text search index for message content (if not exists)
SELECT add_index_if_not_exists(
    'idx_telegram_messages_text_search',
    'telegram_messages',
    'USING gin(to_tsvector(''english'', COALESCE(text, '''')))'
);

-- 6. Verification and validation
DO $$
DECLARE
    constraint_count INTEGER;
    index_count INTEGER;
BEGIN
    -- Verify telegram_messages unique constraint
    SELECT COUNT(*) INTO constraint_count
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu 
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    WHERE tc.constraint_type = 'UNIQUE'
        AND tc.table_name = 'telegram_messages'
        AND tc.constraint_name = 'uq_telegram_messages_telegram_id_group_id';
    
    IF constraint_count > 0 THEN
        RAISE NOTICE 'SUCCESS: telegram_messages has proper unique constraint for UPSERT operations';
    ELSE
        RAISE WARNING 'CRITICAL: telegram_messages missing unique constraint - UPSERT operations will fail!';
    END IF;
    
    -- Verify telegram_users unique constraint
    SELECT COUNT(*) INTO constraint_count
    FROM information_schema.table_constraints tc
    WHERE tc.constraint_type = 'UNIQUE'
        AND tc.table_name = 'telegram_users'
        AND tc.constraint_name LIKE '%telegram_id%';
    
    IF constraint_count > 0 THEN
        RAISE NOTICE 'SUCCESS: telegram_users has proper unique constraint on telegram_id';
    ELSE
        RAISE WARNING 'WARNING: telegram_users missing unique constraint on telegram_id';
    END IF;
    
    -- Count performance indexes
    SELECT COUNT(*) INTO index_count
    FROM pg_indexes 
    WHERE tablename = 'telegram_messages'
        AND indexname LIKE 'idx_telegram_messages_%';
    
    RAISE NOTICE 'Created % performance indexes for telegram_messages table', index_count;
    
    -- Final success message
    RAISE NOTICE 'Database constraints and indexes migration completed successfully';
END $$;

-- Clean up helper functions
DROP FUNCTION IF EXISTS add_constraint_if_not_exists(TEXT, TEXT, TEXT);
DROP FUNCTION IF EXISTS add_index_if_not_exists(TEXT, TEXT, TEXT);

-- Add comment to track this migration
COMMENT ON TABLE telegram_messages IS 'Enhanced with unique constraints for UPSERT operations - Migration 005';
COMMENT ON TABLE telegram_users IS 'Verified unique constraints for UPSERT operations - Migration 005';
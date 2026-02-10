-- Add preview_retry_count column to invite_links table
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'invite_links' 
        AND column_name = 'preview_retry_count'
    ) THEN
        ALTER TABLE invite_links ADD COLUMN preview_retry_count INTEGER DEFAULT 0;
    END IF;
END $$;

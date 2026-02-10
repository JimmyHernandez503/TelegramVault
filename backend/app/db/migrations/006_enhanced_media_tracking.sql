-- Migration 006: Enhanced Media Tracking Fields
-- This migration adds enhanced tracking fields to the media_files table
-- and creates new tables for download task management and batch processing

-- Add enhanced tracking fields to media_files table
ALTER TABLE media_files 
ADD COLUMN IF NOT EXISTS download_attempts INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS last_download_attempt TIMESTAMP,
ADD COLUMN IF NOT EXISTS download_error_category VARCHAR(50),
ADD COLUMN IF NOT EXISTS validation_status VARCHAR(20) DEFAULT 'pending',
ADD COLUMN IF NOT EXISTS validation_error TEXT,
ADD COLUMN IF NOT EXISTS processing_status VARCHAR(20) DEFAULT 'pending',
ADD COLUMN IF NOT EXISTS processing_priority INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS duplicate_detection_method VARCHAR(20);

-- Add index on perceptual_hash if it doesn't exist
CREATE INDEX IF NOT EXISTS idx_media_files_perceptual_hash ON media_files(perceptual_hash);

-- Add indexes for new fields
CREATE INDEX IF NOT EXISTS idx_media_files_processing_status ON media_files(processing_status);
CREATE INDEX IF NOT EXISTS idx_media_files_validation_status ON media_files(validation_status);
CREATE INDEX IF NOT EXISTS idx_media_files_download_attempts ON media_files(download_attempts);
CREATE INDEX IF NOT EXISTS idx_media_files_processing_priority ON media_files(processing_priority);

-- Create download_tasks table
CREATE TABLE IF NOT EXISTS download_tasks (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR(64) UNIQUE NOT NULL,
    media_file_id INTEGER NOT NULL REFERENCES media_files(id) ON DELETE CASCADE,
    
    -- Task details
    task_type VARCHAR(20) NOT NULL,
    priority INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'queued',
    
    -- Processing details
    assigned_worker VARCHAR(50),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    
    -- Error handling and retry logic
    error_message TEXT,
    error_category VARCHAR(50),
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    next_retry_at TIMESTAMP,
    
    -- Task metadata
    task_data TEXT,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for download_tasks
CREATE INDEX IF NOT EXISTS idx_download_tasks_task_id ON download_tasks(task_id);
CREATE INDEX IF NOT EXISTS idx_download_tasks_media_file_id ON download_tasks(media_file_id);
CREATE INDEX IF NOT EXISTS idx_download_tasks_status ON download_tasks(status);
CREATE INDEX IF NOT EXISTS idx_download_tasks_priority ON download_tasks(priority);
CREATE INDEX IF NOT EXISTS idx_download_tasks_next_retry_at ON download_tasks(next_retry_at);
CREATE INDEX IF NOT EXISTS idx_download_tasks_task_type ON download_tasks(task_type);

-- Create batch_processing table
CREATE TABLE IF NOT EXISTS batch_processing (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(64) UNIQUE NOT NULL,
    batch_type VARCHAR(20) NOT NULL,
    
    -- Batch configuration
    batch_size INTEGER NOT NULL,
    max_concurrent INTEGER DEFAULT 5,
    filter_criteria TEXT,
    
    -- Progress tracking
    status VARCHAR(20) DEFAULT 'pending',
    total_items INTEGER DEFAULT 0,
    processed_items INTEGER DEFAULT 0,
    successful_items INTEGER DEFAULT 0,
    failed_items INTEGER DEFAULT 0,
    skipped_items INTEGER DEFAULT 0,
    
    -- Timing and estimation
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    estimated_completion TIMESTAMP,
    
    -- Checkpointing
    last_checkpoint TIMESTAMP,
    checkpoint_data TEXT,
    
    -- Error tracking
    error_summary TEXT,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for batch_processing
CREATE INDEX IF NOT EXISTS idx_batch_processing_batch_id ON batch_processing(batch_id);
CREATE INDEX IF NOT EXISTS idx_batch_processing_status ON batch_processing(status);
CREATE INDEX IF NOT EXISTS idx_batch_processing_batch_type ON batch_processing(batch_type);
CREATE INDEX IF NOT EXISTS idx_batch_processing_started_at ON batch_processing(started_at);

-- Add foreign key relationship between download_tasks and batch_processing
ALTER TABLE download_tasks 
ADD COLUMN IF NOT EXISTS batch_processing_id INTEGER REFERENCES batch_processing(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_download_tasks_batch_processing_id ON download_tasks(batch_processing_id);

-- Update existing media files to set default values for new fields
UPDATE media_files 
SET 
    download_attempts = 0,
    validation_status = CASE 
        WHEN file_path IS NOT NULL AND download_error IS NULL THEN 'valid'
        WHEN download_error IS NOT NULL THEN 'invalid'
        ELSE 'pending'
    END,
    processing_status = CASE 
        WHEN file_path IS NOT NULL AND download_error IS NULL THEN 'completed'
        WHEN download_error IS NOT NULL THEN 'failed'
        ELSE 'pending'
    END,
    processing_priority = 0
WHERE 
    download_attempts IS NULL 
    OR validation_status IS NULL 
    OR processing_status IS NULL 
    OR processing_priority IS NULL;

-- Create a function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updated_at on new tables
CREATE TRIGGER update_download_tasks_updated_at 
    BEFORE UPDATE ON download_tasks 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_batch_processing_updated_at 
    BEFORE UPDATE ON batch_processing 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Add comments for documentation
COMMENT ON TABLE download_tasks IS 'Tracks individual download tasks in the queue system';
COMMENT ON TABLE batch_processing IS 'Tracks batch processing operations for media downloads';
COMMENT ON COLUMN media_files.download_attempts IS 'Number of download attempts made for this media file';
COMMENT ON COLUMN media_files.validation_status IS 'Status of media file validation: pending, valid, invalid, corrupted';
COMMENT ON COLUMN media_files.processing_status IS 'Status of media file processing: pending, queued, processing, completed, failed';
COMMENT ON COLUMN media_files.processing_priority IS 'Priority for processing this media file (higher = more priority)';
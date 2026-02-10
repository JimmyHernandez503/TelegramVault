-- Migration 007: Create download_tasks and batch_processing tables
-- Purpose: Add task queue management and batch processing tracking
-- Date: 2026-01-31

-- Create batch_processing table first (referenced by download_tasks)
CREATE TABLE IF NOT EXISTS batch_processing (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(64) UNIQUE NOT NULL,
    batch_type VARCHAR(20) NOT NULL,
    
    -- Batch configuration
    batch_size INTEGER NOT NULL,
    max_concurrent INTEGER DEFAULT 5,
    filter_criteria TEXT,
    
    -- Progress tracking
    status VARCHAR(20) DEFAULT 'pending' NOT NULL,
    total_items INTEGER DEFAULT 0,
    processed_items INTEGER DEFAULT 0,
    successful_items INTEGER DEFAULT 0,
    failed_items INTEGER DEFAULT 0,
    skipped_items INTEGER DEFAULT 0,
    
    -- Timing and estimation
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    estimated_completion TIMESTAMP,
    
    -- Checkpointing for resumability
    last_checkpoint TIMESTAMP,
    checkpoint_data TEXT,
    
    -- Error tracking
    error_summary TEXT,
    
    -- Metadata
    metadata TEXT,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for batch_processing
CREATE INDEX IF NOT EXISTS idx_batch_processing_batch_id ON batch_processing(batch_id);
CREATE INDEX IF NOT EXISTS idx_batch_processing_batch_type ON batch_processing(batch_type);
CREATE INDEX IF NOT EXISTS idx_batch_processing_status ON batch_processing(status);

-- Create download_tasks table
CREATE TABLE IF NOT EXISTS download_tasks (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR(64) UNIQUE NOT NULL,
    media_file_id INTEGER NOT NULL REFERENCES media_files(id) ON DELETE CASCADE,
    batch_id INTEGER REFERENCES batch_processing(id) ON DELETE SET NULL,
    
    -- Task details
    task_type VARCHAR(20) NOT NULL,
    priority INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'queued' NOT NULL,
    
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
CREATE INDEX IF NOT EXISTS idx_download_tasks_batch_id ON download_tasks(batch_id);
CREATE INDEX IF NOT EXISTS idx_download_tasks_task_type ON download_tasks(task_type);
CREATE INDEX IF NOT EXISTS idx_download_tasks_priority ON download_tasks(priority);
CREATE INDEX IF NOT EXISTS idx_download_tasks_status ON download_tasks(status);
CREATE INDEX IF NOT EXISTS idx_download_tasks_error_category ON download_tasks(error_category);
CREATE INDEX IF NOT EXISTS idx_download_tasks_next_retry_at ON download_tasks(next_retry_at);

-- Create composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_download_tasks_status_priority ON download_tasks(status, priority DESC);
CREATE INDEX IF NOT EXISTS idx_download_tasks_status_next_retry ON download_tasks(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_download_tasks_batch_status ON download_tasks(batch_id, status);

-- Create trigger to update updated_at timestamp for batch_processing
CREATE OR REPLACE FUNCTION update_batch_processing_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_batch_processing_updated_at
    BEFORE UPDATE ON batch_processing
    FOR EACH ROW
    EXECUTE FUNCTION update_batch_processing_updated_at();

-- Create trigger to update updated_at timestamp for download_tasks
CREATE OR REPLACE FUNCTION update_download_tasks_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_download_tasks_updated_at
    BEFORE UPDATE ON download_tasks
    FOR EACH ROW
    EXECUTE FUNCTION update_download_tasks_updated_at();

-- Add comments for documentation
COMMENT ON TABLE batch_processing IS 'Tracks batch processing operations for media downloads and retries';
COMMENT ON TABLE download_tasks IS 'Individual download tasks in the queue system';
COMMENT ON COLUMN batch_processing.batch_id IS 'Unique identifier for the batch';
COMMENT ON COLUMN batch_processing.checkpoint_data IS 'JSON data for resuming interrupted batches';
COMMENT ON COLUMN download_tasks.task_id IS 'Unique identifier for the task';
COMMENT ON COLUMN download_tasks.task_data IS 'JSON data for task-specific parameters';

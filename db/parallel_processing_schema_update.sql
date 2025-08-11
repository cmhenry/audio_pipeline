-- Schema updates for parallel processing support
-- Run this after the main schema to enable parallel job tracking

-- Create table for tracking individual tar file processing
CREATE TABLE IF NOT EXISTS parallel_job_tracking (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    master_job_id INTEGER NOT NULL,  -- SLURM job ID of master controller
    worker_job_id INTEGER,           -- SLURM job ID of worker processing this tar
    date_str VARCHAR(10) NOT NULL,   -- YYYY-MM-DD
    timestamp_str VARCHAR(5) NOT NULL, -- HH_MM
    tar_filename VARCHAR(255) NOT NULL,
    tar_file_path VARCHAR(500) NOT NULL,
    
    -- Processing status for this specific tar file
    status VARCHAR(50) DEFAULT 'pending',  -- pending, processing, completed, failed
    processing_start TIMESTAMP,
    processing_end TIMESTAMP,
    files_processed INTEGER DEFAULT 0,
    files_failed INTEGER DEFAULT 0,
    error_message TEXT,
    
    -- Resource tracking
    gpu_node VARCHAR(100),
    temp_dir_path VARCHAR(500),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(master_job_id, tar_filename)
);

-- Add trigger for updated_at
DROP TRIGGER IF EXISTS update_parallel_job_tracking_updated_at ON parallel_job_tracking;
CREATE TRIGGER update_parallel_job_tracking_updated_at
    BEFORE UPDATE ON parallel_job_tracking
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create indexes for parallel job tracking
CREATE INDEX IF NOT EXISTS idx_parallel_jobs_master_id ON parallel_job_tracking(master_job_id);
CREATE INDEX IF NOT EXISTS idx_parallel_jobs_worker_id ON parallel_job_tracking(worker_job_id);
CREATE INDEX IF NOT EXISTS idx_parallel_jobs_status ON parallel_job_tracking(status);
CREATE INDEX IF NOT EXISTS idx_parallel_jobs_date ON parallel_job_tracking(date_str);
CREATE INDEX IF NOT EXISTS idx_parallel_jobs_timestamp ON parallel_job_tracking(date_str, timestamp_str);

-- Add columns to processing_queue for parallel processing metadata
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name='processing_queue' AND column_name='parallel_processing') THEN
        ALTER TABLE processing_queue 
        ADD COLUMN parallel_processing BOOLEAN DEFAULT FALSE,
        ADD COLUMN total_tar_files INTEGER,
        ADD COLUMN completed_tar_files INTEGER DEFAULT 0,
        ADD COLUMN failed_tar_files INTEGER DEFAULT 0;
    END IF;
END $$;

-- View for parallel processing monitoring
CREATE OR REPLACE VIEW parallel_processing_status AS
SELECT 
    pq.year,
    pq.month,
    pq.date,
    pq.location,
    pq.status as overall_status,
    pq.parallel_processing,
    pq.total_tar_files,
    pq.completed_tar_files,
    pq.failed_tar_files,
    pq.processing_start,
    pq.processing_end,
    pq.slurm_job_id as master_job_id,
    
    -- Aggregate stats from individual jobs
    COUNT(pjt.id) as tracked_jobs,
    COUNT(CASE WHEN pjt.status = 'completed' THEN 1 END) as completed_jobs,
    COUNT(CASE WHEN pjt.status = 'failed' THEN 1 END) as failed_jobs,
    COUNT(CASE WHEN pjt.status = 'processing' THEN 1 END) as running_jobs,
    SUM(pjt.files_processed) as total_files_processed,
    SUM(pjt.files_failed) as total_files_failed,
    
    -- Time estimates
    CASE 
        WHEN pq.processing_end IS NOT NULL THEN 
            EXTRACT(EPOCH FROM (pq.processing_end - pq.processing_start))/3600 
        ELSE NULL 
    END as processing_hours,
    
    pq.error_message

FROM processing_queue pq
LEFT JOIN parallel_job_tracking pjt ON pq.slurm_job_id = pjt.master_job_id
WHERE pq.parallel_processing = TRUE
GROUP BY pq.id, pq.year, pq.month, pq.date, pq.location, pq.status, 
         pq.parallel_processing, pq.total_tar_files, pq.completed_tar_files, 
         pq.failed_tar_files, pq.processing_start, pq.processing_end, 
         pq.slurm_job_id, pq.error_message
ORDER BY pq.processing_start DESC;

-- Function to update parallel processing progress
CREATE OR REPLACE FUNCTION update_parallel_progress(
    p_master_job_id INTEGER
) RETURNS VOID AS $$
DECLARE
    completed_count INTEGER;
    failed_count INTEGER;
    total_count INTEGER;
BEGIN
    -- Count completed and failed jobs for this master job
    SELECT 
        COUNT(CASE WHEN status = 'completed' THEN 1 END),
        COUNT(CASE WHEN status = 'failed' THEN 1 END),
        COUNT(*)
    INTO completed_count, failed_count, total_count
    FROM parallel_job_tracking 
    WHERE master_job_id = p_master_job_id;
    
    -- Update the processing_queue table
    UPDATE processing_queue 
    SET 
        completed_tar_files = completed_count,
        failed_tar_files = failed_count,
        updated_at = CURRENT_TIMESTAMP
    WHERE slurm_job_id = p_master_job_id;
END;
$$ LANGUAGE plpgsql;

-- Create a view for debugging individual tar processing
CREATE OR REPLACE VIEW parallel_job_details AS
SELECT 
    pjt.date_str,
    pjt.timestamp_str,
    pjt.tar_filename,
    pjt.status,
    pjt.master_job_id,
    pjt.worker_job_id,
    pjt.files_processed,
    pjt.files_failed,
    pjt.processing_start,
    pjt.processing_end,
    CASE 
        WHEN pjt.processing_end IS NOT NULL AND pjt.processing_start IS NOT NULL THEN
            EXTRACT(EPOCH FROM (pjt.processing_end - pjt.processing_start))/60 
        ELSE NULL 
    END as processing_minutes,
    pjt.gpu_node,
    pjt.error_message,
    
    -- Join with main processing queue
    pq.status as overall_status,
    pq.location
FROM parallel_job_tracking pjt
JOIN processing_queue pq ON pjt.master_job_id = pq.slurm_job_id
ORDER BY pjt.date_str DESC, pjt.timestamp_str ASC;

COMMENT ON TABLE parallel_job_tracking IS 'Tracks individual tar file processing in parallel pipeline';
COMMENT ON VIEW parallel_processing_status IS 'Aggregated view of parallel processing progress';
COMMENT ON VIEW parallel_job_details IS 'Detailed view of individual tar file processing jobs';
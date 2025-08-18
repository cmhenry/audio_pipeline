# Audio Pipeline Database Schema Documentation

## Overview

This database stores audio file metadata and their transcripts for an audio processing pipeline. It's designed to handle millions of records efficiently with full-text search capabilities.

## Database Connection

```
Database Name: audio_pipeline
Default User: audio_user
Default Password: audio_password (CHANGE THIS!)
Connection String: postgresql://audio_user:audio_password@localhost:5432/audio_pipeline
```

## Tables

### 1. `audio_files`

Stores metadata about audio files.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | UUID | PRIMARY KEY | Unique identifier (auto-generated) |
| `filename` | VARCHAR(255) | NOT NULL, UNIQUE | Audio file name (e.g., "call_001.wav") |
| `file_path` | VARCHAR(500) | | Full path to file in storage |
| `file_size_bytes` | BIGINT | | File size in bytes |
| `month` | INTEGER | NOT NULL, 1-12 | Month of recording |
| `date` | INTEGER | NOT NULL, 1-31 | Day of recording |
| `year` | INTEGER | NOT NULL, ≥2000 | Year of recording |
| `classification` | VARCHAR(100) | | Category (e.g., "customer_support") |
| `classification_probability` | DECIMAL(3,2) | 0.00-1.00 | Confidence score |
| `created_at` | TIMESTAMP | | When record was created |
| `updated_at` | TIMESTAMP | | Last update time (auto-updated) |

### 2. `transcripts`

Stores transcript text separately for performance.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | UUID | PRIMARY KEY | Unique identifier (auto-generated) |
| `audio_file_id` | UUID | NOT NULL, UNIQUE, FK | Links to audio_files.id |
| `transcript_text` | TEXT | NOT NULL | Full transcript text |
| `word_count` | INTEGER | | Number of words in transcript |
| `duration_seconds` | DECIMAL(10,2) | | Audio duration (e.g., 180.50) |
| `created_at` | TIMESTAMP | | When transcript was created |

### 3. `audio_with_transcripts` (VIEW)

Convenient view joining both tables.

```sql
SELECT * FROM audio_with_transcripts 
WHERE filename = 'call_001.wav';
```

## Indexes

- **Date lookup**: `idx_audio_files_date` on (year, month, date)
- **Classification**: `idx_audio_files_classification` 
- **Filename search**: `idx_audio_files_filename`
- **Full-text search**: `idx_transcripts_text_search` using PostgreSQL GIN


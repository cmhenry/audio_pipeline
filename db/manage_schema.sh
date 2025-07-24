#!/bin/bash
# manage_schema.sh - Create and manage audio pipeline database schema

# Configuration
DB_NAME="audio_pipeline"
DB_USER="audio_user"
DB_PASSWORD="audio_password"  # Should match setup script or be updated

# Schema version for future migrations
SCHEMA_VERSION="2.1"

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=== Audio Pipeline Schema Management ===${NC}"
echo "Schema Version: $SCHEMA_VERSION"
echo

# Function to execute SQL and check status
exec_sql() {
    local sql="$1"
    local description="$2"
    
    PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME -c "$sql" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo -e "   ${GREEN}✓${NC} $description"
        return 0
    else
        echo -e "   ${RED}✗${NC} $description"
        return 1
    fi
}

# Check connection first
echo "1. Testing database connection..."
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME -c "SELECT 1;" > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo -e "   ${RED}✗${NC} Cannot connect to database!"
    echo "   Please ensure PostgreSQL is running and credentials are correct."
    echo "   Run ./setup_postgresql.sh first if needed."
    exit 1
fi
echo -e "   ${GREEN}✓${NC} Connected to database"

# Prompt for action
echo
echo "Select operation:"
echo "  1) Create/Update schema (safe - preserves data)"
echo "  2) Reset schema (WARNING: deletes all data)"
echo "  3) Show current schema"
echo "  4) Exit"
echo
read -p "Enter choice [1-4]: " choice

case $choice in
    1)
        echo
        echo -e "${BLUE}Creating/Updating Schema...${NC}"
        ;;
    2)
        echo
        echo -e "${RED}⚠️  WARNING: This will DELETE all data!${NC}"
        read -p "Type 'DELETE ALL DATA' to confirm: " confirmation
        if [ "$confirmation" != "DELETE ALL DATA" ]; then
            echo "Reset cancelled."
            exit 0
        fi
        echo
        echo -e "${BLUE}Resetting Schema...${NC}"
        
        # Drop all objects
        PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME << 'EOF' > /dev/null 2>&1
DROP VIEW IF EXISTS audio_with_transcripts CASCADE;
DROP TABLE IF EXISTS processing_queue CASCADE;
DROP TABLE IF EXISTS audio_metadata CASCADE;
DROP TABLE IF EXISTS transcripts CASCADE;
DROP TABLE IF EXISTS audio_files CASCADE;
DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;
EOF
        echo -e "   ${GREEN}✓${NC} Existing schema dropped"
        ;;
    3)
        echo
        echo -e "${BLUE}Current Schema:${NC}"
        PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME << 'EOF'
\echo 'Tables:'
\dt
\echo
\echo 'Views:'
\dv
\echo
\echo 'Indexes:'
\di
\echo
\echo 'Functions:'
\df
EOF
        exit 0
        ;;
    4)
        echo "Exiting..."
        exit 0
        ;;
    *)
        echo "Invalid choice."
        exit 1
        ;;
esac

# Create schema
echo
echo "2. Creating schema objects..."

# Create or replace function for updated_at
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME << 'EOF' > /dev/null 2>&1
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';
EOF
echo -e "   ${GREEN}✓${NC} Created update_updated_at function"

# Create audio_files table
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME << 'EOF' > /dev/null 2>&1
CREATE TABLE IF NOT EXISTS audio_files (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    filename VARCHAR(255) NOT NULL UNIQUE,
    file_path VARCHAR(500),
    file_size_bytes BIGINT,
    month INTEGER NOT NULL CHECK (month >= 1 AND month <= 12),
    date INTEGER NOT NULL CHECK (date >= 1 AND date <= 31),
    year INTEGER NOT NULL CHECK (year >= 2000),
    classification VARCHAR(100),
    classification_probability DECIMAL(3,2) CHECK (classification_probability >= 0 AND classification_probability <= 1),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add trigger if not exists
DROP TRIGGER IF EXISTS update_audio_files_updated_at ON audio_files;
CREATE TRIGGER update_audio_files_updated_at
    BEFORE UPDATE ON audio_files
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
EOF
echo -e "   ${GREEN}✓${NC} Created audio_files table"

# Create transcripts table
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME << 'EOF' > /dev/null 2>&1
CREATE TABLE IF NOT EXISTS transcripts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    audio_file_id UUID NOT NULL UNIQUE REFERENCES audio_files(id) ON DELETE CASCADE,
    transcript_text TEXT NOT NULL,
    word_count INTEGER,
    duration_seconds DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
EOF
echo -e "   ${GREEN}✓${NC} Created transcripts table"

# Create audio_metadata table
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME << 'EOF' > /dev/null 2>&1
CREATE TABLE IF NOT EXISTS audio_metadata (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    audio_file_id UUID NOT NULL UNIQUE REFERENCES audio_files(id) ON DELETE CASCADE,

    -- Core metadata
    meta_id VARCHAR(255),
    poi_id VARCHAR(255),
    meta_createtime TIMESTAMP,
    meta_scheduletime TIMESTAMP,
    timestamp BIGINT,
    collection_timestamp TIMESTAMP,

    -- Content status and settings
    meta_itemcommentstatus INTEGER,
    meta_diversificationid VARCHAR(255),
    meta_secret BOOLEAN DEFAULT FALSE,
    meta_privateitem BOOLEAN DEFAULT FALSE,
    meta_duetenabled BOOLEAN DEFAULT TRUE,
    meta_stitchenabled BOOLEAN DEFAULT TRUE,
    meta_indexenabled BOOLEAN DEFAULT TRUE,
    meta_iscontentclassified BOOLEAN DEFAULT FALSE,
    meta_isaigc BOOLEAN DEFAULT FALSE,
    meta_isad BOOLEAN DEFAULT FALSE,
    meta_isecvideo BOOLEAN DEFAULT FALSE,

    -- Author information
    author_id VARCHAR(255),
    author_uniqueid VARCHAR(255),
    author_nickname TEXT,
    author_signature TEXT,
    author_roomid VARCHAR(255),
    author_verified BOOLEAN DEFAULT FALSE,
    author_openfavorite BOOLEAN DEFAULT TRUE,
    author_commentsetting INTEGER,
    author_duetsetting INTEGER,
    author_stitchsetting INTEGER,
    author_downloadsetting INTEGER,

    -- Music information
    music_id VARCHAR(255),
    music_title TEXT,
    music_authorname VARCHAR(255),
    music_album TEXT,
    music_duration INTEGER,
    music_schedulesearchtime TIMESTAMP,
    music_collected BOOLEAN DEFAULT FALSE,

    -- Statistics
    stats_diggcount BIGINT DEFAULT 0,
    stats_sharecount BIGINT DEFAULT 0,
    stats_commentcount BIGINT DEFAULT 0,
    stats_playcount BIGINT DEFAULT 0,
    stats_collectcount BIGINT DEFAULT 0,

    -- Video specifications
    video_height INTEGER,
    video_width INTEGER,
    video_duration INTEGER,
    video_bitrate INTEGER,
    video_ratio VARCHAR(50),
    video_encodedtype VARCHAR(50),
    video_format VARCHAR(50),
    video_videoquality VARCHAR(50),
    video_codectype VARCHAR(50),
    video_definition VARCHAR(50),

    -- Location/POI information
    poi_type VARCHAR(100),
    poi_name TEXT,
    poi_address TEXT,
    poi_city VARCHAR(255),
    poi_citycode VARCHAR(50),
    poi_province VARCHAR(255),
    poi_country VARCHAR(255),
    poi_countrycode VARCHAR(10),
    poi_fatherpoiid VARCHAR(255),
    poi_fatherpoiname TEXT,
    poi_category VARCHAR(255),
    poi_tttypecode VARCHAR(50),
    poi_typecode VARCHAR(50),
    poi_tttypenametiny VARCHAR(255),
    poi_tttypenamemedium VARCHAR(255),
    poi_tttypenamesuper VARCHAR(255),

    -- Address information
    adress_addresscountry VARCHAR(255),
    adress_addresslocality VARCHAR(255),
    adress_addressregion VARCHAR(255),
    adress_streetaddress TEXT,

    -- Status and messages
    statuscode INTEGER,
    statusmsg TEXT,

    -- Descriptions and text content
    meta_desc TEXT,
    meta_locationcreated TEXT,
    processed_desc TEXT,
    description_hash VARCHAR(255),

    -- AI/Classification
    meta_aigclabeltype VARCHAR(100),
    meta_aigcdescription TEXT,

    -- Arrays stored as JSONB for flexibility
    meta_diversificationlabels JSONB,
    meta_serverabversions JSONB,
    meta_suggestedwords JSONB,
    meta_adlabelversion JSONB,
    meta_bainfo JSONB,
    subtitle_subtitle_lang JSONB,
    bitrate_bitrate_info JSONB,
    text_extra_user_mention JSONB,
    text_extra_hashtag_mention JSONB,
    warning_warning JSONB,

    -- Duet information
    duetinfo_duetfromid VARCHAR(255),

    -- Processing metadata
    pol VARCHAR(50),
    hour INTEGER,
    country VARCHAR(255),

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add trigger if not exists
DROP TRIGGER IF EXISTS update_audio_metadata_updated_at ON audio_metadata;
CREATE TRIGGER update_audio_metadata_updated_at
    BEFORE UPDATE ON audio_metadata
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
EOF
echo -e "   ${GREEN}✓${NC} Created audio_metadata table"

# Create processing_queue table
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME << 'EOF' > /dev/null 2>&1
CREATE TABLE IF NOT EXISTS processing_queue (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    date INTEGER NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    transfer_start TIMESTAMP,
    transfer_end TIMESTAMP,
    transfer_task_id VARCHAR(255),
    processing_start TIMESTAMP,
    processing_end TIMESTAMP,
    slurm_job_id INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(year, month, date)
);

-- Add trigger if not exists
DROP TRIGGER IF EXISTS update_processing_queue_updated_at ON processing_queue;
CREATE TRIGGER update_processing_queue_updated_at
    BEFORE UPDATE ON processing_queue
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
EOF
echo -e "   ${GREEN}✓${NC} Created processing_queue table"

echo
echo "3. Creating indexes..."

# Create indexes
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME << 'EOF' > /dev/null 2>&1
-- Audio files indexes
CREATE INDEX IF NOT EXISTS idx_audio_files_date ON audio_files(year, month, date);
CREATE INDEX IF NOT EXISTS idx_audio_files_classification ON audio_files(classification);
CREATE INDEX IF NOT EXISTS idx_audio_files_filename ON audio_files(filename);

-- Transcripts indexes
CREATE INDEX IF NOT EXISTS idx_transcripts_audio_file ON transcripts(audio_file_id);
CREATE INDEX IF NOT EXISTS idx_transcripts_text_search 
    ON transcripts USING gin(to_tsvector('english', transcript_text));

-- Metadata indexes
CREATE INDEX IF NOT EXISTS idx_metadata_meta_id ON audio_metadata(meta_id);
CREATE INDEX IF NOT EXISTS idx_metadata_author_id ON audio_metadata(author_id);
CREATE INDEX IF NOT EXISTS idx_metadata_music_id ON audio_metadata(music_id);
CREATE INDEX IF NOT EXISTS idx_metadata_poi_id ON audio_metadata(poi_id);
CREATE INDEX IF NOT EXISTS idx_metadata_createtime ON audio_metadata(meta_createtime);
CREATE INDEX IF NOT EXISTS idx_metadata_stats ON audio_metadata(stats_playcount, stats_diggcount);
CREATE INDEX IF NOT EXISTS idx_metadata_country ON audio_metadata(country);
CREATE INDEX IF NOT EXISTS idx_metadata_author_uniqueid ON audio_metadata(author_uniqueid);

-- JSONB indexes
CREATE INDEX IF NOT EXISTS idx_metadata_hashtags ON audio_metadata USING gin(text_extra_hashtag_mention);
CREATE INDEX IF NOT EXISTS idx_metadata_mentions ON audio_metadata USING gin(text_extra_user_mention);

-- Processing queue indexes
CREATE INDEX IF NOT EXISTS idx_queue_status ON processing_queue(status);
CREATE INDEX IF NOT EXISTS idx_queue_date ON processing_queue(year, month, date);
EOF
echo -e "   ${GREEN}✓${NC} Created indexes"

echo
echo "4. Creating views..."

# Create view
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME << 'EOF' > /dev/null 2>&1
CREATE OR REPLACE VIEW audio_with_transcripts AS
SELECT
    a.id,
    a.filename,
    a.file_path,
    a.file_size_bytes,
    a.month,
    a.date,
    a.year,
    a.classification,
    a.classification_probability,
    t.transcript_text,
    t.word_count,
    t.duration_seconds,
    m.meta_id,
    m.author_nickname,
    m.author_uniqueid,
    m.music_title,
    m.stats_playcount,
    m.stats_diggcount,
    m.stats_commentcount,
    m.video_duration,
    m.country,
    m.poi_name,
    m.poi_city,
    m.meta_desc,
    a.created_at,
    a.updated_at
FROM audio_files a
LEFT JOIN transcripts t ON a.id = t.audio_file_id
LEFT JOIN audio_metadata m ON a.id = m.audio_file_id;
EOF
echo -e "   ${GREEN}✓${NC} Created audio_with_transcripts view"

# Store schema version
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME << EOF > /dev/null 2>&1
CREATE TABLE IF NOT EXISTS schema_version (
    version VARCHAR(20) PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO schema_version (version) VALUES ('$SCHEMA_VERSION')
ON CONFLICT (version) DO NOTHING;
EOF
echo -e "   ${GREEN}✓${NC} Schema version recorded"

# Final summary
echo
echo -e "${GREEN}=== Schema Management Complete ===${NC}"
echo
echo "Schema Status:"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME -t -c "
SELECT 'Tables: ' || COUNT(*) FROM information_schema.tables 
WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME -t -c "
SELECT 'Views: ' || COUNT(*) FROM information_schema.views 
WHERE table_schema = 'public';
"
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME -t -c "
SELECT 'Current Version: ' || MAX(version) FROM schema_version;
"

echo
echo "Available Tables:"
echo "  - audio_files: Core audio file information"
echo "  - transcripts: Audio transcriptions"
echo "  - audio_metadata: Extended metadata (90+ fields)"
echo "  - processing_queue: Job processing status tracking"
echo "  - audio_with_transcripts: Combined view"
echo
echo "To modify the schema:"
echo "  1. Edit this script with new changes"
echo "  2. Update SCHEMA_VERSION at the top"
echo "  3. Run: ./manage_schema.sh"
echo
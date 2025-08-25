#!/bin/bash
# manage_schema.sh - Create and manage audio pipeline database schema

# Configuration
DB_NAME="audio_pipeline"
DB_USER="audio_user"
DB_PASSWORD="audio_password"  # Should match setup script or be updated

# Schema version for future migrations
SCHEMA_VERSION="3.1"

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
DROP VIEW IF EXISTS audio_with_metadata CASCADE;
DROP VIEW IF EXISTS audio_with_transcripts CASCADE;
DROP TABLE IF EXISTS comments CASCADE;
DROP TABLE IF EXISTS subtitles CASCADE;
DROP TABLE IF EXISTS processing_queue CASCADE;
DROP TABLE IF EXISTS audio_metadata CASCADE;
DROP TABLE IF EXISTS transcripts CASCADE;
DROP TABLE IF EXISTS audio_files CASCADE;
DROP TABLE IF EXISTS schema_version CASCADE;
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
    meta_id TEXT NOT NULL,
    filename TEXT NOT NULL UNIQUE,
    file_path VARCHAR(500),
    file_size_bytes NUMERIC,
    month INTEGER NOT NULL CHECK (month >= 1 AND month <= 12),
    date INTEGER NOT NULL CHECK (date >= 1 AND date <= 31),
    year INTEGER NOT NULL CHECK (year >= 2000),
    location TEXT,
    classification TEXT,
    classification_probability DECIMAL(3,2) CHECK (classification_probability >= 0 AND classification_probability <= 1),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(meta_id, year, month, date)
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

# Create audio_metadata table (no foreign key constraint, uses business key)
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME << 'EOF' > /dev/null 2>&1
CREATE TABLE IF NOT EXISTS audio_metadata (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    audio_file_id UUID,

    -- Core metadata (business key)
    meta_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL, 
    date INTEGER NOT NULL,
    
    -- Other metadata fields
    poi_id TEXT,
    meta_createtime NUMERIC,
    meta_scheduletime NUMERIC,
    timestamp NUMERIC,
    collection_timestamp NUMERIC,

    -- Content status and settings
    meta_itemcommentstatus INTEGER,
    meta_diversificationid TEXT,
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
    author_id TEXT,
    author_uniqueid TEXT,
    author_nickname TEXT,
    author_signature TEXT,
    author_roomid TEXT,
    author_verified BOOLEAN DEFAULT FALSE,
    author_openfavorite BOOLEAN DEFAULT TRUE,
    author_commentsetting INTEGER,
    author_duetsetting INTEGER,
    author_stitchsetting INTEGER,
    author_downloadsetting INTEGER,
    author_createtime NUMERIC,
    
    -- Author statistics
    authorstats_followercount NUMERIC DEFAULT 0,
    authorstats_followingcount NUMERIC DEFAULT 0,
    authorstats_heart NUMERIC DEFAULT 0,
    authorstats_heartcount NUMERIC DEFAULT 0,
    authorstats_videocount NUMERIC DEFAULT 0,
    authorstats_diggcount NUMERIC DEFAULT 0,
    authorstats_friendcount NUMERIC DEFAULT 0,

    -- Music information
    music_id TEXT,
    music_title TEXT,
    music_authorname TEXT,
    music_album TEXT,
    music_duration INTEGER,
    music_schedulesearchtime NUMERIC,
    music_collected BOOLEAN DEFAULT FALSE,

    -- Statistics
    stats_diggcount NUMERIC DEFAULT 0,
    stats_sharecount NUMERIC DEFAULT 0,
    stats_commentcount NUMERIC DEFAULT 0,
    stats_playcount NUMERIC DEFAULT 0,
    stats_collectcount NUMERIC DEFAULT 0,

    -- Video specifications
    video_height INTEGER,
    video_width INTEGER,
    video_duration INTEGER,
    video_bitrate INTEGER,
    video_ratio TEXT,
    video_encodedtype TEXT,
    video_format TEXT,
    video_videoquality TEXT,
    video_codectype TEXT,
    video_definition TEXT,

    -- Location/POI information
    poi_type TEXT,
    poi_name TEXT,
    poi_address TEXT,
    poi_city TEXT,
    poi_citycode TEXT,
    poi_province TEXT,
    poi_country TEXT,
    poi_countrycode TEXT,
    poi_fatherpoiid TEXT,
    poi_fatherpoiname TEXT,
    poi_category TEXT,
    poi_tttypecode TEXT,
    poi_typecode TEXT,
    poi_tttypenametiny TEXT,
    poi_tttypenamemedium TEXT,
    poi_tttypenamesuper TEXT,

    -- Address information
    adress_addresscountry TEXT,
    adress_addresslocality TEXT,
    adress_addressregion TEXT,
    adress_streetaddress TEXT,

    -- Status and messages
    statuscode INTEGER,
    statusmsg TEXT,

    -- Descriptions and text content
    meta_desc TEXT,
    meta_locationcreated TEXT,
    processed_desc TEXT,
    description_hash TEXT,

    -- AI/Classification
    meta_aigclabeltype TEXT,
    meta_aigcdescription TEXT,

    -- Arrays stored as TEXT for flexibility
    meta_diversificationlabels TEXT,
    meta_serverabversions TEXT,
    meta_suggestedwords TEXT,
    meta_adlabelversion TEXT,
    meta_bainfo TEXT,
    subtitle_subtitle_lang TEXT,
    bitrate_bitrate_info TEXT,
    text_extra_user_mention TEXT,
    text_extra_hashtag_mention TEXT,
    warning_warning TEXT,

    -- Duet information
    duetinfo_duetfromid TEXT,

    -- Processing metadata
    pol TEXT,
    hour INTEGER,
    country TEXT,
    raw TEXT,
    meta_textlanguage TEXT,
    meta_categorytype INTEGER,

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Business key unique constraint
    UNIQUE(meta_id, year, month, date)
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
    location TEXT,
    status TEXT DEFAULT 'pending',
    transfer_start TIMESTAMP,
    transfer_end TIMESTAMP,
    transfer_task_id TEXT,
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

# Create comments table
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME << 'EOF' > /dev/null 2>&1
CREATE TABLE IF NOT EXISTS comments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    meta_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    date INTEGER NOT NULL,
    
    -- Core comment information
    cid TEXT NOT NULL,
    aweme_id TEXT,
    comment_text TEXT,
    create_time NUMERIC,
    
    -- Comment engagement stats
    digg_count NUMERIC DEFAULT 0,
    reply_count NUMERIC DEFAULT 0,
    reply_comment_total INTEGER DEFAULT 0,
    
    -- Comment metadata
    comment_language TEXT,
    status INTEGER,
    stick_position INTEGER,
    is_comment_translatable BOOLEAN DEFAULT FALSE,
    no_show BOOLEAN DEFAULT FALSE,
    
    -- User engagement flags
    user_digged BOOLEAN DEFAULT FALSE,
    user_buried BOOLEAN DEFAULT FALSE,
    is_author_digged BOOLEAN DEFAULT FALSE,
    author_pin BOOLEAN DEFAULT FALSE,
    
    -- Reply information
    reply_id TEXT,
    reply_to_reply_id TEXT,
    reply_comment TEXT,
    reply_score DECIMAL(5,2),
    show_more_score DECIMAL(5,2),
    
    -- Author information
    uid TEXT,
    sec_uid TEXT,
    nickname TEXT,
    unique_id TEXT,
    
    -- Author verification and profile
    custom_verify TEXT,
    enterprise_verify_reason TEXT,
    
    -- TEXT fields for complex data
    account_labels TEXT,
    label_list TEXT,
    sort_tags TEXT,
    comment_post_item_ids TEXT,
    collect_stat TEXT,
    ad_cover_url TEXT,
    advance_feature_item_order TEXT,
    advanced_feature_info TEXT,
    bold_fields TEXT,
    can_message_follow_status_list TEXT,
    can_set_geofencing TEXT,
    cha_list TEXT,
    cover_url TEXT,
    events TEXT,
    followers_detail TEXT,
    geofencing TEXT,
    homepage_bottom_toast TEXT,
    item_list TEXT,
    mutual_relation_avatars TEXT,
    need_points TEXT,
    platform_sync_info TEXT,
    relative_users TEXT,
    search_highlight TEXT,
    shield_edit_field_info TEXT,
    type_label TEXT,
    user_profile_guide TEXT,
    user_tags TEXT,
    white_cover_url TEXT,
    
    -- Processing metadata
    collection_timestamp NUMERIC,
    hash_unique_id TEXT,
    total INTEGER,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Business key unique constraint for each comment
    UNIQUE(cid, meta_id, year, month, date)
);

-- Add trigger if not exists
DROP TRIGGER IF EXISTS update_comments_updated_at ON comments;
CREATE TRIGGER update_comments_updated_at
    BEFORE UPDATE ON comments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
EOF
echo -e "   ${GREEN}✓${NC} Created comments table"

# Create subtitles table
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME << 'EOF' > /dev/null 2>&1
CREATE TABLE IF NOT EXISTS subtitles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    meta_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    date INTEGER NOT NULL,
    
    -- Core subtitle information
    content TEXT,
    lang TEXT,
    type TEXT,
    
    -- Subtitle metadata
    rest TEXT,
    
    -- Processing metadata
    collection_timestamp NUMERIC,
    hash_unique_id TEXT,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add trigger if not exists
DROP TRIGGER IF EXISTS update_subtitles_updated_at ON subtitles;
CREATE TRIGGER update_subtitles_updated_at
    BEFORE UPDATE ON subtitles
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
EOF
echo -e "   ${GREEN}✓${NC} Created subtitles table"

echo
echo "3. Creating indexes..."

# Create indexes
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME << 'EOF' > /dev/null 2>&1
-- Audio files indexes
CREATE INDEX IF NOT EXISTS idx_audio_files_business_key ON audio_files(meta_id, year, month, date);
CREATE INDEX IF NOT EXISTS idx_audio_files_meta_id ON audio_files(meta_id);
CREATE INDEX IF NOT EXISTS idx_audio_files_date ON audio_files(year, month, date);
CREATE INDEX IF NOT EXISTS idx_audio_files_classification ON audio_files(classification);
CREATE INDEX IF NOT EXISTS idx_audio_files_filename ON audio_files(filename);

-- Transcripts indexes
CREATE INDEX IF NOT EXISTS idx_transcripts_audio_file ON transcripts(audio_file_id);
CREATE INDEX IF NOT EXISTS idx_transcripts_text_search 
    ON transcripts USING gin(to_tsvector('english', transcript_text));

-- Metadata indexes (business key focused)
CREATE INDEX IF NOT EXISTS idx_metadata_business_key ON audio_metadata(meta_id, year, month, date);
CREATE INDEX IF NOT EXISTS idx_metadata_meta_id ON audio_metadata(meta_id);
CREATE INDEX IF NOT EXISTS idx_metadata_author_id ON audio_metadata(author_id);
CREATE INDEX IF NOT EXISTS idx_metadata_music_id ON audio_metadata(music_id);
CREATE INDEX IF NOT EXISTS idx_metadata_poi_id ON audio_metadata(poi_id);
CREATE INDEX IF NOT EXISTS idx_metadata_createtime ON audio_metadata(meta_createtime);
CREATE INDEX IF NOT EXISTS idx_metadata_stats ON audio_metadata(stats_playcount, stats_diggcount);
CREATE INDEX IF NOT EXISTS idx_metadata_country ON audio_metadata(country);
CREATE INDEX IF NOT EXISTS idx_metadata_author_uniqueid ON audio_metadata(author_uniqueid);

-- TEXT indexes
CREATE INDEX IF NOT EXISTS idx_metadata_hashtags ON audio_metadata USING gin(to_tsvector('english', text_extra_hashtag_mention));
CREATE INDEX IF NOT EXISTS idx_metadata_mentions ON audio_metadata USING gin(to_tsvector('english', text_extra_user_mention));

-- Processing queue indexes
CREATE INDEX IF NOT EXISTS idx_queue_status ON processing_queue(status);
CREATE INDEX IF NOT EXISTS idx_queue_date ON processing_queue(year, month, date);

-- Comments indexes (business key focused)
CREATE INDEX IF NOT EXISTS idx_comments_business_key ON comments(meta_id, year, month, date);
CREATE INDEX IF NOT EXISTS idx_comments_meta_id ON comments(meta_id);
CREATE INDEX IF NOT EXISTS idx_comments_cid ON comments(cid);
CREATE INDEX IF NOT EXISTS idx_comments_aweme_id ON comments(aweme_id);
CREATE INDEX IF NOT EXISTS idx_comments_create_time ON comments(create_time);
CREATE INDEX IF NOT EXISTS idx_comments_digg_count ON comments(digg_count);
CREATE INDEX IF NOT EXISTS idx_comments_text_search ON comments USING gin(to_tsvector('english', comment_text));
CREATE INDEX IF NOT EXISTS idx_comments_date ON comments(year, month, date);

-- Subtitles indexes (business key focused)
CREATE INDEX IF NOT EXISTS idx_subtitles_business_key ON subtitles(meta_id, year, month, date);
CREATE INDEX IF NOT EXISTS idx_subtitles_meta_id ON subtitles(meta_id);
CREATE INDEX IF NOT EXISTS idx_subtitles_lang ON subtitles(lang);
CREATE INDEX IF NOT EXISTS idx_subtitles_type ON subtitles(type);
CREATE INDEX IF NOT EXISTS idx_subtitles_content_search ON subtitles USING gin(to_tsvector('english', content));
CREATE INDEX IF NOT EXISTS idx_subtitles_date ON subtitles(year, month, date);
EOF
echo -e "   ${GREEN}✓${NC} Created indexes"

echo
echo "4. Creating views..."

# Create view that uses business key joins
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME << 'EOF' > /dev/null 2>&1
CREATE OR REPLACE VIEW audio_with_metadata AS
SELECT
    m.meta_id,
    m.year,
    m.month,
    m.date,
    -- Audio file info (NULL if not processed yet)
    a.id as audio_file_id,
    a.filename,
    a.file_path,
    a.file_size_bytes,
    a.location,
    a.classification,
    a.classification_probability,
    -- Transcript info (NULL if not processed yet)
    t.transcript_text,
    t.word_count,
    t.duration_seconds,
    -- Metadata from parquet
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
    -- Counts from related tables
    COALESCE(c.comment_count, 0) as comment_count,
    COALESCE(s.subtitle_count, 0) as subtitle_count,
    -- Status flags
    CASE WHEN a.id IS NOT NULL THEN true ELSE false END as audio_file_processed,
    CASE WHEN t.id IS NOT NULL THEN true ELSE false END as transcript_processed,
    -- Timestamps
    m.created_at as metadata_created_at,
    a.created_at as audio_file_created_at
FROM audio_metadata m
LEFT JOIN audio_files a ON (
    m.meta_id = a.meta_id 
    AND m.year = a.year 
    AND m.month = a.month 
    AND m.date = a.date
)
LEFT JOIN transcripts t ON (
    a.id = t.audio_file_id
)
LEFT JOIN (
    SELECT meta_id, year, month, date, COUNT(*) as comment_count
    FROM comments 
    GROUP BY meta_id, year, month, date
) c ON (
    m.meta_id = c.meta_id 
    AND m.year = c.year 
    AND m.month = c.month 
    AND m.date = c.date
)
LEFT JOIN (
    SELECT meta_id, year, month, date, COUNT(*) as subtitle_count
    FROM subtitles 
    GROUP BY meta_id, year, month, date
) s ON (
    m.meta_id = s.meta_id 
    AND m.year = s.year 
    AND m.month = s.month 
    AND m.date = s.date
);
EOF
echo -e "   ${GREEN}✓${NC} Created audio_with_metadata view"

# Store schema version
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME << EOF > /dev/null 2>&1
CREATE TABLE IF NOT EXISTS schema_version (
    version TEXT PRIMARY KEY,
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
echo "  - audio_files: Core audio file information (linked by meta_id + date)"
echo "  - transcripts: Audio transcriptions (linked to audio_files)"
echo "  - audio_metadata: Extended metadata (independent, linked by meta_id + date)"
echo "  - comments: TikTok comments data (independent, linked by meta_id + date)"
echo "  - subtitles: TikTok subtitle/caption data (independent, linked by meta_id + date)"
echo "  - processing_queue: Job processing status tracking"
echo "  - audio_with_metadata: Combined view using business key joins"
echo
echo "Business Key: (meta_id, year, month, date) uniquely identifies related records"
echo
echo "To modify the schema:"
echo "  1. Edit this script with new changes"
echo "  2. Update SCHEMA_VERSION at the top"
echo "  3. Run: ./manage_schema.sh"
echo
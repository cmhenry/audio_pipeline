#!/bin/bash
# show_schema.sh - Display complete database schema with tables, types, and relationships

# Configuration
DB_NAME="audio_pipeline"
DB_USER="audio_user"
DB_PASSWORD="audio_password"

# Color codes
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

echo -e "${BLUE}=== Audio Pipeline Database Schema ===${NC}"
echo -e "Database: ${GREEN}$DB_NAME${NC}"
echo -e "Generated: $(date)"
echo

# Function to run SQL queries
run_query() {
    PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME -t -c "$1" 2>/dev/null
}

# Function to run SQL queries with formatting
run_query_formatted() {
    PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME -c "$1" 2>/dev/null
}

# 1. Schema Version
echo -e "${CYAN}1. SCHEMA VERSION${NC}"
echo "────────────────────────────────────────"
VERSION=$(run_query "SELECT version FROM schema_version ORDER BY applied_at DESC LIMIT 1;" | xargs)
if [ -n "$VERSION" ]; then
    echo -e "Current Version: ${GREEN}$VERSION${NC}"
else
    echo "No version information found"
fi
echo

# 2. Tables Overview
echo -e "${CYAN}2. TABLES OVERVIEW${NC}"
echo "────────────────────────────────────────"
run_query_formatted "
SELECT 
    schemaname as schema,
    tablename as table_name,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname = 'public' 
ORDER BY tablename;"
echo

# 3. Detailed Table Structures
echo -e "${CYAN}3. TABLE STRUCTURES${NC}"
echo "────────────────────────────────────────"

# Get all tables
TABLES=$(run_query "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;")

for table in $TABLES; do
    if [ "$table" != "schema_version" ]; then
        echo -e "\n${GREEN}TABLE: $table${NC}"
        echo "----------------------------------------"
        
        # Get column details
        run_query_formatted "
        SELECT 
            column_name as \"Column\",
            data_type as \"Type\",
            character_maximum_length as \"Max Length\",
            is_nullable as \"Nullable\",
            column_default as \"Default\"
        FROM information_schema.columns
        WHERE table_schema = 'public' 
        AND table_name = '$table'
        ORDER BY ordinal_position;"
        
        # Get constraints
        echo -e "\n  ${YELLOW}Constraints:${NC}"
        CONSTRAINTS=$(run_query "
        SELECT 
            con.conname,
            con.contype,
            pg_get_constraintdef(con.oid)
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
        WHERE nsp.nspname = 'public' 
        AND rel.relname = '$table';")
        
        if [ -n "$CONSTRAINTS" ]; then
            echo "$CONSTRAINTS" | while IFS='|' read -r name type def; do
                name=$(echo $name | xargs)
                type=$(echo $type | xargs)
                def=$(echo $def | xargs)
                case $type in
                    'p') echo "    • PRIMARY KEY: $name - $def" ;;
                    'u') echo "    • UNIQUE: $name - $def" ;;
                    'f') echo "    • FOREIGN KEY: $name - $def" ;;
                    'c') echo "    • CHECK: $name - $def" ;;
                esac
            done
        else
            echo "    • None"
        fi
        
        # Get indexes
        echo -e "\n  ${YELLOW}Indexes:${NC}"
        INDEXES=$(run_query "
        SELECT indexname, indexdef 
        FROM pg_indexes 
        WHERE schemaname = 'public' 
        AND tablename = '$table'
        AND indexname NOT LIKE '%_pkey';")
        
        if [ -n "$INDEXES" ]; then
            echo "$INDEXES" | while IFS='|' read -r idx_name idx_def; do
                idx_name=$(echo $idx_name | xargs)
                echo "    • $idx_name"
            done
        else
            echo "    • None (besides primary key)"
        fi
    fi
done
echo

# 4. Foreign Key Relationships
echo -e "\n${CYAN}4. FOREIGN KEY RELATIONSHIPS${NC}"
echo "────────────────────────────────────────"
FK_RELATIONS=$(run_query "
SELECT 
    tc.table_name || '.' || kcu.column_name || ' → ' || 
    ccu.table_name || '.' || ccu.column_name as relationship,
    rc.delete_rule
FROM information_schema.table_constraints AS tc 
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
    AND ccu.table_schema = tc.table_schema
JOIN information_schema.referential_constraints AS rc
    ON rc.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY' 
AND tc.table_schema = 'public'
ORDER BY tc.table_name;")

if [ -n "$FK_RELATIONS" ]; then
    echo "$FK_RELATIONS" | while IFS='|' read -r relation rule; do
        relation=$(echo $relation | xargs)
        rule=$(echo $rule | xargs)
        echo -e "  ${GREEN}$relation${NC} (ON DELETE $rule)"
    done
else
    echo "  No foreign key relationships found"
fi
echo

# 5. Views
echo -e "${CYAN}5. VIEWS${NC}"
echo "────────────────────────────────────────"
VIEWS=$(run_query "SELECT viewname FROM pg_views WHERE schemaname = 'public' ORDER BY viewname;")

if [ -n "$VIEWS" ]; then
    for view in $VIEWS; do
        echo -e "\n${GREEN}VIEW: $view${NC}"
        echo "----------------------------------------"
        
        # Get view columns
        run_query_formatted "
        SELECT 
            column_name as \"Column\",
            data_type as \"Type\"
        FROM information_schema.columns
        WHERE table_schema = 'public' 
        AND table_name = '$view'
        ORDER BY ordinal_position;"
    done
else
    echo "  No views found"
fi
echo

# 6. Functions and Triggers
echo -e "\n${CYAN}6. FUNCTIONS${NC}"
echo "────────────────────────────────────────"
FUNCTIONS=$(run_query "
SELECT proname 
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname = 'public'
ORDER BY proname;")

if [ -n "$FUNCTIONS" ]; then
    echo "$FUNCTIONS" | while read -r func; do
        func=$(echo $func | xargs)
        echo -e "  • ${GREEN}$func()${NC}"
    done
else
    echo "  No custom functions found"
fi
echo

echo -e "${CYAN}7. TRIGGERS${NC}"
echo "────────────────────────────────────────"
TRIGGERS=$(run_query "
SELECT 
    trigger_name || ' ON ' || event_object_table || 
    ' (' || event_manipulation || ')' as trigger_info
FROM information_schema.triggers
WHERE trigger_schema = 'public'
ORDER BY event_object_table, trigger_name;")

if [ -n "$TRIGGERS" ]; then
    echo "$TRIGGERS" | while read -r trigger; do
        trigger=$(echo $trigger | xargs)
        echo -e "  • ${GREEN}$trigger${NC}"
    done
else
    echo "  No triggers found"
fi
echo

# 7. Data Types Summary
echo -e "\n${CYAN}8. DATA TYPES USED${NC}"
echo "────────────────────────────────────────"
run_query_formatted "
SELECT 
    data_type,
    COUNT(*) as usage_count
FROM information_schema.columns
WHERE table_schema = 'public'
GROUP BY data_type
ORDER BY usage_count DESC;"

# 8. Table Row Counts
echo -e "\n${CYAN}9. TABLE ROW COUNTS${NC}"
echo "────────────────────────────────────────"
for table in $TABLES; do
    if [ "$table" != "schema_version" ]; then
        COUNT=$(run_query "SELECT COUNT(*) FROM $table;" | xargs)
        printf "  %-30s %10s rows\n" "$table:" "$COUNT"
    fi
done
echo

# 9. Database Size
echo -e "${CYAN}10. DATABASE SIZE${NC}"
echo "────────────────────────────────────────"
run_query_formatted "
SELECT 
    pg_database.datname as \"Database\",
    pg_size_pretty(pg_database_size(pg_database.datname)) as \"Size\"
FROM pg_database
WHERE datname = '$DB_NAME';"

echo
echo -e "${MAGENTA}=== End of Schema Report ===${NC}"

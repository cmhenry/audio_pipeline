#!/bin/bash
# setup_postgresql.sh - Install and configure PostgreSQL for audio pipeline

# Configuration
DB_NAME="audio_pipeline"
DB_USER="audio_user"
DB_PASSWORD="audio_password"  # Change this in production!

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== PostgreSQL Installation and Setup ===${NC}"
echo

# Function to check if command succeeded
check_status() {
    if [ $? -eq 0 ]; then
        echo -e "   ${GREEN}✓${NC} $1"
    else
        echo -e "   ${RED}✗${NC} $1"
        exit 1
    fi
}

# 1. Check if PostgreSQL is already installed
echo "1. Checking PostgreSQL installation..."
if command -v psql &> /dev/null; then
    echo -e "   ${YELLOW}⚠${NC}  PostgreSQL is already installed"
    read -p "   Do you want to continue with setup? (y/n): " continue_setup
    if [[ ! "$continue_setup" =~ ^[Yy]$ ]]; then
        echo "   Setup cancelled."
        exit 0
    fi
else
    echo "   Installing PostgreSQL..."
    sudo apt update
    sudo apt install -y postgresql postgresql-contrib
    check_status "PostgreSQL installed"
fi

# 2. Start and enable PostgreSQL
echo
echo "2. Starting PostgreSQL service..."
sudo systemctl start postgresql
check_status "PostgreSQL started"
sudo systemctl enable postgresql
check_status "PostgreSQL enabled for auto-start"

# Wait for PostgreSQL to be ready
echo "   Waiting for PostgreSQL to be ready..."
sleep 5

# 3. Verify PostgreSQL is running
if sudo -u postgres psql -c '\l' > /dev/null 2>&1; then
    echo -e "   ${GREEN}✓${NC} PostgreSQL is running!"
else
    echo -e "   ${RED}✗${NC} PostgreSQL failed to start. Check: sudo systemctl status postgresql"
    exit 1
fi

# 4. Check if database already exists
echo
echo "3. Checking existing database..."
if sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    echo -e "   ${YELLOW}⚠${NC}  Database '$DB_NAME' already exists"
    read -p "   Drop and recreate? (y/n): " drop_db
    if [[ "$drop_db" =~ ^[Yy]$ ]]; then
        sudo -u postgres psql -c "DROP DATABASE IF EXISTS $DB_NAME;"
        sudo -u postgres psql -c "DROP USER IF EXISTS $DB_USER;"
        check_status "Existing database and user dropped"
    else
        echo "   Keeping existing database. Run schema management script to update schema."
        exit 0
    fi
fi

# 5. Create database and user
echo
echo "4. Creating database and user..."
sudo -u postgres psql << EOF
CREATE USER $DB_USER WITH PASSWORD '$DB_PASSWORD';
CREATE DATABASE $DB_NAME OWNER $DB_USER;
\c $DB_NAME
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
GRANT ALL ON SCHEMA public TO $DB_USER;
EOF
check_status "Database and user created"

# 6. Configure authentication
echo
echo "5. Configuring PostgreSQL authentication..."
PG_VERSION=$(ls /etc/postgresql/ | grep -E '^[0-9]+' | sort -V | tail -1)
PG_CONFIG="/etc/postgresql/$PG_VERSION/main/pg_hba.conf"

if [ -f "$PG_CONFIG" ]; then
    # Backup original config
    sudo cp "$PG_CONFIG" "$PG_CONFIG.backup.$(date +%Y%m%d_%H%M%S)"
    
    # Update authentication method
    sudo sed -i 's/local   all             all                                     peer/local   all             all                                     md5/' "$PG_CONFIG"
    sudo systemctl reload postgresql
    check_status "Authentication configured"
else
    echo -e "   ${YELLOW}⚠${NC}  Could not find pg_hba.conf at $PG_CONFIG"
fi

# 7. Test connection
echo
echo "6. Testing database connection..."
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME -c "SELECT 'Connection successful!' as status;" 2>/dev/null
if [ $? -eq 0 ]; then
    echo -e "   ${GREEN}✓${NC} Database connection successful!"
else
    echo -e "   ${RED}✗${NC} Connection test failed"
    echo "   Trying alternative authentication..."
    sudo -u postgres psql -d $DB_NAME -c "ALTER USER $DB_USER WITH PASSWORD '$DB_PASSWORD';"
    PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME -c "SELECT 'Connection successful!' as status;"
    check_status "Alternative connection method"
fi

# 8. Create backup directory
echo
echo "7. Setting up backup directory..."
BACKUP_DIR="/mnt/storage/postgresql/backups"
if [ -d "/mnt/storage" ]; then
    sudo mkdir -p "$BACKUP_DIR"
    sudo chown $USER:$USER "$BACKUP_DIR"
    check_status "Backup directory created at $BACKUP_DIR"
else
    BACKUP_DIR="$HOME/postgresql_backups"
    mkdir -p "$BACKUP_DIR"
    echo -e "   ${YELLOW}⚠${NC}  /mnt/storage not found, using $BACKUP_DIR"
fi

# 9. Create connection helper script
echo
echo "8. Creating connection helper..."
cat > ~/connect_audio_db.sh << EOF
#!/bin/bash
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME
EOF
chmod +x ~/connect_audio_db.sh
check_status "Connection helper created at ~/connect_audio_db.sh"

echo
echo -e "${GREEN}=== PostgreSQL Setup Complete! ===${NC}"
echo
echo "Database Details:"
echo "  Database: $DB_NAME"
echo "  User: $DB_USER"
echo "  Password: $DB_PASSWORD"
echo "  Backup Directory: $BACKUP_DIR"
echo
echo "Next Steps:"
echo "  1. Run ./manage_schema.sh to create/update database schema"
echo "  2. Change the default password with:"
echo "     sudo -u postgres psql -c \"ALTER USER $DB_USER PASSWORD 'new_secure_password';\""
echo
echo "Quick Connect:"
echo "  ~/connect_audio_db.sh"
echo "  OR"
echo "  PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME"
echo
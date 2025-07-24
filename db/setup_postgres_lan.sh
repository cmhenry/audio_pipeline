#!/bin/bash
# enable_postgresql_lan_access.sh - Configure PostgreSQL for LAN access

# Configuration
DB_NAME="audio_pipeline"
DB_USER="audio_user"

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=== PostgreSQL LAN Access Configuration ===${NC}"
echo

# Get PostgreSQL version
PG_VERSION=$(ls /etc/postgresql/ | grep -E '^[0-9]+' | sort -V | tail -1)
if [ -z "$PG_VERSION" ]; then
    echo -e "${RED}Error: PostgreSQL not found!${NC}"
    exit 1
fi

PG_CONFIG_DIR="/etc/postgresql/$PG_VERSION/main"
echo -e "PostgreSQL version: ${GREEN}$PG_VERSION${NC}"
echo -e "Config directory: ${GREEN}$PG_CONFIG_DIR${NC}"
echo

# 1. Get current server IP address
echo "1. Detecting network configuration..."
# Get all IP addresses
IP_ADDRESSES=$(hostname -I | tr ' ' '\n' | grep -E '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$')
echo "   Available IP addresses:"
echo "$IP_ADDRESSES" | while read ip; do
    echo "     - $ip"
done

# Try to detect main LAN IP (usually not 127.x.x.x or 172.17.x.x for Docker)
MAIN_IP=$(hostname -I | tr ' ' '\n' | grep -E '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$' | grep -v '^127\.' | grep -v '^172\.17\.' | head -1)
if [ -z "$MAIN_IP" ]; then
    MAIN_IP=$(hostname -I | awk '{print $1}')
fi

echo -e "   Primary IP: ${GREEN}$MAIN_IP${NC}"

# Calculate subnet (assuming /24)
SUBNET=$(echo $MAIN_IP | cut -d. -f1-3).0/24
echo -e "   Subnet (assumed /24): ${GREEN}$SUBNET${NC}"
echo

# 2. Backup configuration files
echo "2. Backing up configuration files..."
BACKUP_TIME=$(date +%Y%m%d_%H%M%S)
sudo cp "$PG_CONFIG_DIR/postgresql.conf" "$PG_CONFIG_DIR/postgresql.conf.backup.$BACKUP_TIME"
sudo cp "$PG_CONFIG_DIR/pg_hba.conf" "$PG_CONFIG_DIR/pg_hba.conf.backup.$BACKUP_TIME"
echo -e "   ${GREEN}✓${NC} Backups created"

# 3. Configure PostgreSQL to listen on all interfaces
echo
echo "3. Configuring PostgreSQL to listen on network interfaces..."
# Check current listen_addresses
CURRENT_LISTEN=$(sudo grep "^listen_addresses" "$PG_CONFIG_DIR/postgresql.conf" 2>/dev/null)
if [ -z "$CURRENT_LISTEN" ]; then
    # Add listen_addresses if not present
    sudo sed -i "/^#listen_addresses/a listen_addresses = '*'\t\t\t# Listen on all interfaces" "$PG_CONFIG_DIR/postgresql.conf"
else
    # Update existing listen_addresses
    sudo sed -i "s/^listen_addresses.*/listen_addresses = '*'\t\t\t# Listen on all interfaces/" "$PG_CONFIG_DIR/postgresql.conf"
fi
echo -e "   ${GREEN}✓${NC} Set listen_addresses = '*'"

# 4. Configure pg_hba.conf for LAN access
echo
echo "4. Configuring authentication for LAN access..."
echo
echo "Choose authentication method:"
echo "  1) Allow entire subnet $SUBNET (recommended for trusted LAN)"
echo "  2) Allow specific IP address"
echo "  3) Allow all IPs (less secure)"
echo "  4) Custom subnet"
read -p "Enter choice [1-4]: " auth_choice

case $auth_choice in
    1)
        HBA_RULE="host    all             all             $SUBNET            md5"
        echo -e "   Adding rule for subnet: ${GREEN}$SUBNET${NC}"
        ;;
    2)
        read -p "   Enter IP address to allow: " ALLOWED_IP
        HBA_RULE="host    all             all             $ALLOWED_IP/32         md5"
        echo -e "   Adding rule for IP: ${GREEN}$ALLOWED_IP${NC}"
        ;;
    3)
        HBA_RULE="host    all             all             0.0.0.0/0              md5"
        echo -e "   ${YELLOW}⚠ Warning: Allowing all IPs!${NC}"
        ;;
    4)
        read -p "   Enter custom subnet (e.g., 192.168.1.0/24): " CUSTOM_SUBNET
        HBA_RULE="host    all             all             $CUSTOM_SUBNET         md5"
        echo -e "   Adding rule for subnet: ${GREEN}$CUSTOM_SUBNET${NC}"
        ;;
    *)
        echo -e "${RED}Invalid choice${NC}"
        exit 1
        ;;
esac

# Check if rule already exists
if sudo grep -q "$HBA_RULE" "$PG_CONFIG_DIR/pg_hba.conf" 2>/dev/null; then
    echo -e "   ${YELLOW}Rule already exists${NC}"
else
    # Add the rule before the first 'local' line
    sudo sed -i "/^local/i $HBA_RULE" "$PG_CONFIG_DIR/pg_hba.conf"
    echo -e "   ${GREEN}✓${NC} Authentication rule added"
fi

# 5. Configure firewall
echo
echo "5. Configuring firewall..."
# Check if ufw is installed and active
if command -v ufw &> /dev/null; then
    if sudo ufw status | grep -q "Status: active"; then
        echo "   UFW firewall detected"
        sudo ufw allow 5432/tcp comment "PostgreSQL"
        echo -e "   ${GREEN}✓${NC} Firewall rule added for port 5432"
    else
        echo -e "   ${YELLOW}UFW is installed but not active${NC}"
    fi
else
    # Check for iptables
    if command -v iptables &> /dev/null; then
        echo "   iptables detected"
        sudo iptables -A INPUT -p tcp --dport 5432 -j ACCEPT
        echo -e "   ${GREEN}✓${NC} iptables rule added (temporary - will not persist after reboot)"
        echo -e "   ${YELLOW}Note: To make iptables rules persistent, install iptables-persistent${NC}"
    else
        echo -e "   ${YELLOW}No firewall detected${NC}"
    fi
fi

# 6. Restart PostgreSQL
echo
echo "6. Restarting PostgreSQL..."
sudo systemctl restart postgresql
sleep 3
if sudo systemctl is-active --quiet postgresql; then
    echo -e "   ${GREEN}✓${NC} PostgreSQL restarted successfully"
else
    echo -e "   ${RED}✗ PostgreSQL failed to restart${NC}"
    echo "   Check logs: sudo journalctl -xeu postgresql"
    exit 1
fi

# 7. Test configuration
echo
echo "7. Testing configuration..."
# Check if PostgreSQL is listening on port 5432
if sudo ss -tlnp | grep -q ":5432"; then
    echo -e "   ${GREEN}✓${NC} PostgreSQL is listening on port 5432"
    sudo ss -tlnp | grep ":5432"
else
    echo -e "   ${RED}✗ PostgreSQL is not listening on network interfaces${NC}"
fi

# 8. Generate connection examples
echo
echo -e "${GREEN}=== Configuration Complete! ===${NC}"
echo
echo "Connection examples from other machines:"
echo
echo "1. Using psql:"
echo "   psql -h $MAIN_IP -U $DB_USER -d $DB_NAME"
echo
echo "2. Using connection string:"
echo "   postgresql://$DB_USER:password@$MAIN_IP:5432/$DB_NAME"
echo
echo "3. Python psycopg2:"
echo "   conn = psycopg2.connect("
echo "       host='$MAIN_IP',"
echo "       database='$DB_NAME',"
echo "       user='$DB_USER',"
echo "       password='your_password'"
echo "   )"
echo
echo "4. From this machine (test):"
echo "   PGPASSWORD=your_password psql -h $MAIN_IP -U $DB_USER -d $DB_NAME"
echo
echo -e "${YELLOW}Security Notes:${NC}"
echo "- Ensure you're using strong passwords"
echo "- Consider using SSL/TLS for encrypted connections"
echo "- Limit access to specific IPs when possible"
echo "- Monitor pg_hba.conf for unauthorized changes"
echo
echo "Configuration files:"
echo "- $PG_CONFIG_DIR/postgresql.conf"
echo "- $PG_CONFIG_DIR/pg_hba.conf"
echo
echo "To revert changes, restore from backups:"
echo "- postgresql.conf.backup.$BACKUP_TIME"
echo "- pg_hba.conf.backup.$BACKUP_TIME"
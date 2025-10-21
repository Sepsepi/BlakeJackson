#!/bin/bash
################################################################################
# Cron Job Diagnostic Script
################################################################################
# This script checks for common issues that prevent cron jobs from running
# Run this on the server: bash diagnose_cron_issue.sh
################################################################################

echo "================================================================================"
echo "CRON JOB DIAGNOSTIC SCRIPT"
echo "================================================================================"
echo "Date: $(date)"
echo "Server: $(hostname)"
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "================================================================================"
echo "TEST 1: Checking if cron service is running"
echo "================================================================================"
if systemctl is-active --quiet cron 2>/dev/null || systemctl is-active --quiet crond 2>/dev/null; then
    echo -e "${GREEN}✅ Cron service is running${NC}"
else
    echo -e "${RED}❌ Cron service is NOT running${NC}"
    echo "   Try: sudo systemctl start cron"
fi
echo ""

echo "================================================================================"
echo "TEST 2: Checking crontab configuration"
echo "================================================================================"
if crontab -l > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Crontab exists${NC}"
    echo "Current crontab entries:"
    crontab -l | grep -v '^#' | grep -v '^$'
    echo ""

    # Check for the specific weekly automation job
    if crontab -l | grep -q "run_weekly_automation.sh"; then
        echo -e "${GREEN}✅ Found weekly automation job in crontab${NC}"
    else
        echo -e "${RED}❌ Weekly automation job NOT found in crontab${NC}"
        echo "   Expected: 0 10 * * 0 /root/cron-job/run_weekly_automation.sh"
    fi
else
    echo -e "${RED}❌ No crontab configured${NC}"
    echo "   Run: crontab -e"
fi
echo ""

echo "================================================================================"
echo "TEST 3: Checking script files exist"
echo "================================================================================"
if [ -f "/root/cron-job/run_weekly_automation.sh" ]; then
    echo -e "${GREEN}✅ run_weekly_automation.sh exists${NC}"
    ls -lh /root/cron-job/run_weekly_automation.sh
else
    echo -e "${RED}❌ run_weekly_automation.sh NOT FOUND${NC}"
fi
echo ""

if [ -f "/root/cron-job/weekly_automation.py" ]; then
    echo -e "${GREEN}✅ weekly_automation.py exists${NC}"
    ls -lh /root/cron-job/weekly_automation.py
else
    echo -e "${RED}❌ weekly_automation.py NOT FOUND${NC}"
fi
echo ""

echo "================================================================================"
echo "TEST 4: Checking script permissions"
echo "================================================================================"
if [ -x "/root/cron-job/run_weekly_automation.sh" ]; then
    echo -e "${GREEN}✅ run_weekly_automation.sh is executable${NC}"
else
    echo -e "${RED}❌ run_weekly_automation.sh is NOT executable${NC}"
    echo "   Fix: chmod +x /root/cron-job/run_weekly_automation.sh"
fi
echo ""

echo "================================================================================"
echo "TEST 5: Checking logs directory"
echo "================================================================================"
if [ -d "/root/cron-job/logs" ]; then
    echo -e "${GREEN}✅ logs directory exists${NC}"

    # Show recent log files
    echo "Recent log files:"
    ls -lht /root/cron-job/logs/ 2>/dev/null | head -10

    # Check for recent automation logs
    echo ""
    echo "Recent weekly automation logs:"
    ls -lht /root/cron-job/weekly_automation_*.log 2>/dev/null | head -5
else
    echo -e "${YELLOW}⚠️ logs directory does NOT exist${NC}"
    echo "   Creating: mkdir -p /root/cron-job/logs"
    mkdir -p /root/cron-job/logs
fi
echo ""

echo "================================================================================"
echo "TEST 6: Checking cron logs for recent execution"
echo "================================================================================"
echo "Checking system logs for cron executions..."

# Check for cron executions in the last 14 days
if [ -f "/var/log/syslog" ]; then
    echo "Recent cron executions (from /var/log/syslog):"
    grep "run_weekly_automation" /var/log/syslog 2>/dev/null | tail -20
elif [ -f "/var/log/cron" ]; then
    echo "Recent cron executions (from /var/log/cron):"
    grep "run_weekly_automation" /var/log/cron 2>/dev/null | tail -20
else
    echo -e "${YELLOW}⚠️ Could not find system cron logs${NC}"
fi
echo ""

echo "================================================================================"
echo "TEST 7: Checking most recent weekly_automation.log"
echo "================================================================================"
if [ -f "/root/cron-job/logs/weekly_automation.log" ]; then
    echo "Last 50 lines of /root/cron-job/logs/weekly_automation.log:"
    tail -50 /root/cron-job/logs/weekly_automation.log
else
    echo -e "${YELLOW}⚠️ No weekly_automation.log found${NC}"
fi
echo ""

echo "================================================================================"
echo "TEST 8: Checking environment variables"
echo "================================================================================"
if [ -f "/root/cron-job/.env" ]; then
    echo -e "${GREEN}✅ .env file exists${NC}"
    echo "Environment variables (without values):"
    grep -v '^#' /root/cron-job/.env | grep -v '^$' | cut -d= -f1
else
    echo -e "${RED}❌ .env file NOT FOUND${NC}"
fi
echo ""

echo "================================================================================"
echo "TEST 9: Checking Python and dependencies"
echo "================================================================================"

# Check Python version
if command -v python3 &> /dev/null; then
    echo -e "${GREEN}✅ Python3 installed:${NC} $(python3 --version)"
else
    echo -e "${RED}❌ Python3 NOT found${NC}"
fi

# Check pyenv
if [ -d "$HOME/.pyenv" ]; then
    echo -e "${GREEN}✅ pyenv installed${NC}"
else
    echo -e "${YELLOW}⚠️ pyenv NOT found (may cause issues)${NC}"
fi
echo ""

echo "================================================================================"
echo "TEST 10: Checking disk space"
echo "================================================================================"
df -h /root | tail -1
echo ""

echo "================================================================================"
echo "TEST 11: Testing manual execution"
echo "================================================================================"
echo "To test if the script works manually, run:"
echo "   cd /root/cron-job && bash run_weekly_automation.sh"
echo ""
echo "Or test the Python script directly:"
echo "   cd /root/cron-job && python3 weekly_automation.py"
echo ""

echo "================================================================================"
echo "DIAGNOSTIC SUMMARY"
echo "================================================================================"
echo "If the cron job is not running, check the red ❌ items above."
echo ""
echo "Common issues:"
echo "1. Cron service not running - Fix: sudo systemctl start cron"
echo "2. Crontab not configured - Fix: crontab -e (add the job)"
echo "3. Script not executable - Fix: chmod +x run_weekly_automation.sh"
echo "4. Missing .env file - Fix: Copy .env.template to .env and configure"
echo "5. Pyenv not initialized - Fix: Already handled in run_weekly_automation.sh"
echo ""
echo "To view next scheduled run time:"
echo "   crontab -l | grep run_weekly_automation"
echo ""
echo "To check if cron will run it:"
echo "   Next Sunday at 10:00 AM UTC"
echo ""
echo "================================================================================"

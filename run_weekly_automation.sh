#!/bin/bash
################################################################################
# Broward Lis Pendens Weekly Automation - Linux Cron Job Script
################################################################################
# This script runs the weekly Broward Lis Pendens automation pipeline
# Designed for Linux cron jobs
################################################################################

# Initialize pyenv (required for cron jobs)
export PYENV_ROOT="$HOME/.pyenv"
if [[ -d $PYENV_ROOT/bin ]]; then
    export PATH="$PYENV_ROOT/bin:$PATH"
    eval "$(pyenv init - bash)"
    eval "$(pyenv virtualenv-init -)"
fi

# Change to the script directory
cd /root/cron-job

# Log the start time
echo "============================================================================" >> logs/weekly_automation.log
echo "Weekly Automation Started: $(date)" >> logs/weekly_automation.log
echo "============================================================================" >> logs/weekly_automation.log

# Run the Python automation script
python3 weekly_automation.py >> logs/weekly_automation.log 2>&1

# Capture the exit code
EXIT_CODE=$?

# Log the completion
echo "" >> logs/weekly_automation.log
echo "Weekly Automation Completed: $(date)" >> logs/weekly_automation.log
echo "Exit Code: $EXIT_CODE" >> logs/weekly_automation.log
echo "============================================================================" >> logs/weekly_automation.log
echo "" >> logs/weekly_automation.log

# Exit with the same code as the Python script
exit $EXIT_CODE


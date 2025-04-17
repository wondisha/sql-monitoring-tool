@echo off
cd /d %~dp0
python src/agent_job.py >> monitoring_results/agent.log 2>&1
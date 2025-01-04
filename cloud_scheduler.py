import asyncio
import logging
import sys
import os
from datetime import datetime, timedelta
import time

# Set up logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/scheduler.log')
    ]
)
logger = logging.getLogger(__name__)

# Configuration
SCAN_HOUR = 10  # 2 AM UTC
SCAN_MINUTE = 15

async def run_scanner():
    """Run the trend scanner"""
    try:
        from trend_scanner import main
        logger.info("Starting trend scanner...")
        await main()
        logger.info("Trend scanner completed successfully")
    except Exception as e:
        logger.error(f"Error running trend scanner: {str(e)}", exc_info=True)

async def scheduler():
    """Schedule daily runs"""
    while True:
        now = datetime.utcnow()
        target = now.replace(hour=SCAN_HOUR, minute=SCAN_MINUTE, second=0, microsecond=0)
        
        # If we've passed today's target time, set target to tomorrow
        if now >= target:
            target += timedelta(days=1)
        
        # Calculate wait time
        wait_seconds = (target - now).total_seconds()
        
        logger.info(f"Current time (UTC): {now}")
        logger.info(f"Next scan scheduled for: {target}")
        logger.info(f"Waiting for {wait_seconds/3600:.1f} hours")
        
        # Wait until next scheduled time
        await asyncio.sleep(wait_seconds)
        
        # Run the scanner
        await run_scanner()
        
        # After completion, log next run time
        next_run = target + timedelta(days=1)
        logger.info(f"Scan complete. Next run scheduled for: {next_run}")

if __name__ == "__main__":
    try:
        logger.info("=== Trend Scanner Scheduler Starting ===")
        asyncio.run(scheduler())
    except KeyboardInterrupt:
        logger.info("Scheduler shutdown requested")
    except Exception as e:
        logger.error(f"Fatal scheduler error: {str(e)}", exc_info=True) 
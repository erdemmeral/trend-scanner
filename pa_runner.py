import asyncio
import logging
import sys
from datetime import datetime
from trend_scanner import main

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('scanner.log')
    ]
)
logger = logging.getLogger(__name__)

async def runner():
    """
    Runner with error handling and restart capability
    """
    while True:
        try:
            logger.info(f"Starting scanner at {datetime.now()}")
            await main()
        except Exception as e:
            logger.error(f"Scanner crashed: {str(e)}", exc_info=True)
            logger.info("Restarting in 60 seconds...")
            await asyncio.sleep(60)
        else:
            logger.info("Scanner completed normally, restarting in 60 seconds...")
            await asyncio.sleep(60)

if __name__ == "__main__":
    try:
        logger.info("=== Scanner Runner Starting ===")
        asyncio.run(runner())
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
    finally:
        logger.info("=== Scanner Runner Stopped ===") 
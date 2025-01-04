import logging
import sys
import os
from datetime import datetime, timedelta
import asyncio
import random
import gc
from telegram.ext import Application
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from pytrends.request import TrendReq
import time

# Set up logging first
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/trend_scanner.log')
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_IDS = os.getenv('TELEGRAM_CHAT_IDS', '').split(',') if os.getenv('TELEGRAM_CHAT_IDS') else []
SCAN_INTERVAL_HOURS = int(os.getenv('SCAN_INTERVAL_HOURS', '24'))

# Validate environment variables
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN not found in environment variables")
    sys.exit(1)

if not TELEGRAM_CHAT_IDS:
    logger.error("TELEGRAM_CHAT_IDS not found in environment variables")
    sys.exit(1)

class TrendScanner:
    def __init__(self):
        self.telegram = None
        self.pytrends = TrendReq(hl='en-US', tz=360)
        self.last_scan_time = None
        self.request_count = 0
        self.last_request_time = time.time()
        self.request_limit = 10
        self.cooldown_minutes = 1
        self.consecutive_429s = 0
        self.base_delay = 5
        
        # Initialize categories and search terms
        self.categories = {
            'AI and Machine Learning': {
                'search_terms': ['artificial intelligence', 'machine learning', 'deep learning', 'neural networks'],
                'stocks': {'NVDA': 'NVIDIA', 'AI': 'C3.ai', 'MSFT': 'Microsoft'}
            },
            'Cloud Computing': {
                'search_terms': ['cloud computing', 'cloud services', 'cloud storage', 'cloud security'],
                'stocks': {'AMZN': 'Amazon', 'MSFT': 'Microsoft', 'GOOGL': 'Google'}
            }
            # Add more categories as needed
        }

    async def get_recent_trend_data(self, term):
        """Get recent trend data with improved rate limiting"""
        try:
            logger.info(f"Getting trend data for: {term}")
            
            # First get today's data
            today = datetime.now().date()
            today_str = today.strftime('%Y-%m-%d')
            
            # Add delay before API call
            await asyncio.sleep(random.uniform(5, 10))
            
            # Get today's data first
            self.pytrends.build_payload([term], timeframe=f'{today_str} {today_str}', geo='US')
            today_data = self.pytrends.interest_over_time()
            
            if today_data is None or today_data.empty:
                logger.info(f"No today's data available for: {term}")
                return None
            
            today_value = float(today_data[term].iloc[-1])
            logger.info(f"\nToday's data for {term}:")
            logger.info(f"Today's value: {today_value}")
            
            # If today's value is less than 90, no need to check historical data
            if today_value < 90:
                logger.info(f"Today's value ({today_value}) is below threshold of 90")
                return None
            
            # If today's value is high enough, get 90-day data for comparison
            ninety_day_timeframe = f"{(today - timedelta(days=90)).strftime('%Y-%m-%d')} {today_str}"
            await asyncio.sleep(random.uniform(5, 10))
            
            self.pytrends.build_payload([term], timeframe=ninety_day_timeframe, geo='US')
            historical_data = self.pytrends.interest_over_time()
            
            if historical_data is None or historical_data.empty:
                logger.info(f"No historical data available for: {term}")
                return None
            
            # Log historical data statistics
            historical_max = historical_data[term].max()
            historical_mean = historical_data[term].mean()
            logger.info(f"\nHistorical data for {term}:")
            logger.info(f"90-day maximum: {historical_max}")
            logger.info(f"90-day average: {historical_mean:.2f}")
            logger.info(f"Today vs Historical max: {today_value/historical_max*100:.1f}%")
            
            # Return data only if today's value is significant
            if today_value >= historical_max:
                logger.info(f"Today's value ({today_value}) is higher than historical maximum ({historical_max})")
                return historical_data
            else:
                logger.info(f"Today's value ({today_value}) is not higher than historical maximum ({historical_max})")
                return None
            
        except Exception as e:
            logger.error(f"Error getting trend data: {str(e)}")
            return None

    async def scan_trends_with_notification(self, search_terms, category):
        """Scan trends and send notifications"""
        results = []
        
        for term in search_terms:
            try:
                logger.info(f"\nAnalyzing term: {term}")
                trend_data = await self.get_recent_trend_data(term)
                
                if trend_data is not None:
                    max_value = trend_data[term].max()
                    if max_value >= 90:
                        logger.info(f"Breakout detected for {term}: {max_value}")
                        results.append({
                            'term': term,
                            'value': max_value,
                            'category': category
                        })
                
            except Exception as e:
                logger.error(f"Error analyzing {term}: {str(e)}")
                continue
                
        return results

    async def run_continuous_scan(self):
        """Run a single scan cycle"""
        try:
            logger.info("Starting scan cycle...")
            
            for category, data in self.categories.items():
                logger.info(f"\nScanning category: {category}")
                results = await self.scan_trends_with_notification(
                    data['search_terms'],
                    category
                )
                
                if results:
                    message = f"🚨 Breakout Alert for {category}:\n\n"
                    for r in results:
                        message += f"• {r['term']}: {r['value']:.1f}\n"
                    await self.send_telegram_alert(message)
            
            logger.info("Scan cycle complete")
            
        except Exception as e:
            logger.error(f"Error in scan cycle: {str(e)}")
            raise

    async def start_app(self):
        """Initialize Telegram"""
        try:
            logger.info("Loading Telegram components...")
            self.telegram = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
            await self.telegram.initialize()
            await self.telegram.start()
            logger.info("Telegram application started successfully")
        except Exception as e:
            logger.error(f"Error in start_app: {str(e)}")
            raise

    async def send_telegram_alert(self, message):
        """Send alert to Telegram"""
        try:
            for chat_id in TELEGRAM_CHAT_IDS:
                await self.telegram.bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode='HTML'
                )
                logger.info(f"Alert sent to chat_id: {chat_id}")
        except Exception as e:
            logger.error(f"Error sending Telegram message: {str(e)}")

async def main():
    """Main application entry point"""
    try:
        logger.info("=== Tech Trend Scanner Starting ===")
        logger.info(f"Current time: {datetime.now()}")
        scanner = TrendScanner()
        logger.info("Starting application...")
        await scanner.start_app()
        logger.info("Running scan...")
        await scanner.run_continuous_scan()
    except Exception as e:
        logger.error(f"Critical error: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
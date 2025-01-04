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
            ninety_day_timeframe = f"{(datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')} {datetime.now().strftime('%Y-%m-%d')}"
            
            self.pytrends.build_payload([term], timeframe=ninety_day_timeframe, geo='US')
            trend_data = self.pytrends.interest_over_time()
            
            if trend_data is None or trend_data.empty:
                logger.info(f"No data available for: {term}")
                return None
                
            logger.info(f"Successfully retrieved data for: {term}")
            return trend_data
            
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
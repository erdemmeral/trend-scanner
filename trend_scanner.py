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
                'search_terms': ['artificial intelligence', 'machine learning', 'deep learning', 'neural networks', 'AI models', 'GPT-4', 'large language models', 'computer vision AI'],
                'stocks': {'NVDA': 'NVIDIA', 'AI': 'C3.ai', 'PLTR': 'Palantir', 'BBAI': 'BigBear.ai', 'UPST': 'Upstart', 'SOUN': 'SoundHound', 'PATH': 'UiPath'}
            },
            'Cloud Computing': {
                'search_terms': ['cloud computing', 'cloud services', 'cloud storage', 'cloud security', 'hybrid cloud', 'multi cloud', 'serverless computing'],
                'stocks': {'AMZN': 'Amazon', 'MSFT': 'Microsoft', 'DDOG': 'Datadog', 'NET': 'Cloudflare', 'DOMO': 'Domo', 'NCNO': 'nCino', 'SUMO': 'Sumo Logic'}
            },
            'Cybersecurity': {
                'search_terms': ['cybersecurity', 'zero trust security', 'ransomware protection', 'endpoint security', 'cloud security', 'network security', 'cyber attack'],
                'stocks': {'CRWD': 'CrowdStrike', 'PANW': 'Palo Alto', 'S': 'SentinelOne', 'TENB': 'Tenable', 'RDWR': 'Radware', 'VRNS': 'Varonis', 'CYBR': 'CyberArk'}
            },
            'Quantum Computing': {
                'search_terms': ['quantum computing', 'quantum computer', 'quantum supremacy', 'quantum advantage', 'quantum encryption', 'quantum processor'],
                'stocks': {'IBM': 'IBM', 'IONQ': 'IonQ', 'RGTI': 'Rigetti', 'ARQQ': 'Arqit Quantum', 'QUBT': 'Quantum Computing'}
            },
            'Semiconductor Industry': {
                'search_terms': ['semiconductor shortage', 'chip manufacturing', 'semiconductor industry', 'chip production', 'semiconductor equipment', 'chip design'],
                'stocks': {'NVDA': 'NVIDIA', 'AMD': 'AMD', 'LSCC': 'Lattice Semi', 'POWI': 'Power Integrations', 'MPWR': 'Monolithic Power', 'DIOD': 'Diodes Inc'}
            },
            'DevOps and SRE': {
                'search_terms': ['DevOps', 'Site Reliability Engineering', 'GitOps', 'DevSecOps', 'Infrastructure as Code', 'continuous deployment'],
                'stocks': {'TEAM': 'Atlassian', 'DDOG': 'Datadog', 'PD': 'PagerDuty', 'ESTC': 'Elastic', 'API': 'Agora', 'DT': 'Dynatrace'}
            },
            'Edge Computing': {
                'search_terms': ['edge computing', 'edge AI', 'edge cloud', 'edge analytics', 'IoT edge', 'edge security'],
                'stocks': {'FSLY': 'Fastly', 'NET': 'Cloudflare', 'AKAM': 'Akamai', 'EQIX': 'Equinix', 'LLAP': 'Terran Orbital', 'SWCH': 'Switch'}
            },
            'Robotics and Automation': {
                'search_terms': ['industrial robotics', 'collaborative robots', 'robot automation', 'warehouse robotics', 'medical robotics', 'robotic process automation'],
                'stocks': {'ABB': 'ABB Ltd', 'ISRG': 'Intuitive Surgical', 'IRBT': 'iRobot', 'BRKS': 'Brooks Automation', 'AVAV': 'AeroVironment', 'NNDM': 'Nano Dimension'}
            },
            'Electric Vehicles': {
                'search_terms': ['electric vehicles', 'EV charging', 'EV battery', 'electric car', 'EV technology', 'autonomous vehicles'],
                'stocks': {'TSLA': 'Tesla', 'RIVN': 'Rivian', 'CHPT': 'ChargePoint', 'BLNK': 'Blink Charging', 'FSR': 'Fisker', 'NKLA': 'Nikola'}
            },
            'Space Technology': {
                'search_terms': ['space technology', 'satellite internet', 'space exploration', 'rocket technology', 'space tourism', 'satellite communication'],
                'stocks': {'SPCE': 'Virgin Galactic', 'RKLB': 'Rocket Lab', 'ASTR': 'Astra Space', 'MNTS': 'Momentus', 'IRDM': 'Iridium', 'BKSY': 'BlackSky'}
            },
            'Healthcare Technology': {
                'search_terms': ['digital health', 'telemedicine', 'health tech', 'medical AI', 'remote patient monitoring', 'digital therapeutics', 'healthcare analytics'],
                'stocks': {'TDOC': 'Teladoc', 'AMWL': 'Amwell', 'DOCS': 'Doximity', 'PHIC': 'Population Health', 'ONEM': '1Life Healthcare', 'HTEC': 'ROBO Health Tech ETF'}
            },
            'Biotech Innovation': {
                'search_terms': ['gene therapy', 'CRISPR technology', 'mRNA technology', 'biotech research', 'precision medicine', 'genomic sequencing'],
                'stocks': {'CRSP': 'CRISPR', 'NTLA': 'Intellia', 'EDIT': 'Editas Medicine', 'BEAM': 'Beam Therapeutics', 'VERV': 'Verve Therapeutics', 'DNAY': 'Codex DNA'}
            },
            'Fintech and Digital Payments': {
                'search_terms': ['digital payments', 'cryptocurrency', 'blockchain technology', 'digital banking', 'mobile payments', 'payment processing'],
                'stocks': {'SQ': 'Block', 'AFRM': 'Affirm', 'UPST': 'Upstart', 'MARA': 'Marathon Digital', 'RIOT': 'Riot Platforms', 'BITF': 'Bitfarms'}
            },
            'Metaverse and AR/VR': {
                'search_terms': ['metaverse', 'virtual reality', 'augmented reality', 'mixed reality', 'VR gaming', 'AR applications'],
                'stocks': {'META': 'Meta', 'U': 'Unity', 'MTTR': 'Matterport', 'IMMR': 'Immersion', 'VUZI': 'Vuzix', 'KOPN': 'Kopin'}
            },
            'Clean Energy Tech': {
                'search_terms': ['renewable energy', 'solar technology', 'wind power', 'energy storage', 'green hydrogen', 'smart grid'],
                'stocks': {'ENPH': 'Enphase', 'FSLR': 'First Solar', 'RUN': 'Sunrun', 'NOVA': 'Sunnova', 'STEM': 'Stem Inc', 'BLDP': 'Ballard Power'}
            },
            'Smart Manufacturing': {
                'search_terms': ['industrial IoT', 'smart factory', '3D printing', 'digital twin', 'predictive maintenance', 'manufacturing automation'],
                'stocks': {'DDD': '3D Systems', 'XONE': 'ExOne', 'MKFG': 'Markforged', 'VLD': 'Velo3D', 'SHCR': 'Sharecare', 'FARO': 'FARO Technologies'}
            },
            '5G and Connectivity': {
                'search_terms': ['5G network', '5G technology', 'wireless infrastructure', 'network virtualization', '5G applications', 'mobile edge computing'],
                'stocks': {'ERIC': 'Ericsson', 'NOK': 'Nokia', 'AVNW': 'Aviat Networks', 'GILT': 'Gilat Satellite', 'CMBM': 'Cambium Networks', 'INFN': 'Infinera'}
            },
            'Data Analytics': {
                'search_terms': ['big data analytics', 'data science', 'predictive analytics', 'business intelligence', 'data visualization', 'real-time analytics'],
                'stocks': {'SNOW': 'Snowflake', 'MDB': 'MongoDB', 'AYX': 'Alteryx', 'CLDR': 'Cloudera', 'TYL': 'Tyler Tech', 'ALTR': 'Altair Engineering'}
            }
        }

    async def get_recent_trend_data(self, term):
        """Get recent trend data with improved rate limiting"""
        try:
            logger.info(f"Getting trend data for: {term}")
            
            # Get 90-day data including today
            today = datetime.now().date()
            today_str = today.strftime('%Y-%m-%d')
            ninety_day_timeframe = f"{(today - timedelta(days=90)).strftime('%Y-%m-%d')} {today_str}"
            
            await asyncio.sleep(random.uniform(5, 10))
            
            self.pytrends.build_payload([term], timeframe=ninety_day_timeframe, geo='US')
            historical_data = self.pytrends.interest_over_time()
            
            if historical_data is None or historical_data.empty:
                logger.info(f"No data available for: {term}")
                return None
            
            # Get today's value from the 90-day data
            today_value = float(historical_data[term].iloc[-1])
            logger.info(f"\nToday's value for {term}: {today_value}")
            
            # If today's value is less than 90, no need to continue
            if today_value < 90:
                logger.info(f"Today's value ({today_value}) is below threshold of 90")
                return None
            
            # Calculate statistical measures using data excluding today
            historical_data_prev = historical_data[:-1]  # Exclude today
            historical_mean = historical_data_prev[term].mean()
            historical_std = historical_data_prev[term].std()
            z_score = (today_value - historical_mean) / historical_std if historical_std > 0 else 0
            
            logger.info(f"\nHistorical statistics (90-day):")
            logger.info(f"Average: {historical_mean:.2f}")
            logger.info(f"Std Dev: {historical_std:.2f}")
            logger.info(f"Z-score: {z_score:.2f}")
            logger.info(f"Today vs Average: {(today_value/historical_mean*100):.1f}%")
            
            # Check for breakout conditions
            is_breakout = (
                today_value >= 90 and  # Base threshold
                today_value >= historical_mean * 1.5 and  # 50% above average
                z_score >= 2.0  # At least 2 standard deviations
            )
            
            if is_breakout:
                logger.info(f"Breakout confirmed for {term}:")
                logger.info(f"- Today's value ({today_value}) is 50%+ above average ({historical_mean:.1f})")
                logger.info(f"- Z-score of {z_score:.2f} indicates statistical significance")
                return historical_data
            else:
                logger.info(f"No breakout for {term}:")
                if today_value < 90:
                    logger.info("- Below base threshold of 90")
                if today_value < historical_mean * 1.5:
                    logger.info("- Not 50% above average")
                if z_score < 2.0:
                    logger.info("- Z-score below threshold")
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
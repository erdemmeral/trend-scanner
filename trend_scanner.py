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
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

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
        # Set timezone to US Eastern (360 = Central Time)
        self.pytrends = TrendReq(
            hl='en-US',  # Language set to US English
            tz=240,      # Eastern Time (ET)
            geo='US'     # Default geo location to US
        )
        self.last_scan_time = None
        self.request_count = 0
        self.last_request_time = time.time()
        self.request_limit = 10
        self.cooldown_minutes = 1
        self.consecutive_429s = 0
        self.base_delay = 5
        
        # Initialize categories with term-specific stocks
        self.categories = {
            'AI and Machine Learning': {
                'search_terms': {
                    'artificial intelligence': {'NVDA': 'NVIDIA', 'AI': 'C3.ai', 'PLTR': 'Palantir'},
                    'machine learning': {'NVDA': 'NVIDIA', 'AI': 'C3.ai', 'MSFT': 'Microsoft'},
                    'deep learning': {'NVDA': 'NVIDIA', 'AMD': 'AMD', 'GOOGL': 'Google'},
                    'neural networks': {'NVDA': 'NVIDIA', 'AMD': 'AMD', 'INTC': 'Intel'},
                    'GPT-4': {'MSFT': 'Microsoft', 'GOOGL': 'Google', 'META': 'Meta'},
                    'large language models': {'MSFT': 'Microsoft', 'GOOGL': 'Google', 'META': 'Meta'},
                    'computer vision AI': {'NVDA': 'NVIDIA', 'PATH': 'UiPath', 'BBAI': 'BigBear.ai'}
                }
            },
            'Cloud Computing': {
                'search_terms': {
                    'cloud computing': {'AMZN': 'Amazon', 'MSFT': 'Microsoft', 'GOOGL': 'Google'},
                    'cloud services': {'DDOG': 'Datadog', 'NET': 'Cloudflare', 'SNOW': 'Snowflake'},
                    'cloud storage': {'BOX': 'Box', 'DBX': 'Dropbox', 'AMZN': 'Amazon AWS'},
                    'cloud security': {'NET': 'Cloudflare', 'PANW': 'Palo Alto', 'CRWD': 'CrowdStrike'},
                    'hybrid cloud': {'IBM': 'IBM', 'VMW': 'VMware', 'RHT': 'Red Hat'},
                    'multi cloud': {'DDOG': 'Datadog', 'ESTC': 'Elastic', 'SUMO': 'Sumo Logic'}
                }
            },
            'Cybersecurity': {
                'search_terms': {
                    'cybersecurity': {'CRWD': 'CrowdStrike', 'PANW': 'Palo Alto', 'FTNT': 'Fortinet'},
                    'zero trust security': {'OKTA': 'Okta', 'ZS': 'Zscaler', 'NET': 'Cloudflare'},
                    'ransomware protection': {'CRWD': 'CrowdStrike', 'S': 'SentinelOne', 'VRNS': 'Varonis'},
                    'endpoint security': {'CRWD': 'CrowdStrike', 'BB': 'BlackBerry', 'TENB': 'Tenable'},
                    'network security': {'PANW': 'Palo Alto', 'FTNT': 'Fortinet', 'RDWR': 'Radware'},
                    'cyber attack': {'CYBR': 'CyberArk', 'RPD': 'Rapid7', 'MNDT': 'Mandiant'}
                }
            },
            'Quantum Computing': {
                'search_terms': {
                    'quantum computing': {'IBM': 'IBM', 'IONQ': 'IonQ', 'RGTI': 'Rigetti'},
                    'quantum computer': {'IONQ': 'IonQ', 'RGTI': 'Rigetti', 'QUBT': 'Quantum Computing'},
                    'quantum supremacy': {'GOOGL': 'Google', 'IBM': 'IBM', 'IONQ': 'IonQ'},
                    'quantum encryption': {'ARQQ': 'Arqit Quantum', 'IBM': 'IBM', 'QUBT': 'Quantum Computing'},
                    'quantum processor': {'IONQ': 'IonQ', 'RGTI': 'Rigetti', 'IBM': 'IBM'}
                }
            },
            'Semiconductor Industry': {
                'search_terms': {
                    'semiconductor shortage': {'TSM': 'TSMC', 'INTC': 'Intel', 'UMC': 'United Micro'},
                    'chip manufacturing': {'AMAT': 'Applied Materials', 'ASML': 'ASML', 'LRCX': 'Lam Research'},
                    'semiconductor industry': {'NVDA': 'NVIDIA', 'AMD': 'AMD', 'QCOM': 'Qualcomm'},
                    'chip production': {'TSM': 'TSMC', 'INTC': 'Intel', 'SSNLF': 'Samsung'},
                    'semiconductor equipment': {'AMAT': 'Applied Materials', 'KLAC': 'KLA Corp', 'TER': 'Teradyne'},
                    'chip design': {'NVDA': 'NVIDIA', 'ARM': 'ARM Holdings', 'CDNS': 'Cadence Design'}
                }
            },
            'DevOps and SRE': {
                'search_terms': {
                    'DevOps': {'TEAM': 'Atlassian', 'DDOG': 'Datadog', 'PD': 'PagerDuty'},
                    'Site Reliability Engineering': {'DDOG': 'Datadog', 'NOW': 'ServiceNow', 'DT': 'Dynatrace'},
                    'GitOps': {'TEAM': 'Atlassian', 'GTLB': 'GitLab', 'GHYB': 'GitHub'},
                    'DevSecOps': {'PANW': 'Palo Alto', 'CRWD': 'CrowdStrike', 'FTNT': 'Fortinet'},
                    'Infrastructure as Code': {'HASHI': 'HashiCorp', 'MSFT': 'Microsoft', 'RHT': 'Red Hat'},
                    'continuous deployment': {'TEAM': 'Atlassian', 'DDOG': 'Datadog', 'NEWR': 'New Relic'}
                }
            },
            'Edge Computing': {
                'search_terms': {
                    'edge computing': {'FSLY': 'Fastly', 'NET': 'Cloudflare', 'AKAM': 'Akamai'},
                    'edge AI': {'NVDA': 'NVIDIA', 'INTC': 'Intel', 'XLNX': 'Xilinx'},
                    'edge cloud': {'AMZN': 'Amazon', 'MSFT': 'Microsoft', 'NET': 'Cloudflare'},
                    'edge analytics': {'DDOG': 'Datadog', 'SPLK': 'Splunk', 'DT': 'Dynatrace'},
                    'IoT edge': {'CSCO': 'Cisco', 'DELL': 'Dell', 'HPE': 'HP Enterprise'},
                    'edge security': {'NET': 'Cloudflare', 'PANW': 'Palo Alto', 'FTNT': 'Fortinet'}
                }
            },
            'Robotics and Automation': {
                'search_terms': {
                    'industrial robotics': {'ABB': 'ABB Ltd', 'FANUY': 'Fanuc', 'SIEGY': 'Siemens'},
                    'collaborative robots': {'ABB': 'ABB Ltd', 'ISRG': 'Intuitive Surgical', 'TER': 'Teradyne'},
                    'robot automation': {'ROK': 'Rockwell Automation', 'CGNX': 'Cognex', 'BRKS': 'Brooks Automation'},
                    'warehouse robotics': {'AMZN': 'Amazon', 'KION': 'KION Group', 'THNKY': 'THK Co'},
                    'medical robotics': {'ISRG': 'Intuitive Surgical', 'MASI': 'Masimo', 'STXS': 'Stereotaxis'},
                    'robotic process automation': {'PATH': 'UiPath', 'NICE': 'Nice Ltd', 'BRKS': 'Brooks Automation'}
                }
            },
            'Electric Vehicles': {
                'search_terms': {
                    'electric vehicles': {'TSLA': 'Tesla', 'NIO': 'NIO', 'XPEV': 'XPeng'},
                    'EV charging': {'CHPT': 'ChargePoint', 'BLNK': 'Blink Charging', 'EVGO': 'EVgo'},
                    'EV battery': {'QS': 'QuantumScape', 'FREY': 'FREYR Battery', 'MVST': 'Microvast'},
                    'electric car': {'TSLA': 'Tesla', 'LCID': 'Lucid', 'RIVN': 'Rivian'},
                    'EV technology': {'TSLA': 'Tesla', 'ALB': 'Albemarle', 'PCRFY': 'Panasonic'},
                    'autonomous vehicles': {'TSLA': 'Tesla', 'GOOGL': 'Waymo/Google', 'APTV': 'Aptiv'}
                }
            },
            'Space Technology': {
                'search_terms': {
                    'space technology': {'SPCE': 'Virgin Galactic', 'BA': 'Boeing', 'LMT': 'Lockheed Martin'},
                    'satellite internet': {'STRL': 'Starlink/SpaceX', 'VSAT': 'Viasat', 'MAXR': 'Maxar'},
                    'space exploration': {'RKLB': 'Rocket Lab', 'SPCE': 'Virgin Galactic', 'BA': 'Boeing'},
                    'rocket technology': {'RKLB': 'Rocket Lab', 'ASTR': 'Astra Space', 'BA': 'Boeing'},
                    'space tourism': {'SPCE': 'Virgin Galactic', 'BKNG': 'Booking Holdings', 'EXPE': 'Expedia'},
                    'satellite communication': {'IRDM': 'Iridium', 'GSAT': 'Globalstar', 'SATS': 'EchoStar'}
                }
            },
            'Healthcare Technology': {
                'search_terms': {
                    'digital health': {'TDOC': 'Teladoc', 'AMWL': 'Amwell', 'DOCS': 'Doximity'},
                    'telemedicine': {'TDOC': 'Teladoc', 'AMWL': 'Amwell', 'ONEM': '1Life Healthcare'},
                    'health tech': {'VEEV': 'Veeva Systems', 'CERN': 'Cerner', 'INOV': 'Inovalon'},
                    'medical AI': {'ISRG': 'Intuitive Surgical', 'NVTA': 'Invitae', 'SDGR': 'Schrödinger'},
                    'remote patient monitoring': {'DXCM': 'Dexcom', 'TNDM': 'Tandem Diabetes', 'PHG': 'Philips'},
                    'digital therapeutics': {'PEAR': 'Pear Therapeutics', 'LVGO': 'Livongo', 'OMCL': 'Omnicell'},
                    'healthcare analytics': {'CERN': 'Cerner', 'INOV': 'Inovalon', 'HCAT': 'Health Catalyst'}
                }
            },
            'Biotech Innovation': {
                'search_terms': {
                    'gene therapy': {'CRSP': 'CRISPR Therapeutics', 'NTLA': 'Intellia', 'EDIT': 'Editas'},
                    'CRISPR technology': {'CRSP': 'CRISPR Therapeutics', 'NTLA': 'Intellia', 'BEAM': 'Beam Therapeutics'},
                    'mRNA technology': {'MRNA': 'Moderna', 'BNTX': 'BioNTech', 'ARCT': 'Arcturus'},
                    'biotech research': {'ILMN': 'Illumina', 'TMO': 'Thermo Fisher', 'DHR': 'Danaher'},
                    'precision medicine': {'EXAS': 'Exact Sciences', 'GH': 'Guardant Health', 'NVTA': 'Invitae'},
                    'genomic sequencing': {'ILMN': 'Illumina', 'PACB': 'Pacific Biosciences', 'DNA': 'Ginkgo Bioworks'}
                }
            },
            'Fintech and Digital Payments': {
                'search_terms': {
                    'digital payments': {'SQ': 'Block', 'PYPL': 'PayPal', 'V': 'Visa'},
                    'cryptocurrency': {'COIN': 'Coinbase', 'MARA': 'Marathon Digital', 'RIOT': 'Riot Platforms'},
                    'blockchain technology': {'COIN': 'Coinbase', 'SQ': 'Block', 'IBM': 'IBM'},
                    'digital banking': {'SQ': 'Block', 'SOFI': 'SoFi', 'AFRM': 'Affirm'},
                    'mobile payments': {'SQ': 'Block', 'PYPL': 'PayPal', 'AAPL': 'Apple'},
                    'payment processing': {'V': 'Visa', 'MA': 'Mastercard', 'ADYEY': 'Adyen'}
                }
            },
            'Metaverse and AR/VR': {
                'search_terms': {
                    'metaverse': {'META': 'Meta', 'RBLX': 'Roblox', 'U': 'Unity'},
                    'virtual reality': {'META': 'Meta', 'SONY': 'Sony', 'MSFT': 'Microsoft'},
                    'augmented reality': {'SNAP': 'Snap', 'MSFT': 'Microsoft', 'AAPL': 'Apple'},
                    'mixed reality': {'MSFT': 'Microsoft', 'META': 'Meta', 'AAPL': 'Apple'},
                    'VR gaming': {'U': 'Unity', 'RBLX': 'Roblox', 'SONY': 'Sony'},
                    'AR applications': {'SNAP': 'Snap', 'U': 'Unity', 'VUZI': 'Vuzix'}
                }
            },
            'Clean Energy Tech': {
                'search_terms': {
                    'renewable energy': {'NEE': 'NextEra', 'ENPH': 'Enphase', 'SEDG': 'SolarEdge'},
                    'solar technology': {'ENPH': 'Enphase', 'SEDG': 'SolarEdge', 'FSLR': 'First Solar'},
                    'wind power': {'NEE': 'NextEra', 'GE': 'General Electric', 'VWDRY': 'Vestas'},
                    'energy storage': {'TSLA': 'Tesla', 'STEM': 'Stem Inc', 'FLUX': 'Flux Power'},
                    'green hydrogen': {'PLUG': 'Plug Power', 'BE': 'Bloom Energy', 'FCEL': 'FuelCell'},
                    'smart grid': {'ITRI': 'Itron', 'ENPH': 'Enphase', 'POWR': 'PowerSecure'}
                }
            },
            'Smart Manufacturing': {
                'search_terms': {
                    'industrial IoT': {'HON': 'Honeywell', 'ROK': 'Rockwell', 'PTC': 'PTC Inc'},
                    'smart factory': {'ROK': 'Rockwell', 'ABB': 'ABB Ltd', 'SIEGY': 'Siemens'},
                    '3D printing': {'DDD': '3D Systems', 'SSYS': 'Stratasys', 'MTLS': 'Materialise'},
                    'digital twin': {'PTC': 'PTC Inc', 'ANSS': 'ANSYS', 'DASTY': 'Dassault'},
                    'predictive maintenance': {'PTC': 'PTC Inc', 'ADSK': 'Autodesk', 'DDOG': 'Datadog'},
                    'manufacturing automation': {'ROK': 'Rockwell', 'CGNX': 'Cognex', 'ZBRA': 'Zebra Tech'}
                }
            },
            '5G and Connectivity': {
                'search_terms': {
                    '5G network': {'ERIC': 'Ericsson', 'NOK': 'Nokia', 'QCOM': 'Qualcomm'},
                    '5G technology': {'QCOM': 'Qualcomm', 'ERIC': 'Ericsson', 'TMUS': 'T-Mobile'},
                    'wireless infrastructure': {'AMT': 'American Tower', 'CCI': 'Crown Castle', 'SBAC': 'SBA Comm'},
                    'network virtualization': {'VMW': 'VMware', 'CSCO': 'Cisco', 'RBBN': 'Ribbon Comm'},
                    '5G applications': {'QCOM': 'Qualcomm', 'SWKS': 'Skyworks', 'KEYS': 'Keysight'},
                    'mobile edge computing': {'AKAM': 'Akamai', 'FSLY': 'Fastly', 'NET': 'Cloudflare'}
                }
            },
            'Data Analytics': {
                'search_terms': {
                    'big data analytics': {'SNOW': 'Snowflake', 'PLTR': 'Palantir', 'SPLK': 'Splunk'},
                    'data science': {'PLTR': 'Palantir', 'SNOW': 'Snowflake', 'WDAY': 'Workday'},
                    'predictive analytics': {'AYX': 'Alteryx', 'DDOG': 'Datadog', 'SPLK': 'Splunk'},
                    'business intelligence': {'CRM': 'Salesforce', 'MSFT': 'Microsoft', 'PLAN': 'Anaplan'},
                    'data visualization': {'DATA': 'Tableau', 'QLIK': 'Qlik', 'TIBX': 'TIBCO'},
                    'real-time analytics': {'DDOG': 'Datadog', 'ESTC': 'Elastic', 'NEWR': 'New Relic'}
                }
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
            
            # Set geo to 'US' for United States
            self.pytrends.build_payload(
                [term],
                timeframe=ninety_day_timeframe,
                geo='US',  # Explicitly set to United States
                gprop=''   # General web search
            )
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
            
            # Calculate 90-day statistics (excluding today)
            historical_data_prev = historical_data[:-1]  # Exclude today
            ninety_day_mean = historical_data_prev[term].mean()
            ninety_day_std = historical_data_prev[term].std()
            z_score = (today_value - ninety_day_mean) / ninety_day_std if ninety_day_std > 0 else 0
            
            # Calculate 30-day statistics
            thirty_day_data = historical_data.tail(30)[:-1]  # Last 30 days excluding today
            thirty_day_mean = thirty_day_data[term].mean()
            thirty_day_std = thirty_day_data[term].std()
            thirty_day_z_score = (today_value - thirty_day_mean) / thirty_day_std if thirty_day_std > 0 else 0
            
            logger.info(f"\nHistorical Statistics:")
            logger.info(f"90-day Average: {ninety_day_mean:.2f}")
            logger.info(f"90-day Std Dev: {ninety_day_std:.2f}")
            logger.info(f"90-day Z-score: {z_score:.2f}")
            logger.info(f"30-day Average: {thirty_day_mean:.2f}")
            logger.info(f"30-day Std Dev: {thirty_day_std:.2f}")
            logger.info(f"30-day Z-score: {thirty_day_z_score:.2f}")
            logger.info(f"Today vs 90-day Avg: {(today_value/ninety_day_mean*100):.1f}%")
            logger.info(f"Today vs 30-day Avg: {(today_value/thirty_day_mean*100):.1f}%")
            
            # Check for breakout conditions
            is_breakout = (
                today_value >= 90 and  # Base threshold
                today_value >= ninety_day_mean * 1.5 and  # 50% above 90-day average
                z_score >= 2.0 and  # Significant deviation from 90-day
                today_value >= thirty_day_mean * 1.3  # 30% above 30-day average
            )
            
            if is_breakout:
                logger.info(f"Breakout confirmed for {term}:")
                logger.info(f"- Today's value ({today_value}) is 50%+ above 90-day average ({ninety_day_mean:.1f})")
                logger.info(f"- Today's value is 30%+ above 30-day average ({thirty_day_mean:.1f})")
                logger.info(f"- 90-day Z-score of {z_score:.2f} indicates statistical significance")
                return historical_data
            else:
                logger.info(f"No breakout for {term}:")
                if today_value < 90:
                    logger.info("- Below base threshold of 90")
                if today_value < ninety_day_mean * 1.5:
                    logger.info("- Not 50% above 90-day average")
                if today_value < thirty_day_mean * 1.3:
                    logger.info("- Not 30% above 30-day average")
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
                    list(data['search_terms'].keys()),
                    category
                )
                
                if results:
                    message = f"🚨 Breakout Alert for {category}:\n\n"
                    for r in results:
                        term = r['term']
                        message += f"📈 Term: {term}\n"
                        message += f"Value: {r['value']:.1f}\n"
                        
                        # Add stocks specific to this term
                        message += "\n💼 Related Stocks:\n"
                        term_stocks = data['search_terms'][term]
                        for symbol, company in term_stocks.items():
                            message += f"${symbol} - {company}\n"
                        
                        message += "\n"  # Add spacing between terms
                    
                    logger.info(f"Breakout detected in {category}:")
                    logger.info(message)
                    
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
        
        # Create scheduler
        scheduler = AsyncIOScheduler()
        
        # Schedule the scan to run at 10:15 UTC daily
        scheduler.add_job(
            scanner.run_continuous_scan,
            CronTrigger(hour=12, minute=30),
            name='daily_scan'
        )
        
        # Start the scheduler
        scheduler.start()
        
        # Keep the program running
        try:
            await asyncio.get_event_loop().create_future()  # run forever
        except (KeyboardInterrupt, SystemExit):
            pass
            
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
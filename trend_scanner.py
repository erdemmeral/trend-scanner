import os
import asyncio
import logging
import signal
import sys
import gc
import warnings
from datetime import datetime, timedelta
import random
import pandas as pd
from itertools import cycle
import time

# Filter out specific warnings
warnings.filterwarnings('ignore', category=FutureWarning)

# Set up logging to ignore lower level logs from other packages
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress other packages' logging
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('pytrends').setLevel(logging.WARNING)
logging.getLogger('pandas').setLevel(logging.WARNING)

# Load environment variables (minimal)
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_IDS = os.getenv('TELEGRAM_CHAT_IDS')
SCAN_INTERVAL_HOURS = int(os.getenv('SCAN_INTERVAL_HOURS', '24'))

# At the top of the file, add test mode settings
IS_TEST_MODE = False  # Set to False for production
TEST_BATCH_SIZE = 3  # Won't be used in production
DISABLE_TELEGRAM = False  # Enable Telegram for production
USE_WEB_SERVER = False  # Disable web server since PythonAnywhere handles that

def log_memory():
    """Helper function to log memory usage"""
    try:
        import psutil
        process = psutil.Process()
        memory = process.memory_info().rss / 1024 / 1024  # Convert to MB
        logger.info(f"Memory usage: {memory:.1f} MB")
    except ImportError:
        pass

# Global variable for the scanner instance
scanner = None

class TrendScanner:
    def __init__(self):
        """
        Initialize scanner with minimal imports
        """
        logger.info("Initializing TrendScanner...")
        log_memory()  # Should show much lower memory usage
        
        # Don't initialize any components yet
        self.pytrends = None
        self.telegram = None
        self.trend_analyzer = None
        self.categories = None
        self.request_delay = 20
        self.last_scan_time = None
        self.request_count = 0
        self.last_request_time = time.time()
        self.request_limit = 10  # Increased from 5 to 10
        self.cooldown_minutes = 1  # Reduced from 2 to 1
        self.consecutive_429s = 0  # Track consecutive rate limits
        self.base_delay = 5  # Start with 5 seconds between requests
        
        log_memory()

    async def initialize_components(self):
        """
        Initialize with full category structure but optimized scanning
        """
        try:
            if not self.pytrends:
                logger.info("Loading pytrends...")
                from pytrends.request import TrendReq
                import pandas as pd
                
                pd.options.mode.chained_assignment = None
                
                self.pytrends = TrendReq(
                    hl='en-US',
                    tz=360,
                    timeout=(30,60),
                    retries=1,
                    backoff_factor=2
                )

            if not self.categories:
                self.categories = {
                    'Quantum Computing': {
                        'search_terms': [
                            'quantum computing', 'quantum computer', 'quantum technology',
                            'quantum processor', 'quantum supremacy', 'quantum advantage',
                            'quantum error correction', 'quantum software', 'quantum cloud'
                        ],
                        'stocks': {
                            'IBM': 'Quantum Computing - Hybrid Systems',
                            'GOOGL': 'Quantum Research',
                            'IONQ': 'Pure-Play Quantum Computing',
                            'RGTI': 'Superconducting Quantum',
                            'QTWO': 'Quantum Software',
                            'QUBT': 'Quantum Software Solutions',
                            'ARQQ': 'Quantum Encryption',
                            'QBTS': 'Quantum Cloud Services',
                            'QTMM': 'Quantum Materials',
                            'DWAC': 'Quantum Technology SPAC'
                        }
                    },
                    'AI and Machine Learning': {
                        'search_terms': [
                            'artificial intelligence', 'machine learning', 'AI technology',
                            'neural networks', 'deep learning', 'large language models',
                            'generative AI', 'AI chatbot', 'computer vision AI',
                            'AI automation', 'AI chips', 'AI software'
                        ],
                        'stocks': {
                            'NVDA': 'AI Hardware - GPU',
                            'AI': 'Enterprise AI Solutions',
                            'PLTR': 'AI Analytics Platform',
                            'CFLT': 'AI Data Platform',
                            'META': 'AI Applications and Research',
                            'MSFT': 'Enterprise AI',
                            'GOOGL': 'AI Research',
                            'BBAI': 'AI Defense Solutions',
                            'SOUN': 'Voice AI Technology',
                            'UPST': 'AI Lending Platform',
                            'MTTR': 'AI Spatial Computing',
                            'AAIT': 'AI Trading Systems'
                        }
                    },
                    'Cybersecurity': {
                        'search_terms': [
                            'cybersecurity', 'network security', 'cloud security',
                            'endpoint security', 'zero trust security', 'cyber defense',
                            'ransomware protection', 'security software', 'threat detection'
                        ],
                        'stocks': {
                            'CSCO': 'Network Security',
                            'PANW': 'Security Platform',
                            'CRWD': 'Cloud Security',
                            'ZS': 'Zero Trust Security',
                            'FTNT': 'Security Solutions',
                            'OKTA': 'Identity Security',
                            'RDWR': 'Application Security',
                            'CYBR': 'Privileged Access',
                            'TENB': 'Vulnerability Management',
                            'SCWX': 'Security Analytics',
                            'VRNS': 'Data Security'
                        }
                    },
                    'Cloud Computing': {
                        'search_terms': [
                            'cloud computing', 'cloud services', 'cloud infrastructure',
                            'cloud storage', 'cloud platform', 'cloud security',
                            'edge computing', 'hybrid cloud', 'multi cloud'
                        ],
                        'stocks': {
                            'AMZN': 'Cloud Infrastructure - AWS',
                            'MSFT': 'Cloud Platform - Azure',
                            'GOOGL': 'Cloud Services - GCP',
                            'NET': 'Edge Computing',
                            'DDOG': 'Cloud Monitoring'
                        }
                    },
                    'Semiconductor Tech': {
                        'search_terms': [
                            'semiconductor technology', 'chip manufacturing', 'processor technology',
                            'advanced chips', 'semiconductor design', 'chip shortage',
                            'semiconductor equipment', 'chip fabrication', 'silicon technology'
                        ],
                        'stocks': {
                            'NVDA': 'GPU and AI Chips',
                            'AMD': 'Processors and Graphics',
                            'INTC': 'Integrated Circuits',
                            'TSM': 'Chip Manufacturing',
                            'AMAT': 'Semiconductor Equipment'
                        }
                    },
                    'Robotics and Automation': {
                        'search_terms': [
                            'robotics technology', 'industrial automation', 'robot manufacturing',
                            'autonomous robots', 'collaborative robots', 'robot software',
                            'robotic process automation', 'robot AI', 'robot systems'
                        ],
                        'stocks': {
                            'ABB': 'Industrial Robotics',
                            'ISRG': 'Surgical Robotics',
                            'UiPath': 'Process Automation',
                            'IRBT': 'Consumer Robotics',
                            'KUKA': 'Manufacturing Robotics'
                        }
                    },
                    'Electric Vehicles': {
                        'search_terms': [
                            'electric vehicles', 'EV technology', 'electric cars',
                            'EV batteries', 'EV charging', 'autonomous vehicles',
                            'electric trucks', 'EV manufacturing', 'EV infrastructure'
                        ],
                        'stocks': {
                            'TSLA': 'EV Manufacturing and Technology',
                            'RIVN': 'Electric Trucks and SUVs',
                            'LCID': 'Luxury Electric Vehicles',
                            'NIO': 'EV and Battery Technology',
                            'CHPT': 'EV Charging Infrastructure'
                        }
                    },
                    'Renewable Energy': {
                        'search_terms': [
                            'renewable energy', 'solar technology', 'wind energy',
                            'energy storage', 'clean energy', 'green hydrogen',
                            'battery technology', 'sustainable energy', 'grid technology'
                        ],
                        'stocks': {
                            'ENPH': 'Solar Technology',
                            'SEDG': 'Solar Power Solutions',
                            'PLUG': 'Hydrogen Technology',
                            'NEE': 'Renewable Utilities',
                            'BEP': 'Clean Energy Operations'
                        }
                    },
                    'AI Compliance': {
                        'search_terms': [
                            'AI compliance', 'AI regulation', 'AI governance',
                            'AI risk management', 'regulatory AI', 'AI audit',
                            'AI ethics compliance', 'AI policy framework', 'AI standards'
                        ],
                        'stocks': {
                            'IBM': 'AI Governance Solutions',
                            'PLTR': 'AI Risk Analytics',
                            'MSFT': 'Responsible AI Tools',
                            'ACN': 'AI Compliance Consulting',
                            'SNOW': 'Data Compliance Platform'
                        }
                    },
                    'Green Computing': {
                        'search_terms': [
                            'green coding', 'sustainable programming', 'eco friendly software',
                            'energy efficient computing', 'sustainable IT', 'green software',
                            'carbon aware computing', 'sustainable cloud', 'green tech'
                        ],
                        'stocks': {
                            'MSFT': 'Sustainable Cloud Solutions',
                            'GOOGL': 'Carbon-Free Computing',
                            'VMW': 'Green Virtualization',
                            'HPE': 'Sustainable IT Infrastructure',
                            'DELL': 'Green Computing Solutions'
                        }
                    },
                    'Chatbot Technology': {
                        'search_terms': [
                            'AI chatbot', 'conversational AI', 'customer service AI',
                            'chatbot platform', 'virtual assistant', 'business chatbot',
                            'chatbot automation', 'AI customer support', 'chat automation'
                        ],
                        'stocks': {
                            'MSFT': 'Enterprise Chatbot Solutions',
                            'GOOGL': 'Conversational AI Platform',
                            'CRM': 'Customer Service AI',
                            'TWLO': 'Communication Platform',
                            'NICE': 'Customer Experience AI'
                        }
                    },
                    'Advanced Cybersecurity': {
                        'search_terms': [
                            'AI cybersecurity', 'machine learning security', 'automated security',
                            'threat detection AI', 'predictive security', 'security automation',
                            'AI threat hunting', 'autonomous security', 'AI malware detection'
                        ],
                        'stocks': {
                            'CRWD': 'AI-Powered Security',
                            'PANW': 'ML Security Solutions',
                            'FTNT': 'Automated Threat Response',
                            'ZS': 'Cloud Security AI',
                            'CYBR': 'Security Automation'
                        }
                    },
                    'Health Tech Innovation': {
                        'search_terms': [
                            'AI healthcare', 'medical technology', 'digital health',
                            'health tech innovation', 'medical AI', 'healthcare automation',
                            'AI diagnostics', 'telehealth technology', 'medical robotics'
                        ],
                        'stocks': {
                            'ISRG': 'Surgical Robotics',
                            'VEEV': 'Healthcare Cloud',
                            'TDOC': 'Telehealth Platform',
                            'DXCM': 'Medical Devices',
                            'INSP': 'Medical Technology',
                            'OMCL': 'Healthcare Automation',
                            'AMWL': 'Virtual Care',
                            'RXRX': 'AI Drug Discovery',
                            'SDGR': 'Molecular Design',
                            'PYCR': 'Healthcare Analytics',
                            'HTOO': 'Digital Health'
                        }
                    },
                    'Edge Computing': {
                        'search_terms': [
                            'edge computing', 'edge AI', 'edge analytics',
                            'IoT edge', 'edge cloud', 'distributed computing',
                            'edge processing', 'edge security', 'edge networking'
                        ],
                        'stocks': {
                            'FSLY': 'Edge Computing Platform',
                            'NET': 'Edge Network Services',
                            'AKAM': 'Edge Security Solutions',
                            'DDOG': 'Edge Monitoring',
                            'CSCO': 'Edge Infrastructure'
                        }
                    },
                    'Blockchain Technology': {
                        'search_terms': [
                            'blockchain technology', 'enterprise blockchain', 'blockchain security',
                            'blockchain platform', 'blockchain analytics', 'smart contracts',
                            'blockchain infrastructure', 'blockchain cloud', 'blockchain development'
                        ],
                        'stocks': {
                            'COIN': 'Blockchain Trading',
                            'SQ': 'Blockchain Payments',
                            'IBM': 'Enterprise Blockchain',
                            'MSTR': 'Blockchain Investment',
                            'RIOT': 'Blockchain Infrastructure'
                        }
                    },
                    'Digital Transformation': {
                        'search_terms': [
                            'digital transformation', 'business automation', 'digital workflow',
                            'enterprise digitization', 'digital strategy', 'digital innovation',
                            'digital platform', 'digital solutions', 'digital operations'
                        ],
                        'stocks': {
                            'NOW': 'Digital Workflow',
                            'CRM': 'Digital Platform',
                            'WDAY': 'Enterprise Digital',
                            'ADBE': 'Digital Experience',
                            'INTU': 'Digital Finance'
                        }
                    },
                    'Data Analytics': {
                        'search_terms': [
                            'big data analytics', 'predictive analytics', 'data visualization',
                            'business intelligence', 'data science', 'real-time analytics',
                            'data platform', 'analytics cloud', 'enterprise analytics'
                        ],
                        'stocks': {
                            'SNOW': 'Data Cloud Platform',
                            'PLTR': 'Data Analytics',
                            'DDOG': 'Monitoring Analytics',
                            'MDB': 'Database Platform',
                            'TYL': 'Government Analytics'
                        }
                    },
                    '5G Technology': {
                        'search_terms': [
                            '5G network', '5G infrastructure', '5G applications',
                            '5G security', '5G edge computing', '5G IoT',
                            '5G enterprise', '5G cloud', '5G innovation'
                        ],
                        'stocks': {
                            'QCOM': '5G Chips',
                            'NOK': '5G Infrastructure',
                            'ERIC': '5G Networks',
                            'TMUS': '5G Services',
                            'AMT': '5G Tower Infrastructure'
                        }
                    },
                    'DevOps and SRE': {
                        'search_terms': [
                            'DevOps platform', 'site reliability', 'CI/CD pipeline',
                            'infrastructure automation', 'DevOps tools', 'cloud native',
                            'container orchestration', 'microservices', 'DevSecOps'
                        ],
                        'stocks': {
                            'TEAM': 'DevOps Tools',
                            'DDOG': 'Observability Platform',
                            'PD': 'Incident Response',
                            'ESTC': 'Log Analytics',
                            'NEWR': 'Observability Solutions'
                        }
                    },
                    'Metaverse Technology': {
                        'search_terms': [
                            'metaverse platform', 'virtual reality', 'augmented reality',
                            'mixed reality', 'digital twin', 'metaverse infrastructure',
                            'virtual worlds', 'AR technology', 'VR development'
                        ],
                        'stocks': {
                            'META': 'Metaverse Platform',
                            'RBLX': 'Virtual Worlds',
                            'U': 'Development Platform',
                            'NVDA': 'Metaverse Hardware',
                            'MSFT': 'Enterprise Metaverse'
                        }
                    },
                    'Agricultural Biotechnology': {
                        'search_terms': [
                            'agricultural biotechnology', 'crop biotechnology', 'agtech innovation',
                            'gene editing crops', 'agricultural genomics', 'precision agriculture',
                            'biotech farming', 'plant biotechnology', 'agricultural genetics',
                            'sustainable biotech farming', 'vertical farming technology', 'biofertilizers'
                        ],
                        'stocks': {
                            'CTVA': 'Agricultural Science',
                            'BIIB': 'Biotech Research',
                            'FMC': 'Agricultural Solutions',
                            'BG': 'Agribusiness Technology',
                            'ADM': 'Agricultural Processing'
                        }
                    },
                    'Enterprise Blockchain': {
                        'search_terms': [
                            'enterprise blockchain', 'supply chain blockchain', 'blockchain identity',
                            'blockchain healthcare', 'blockchain logistics', 'blockchain voting',
                            'blockchain energy', 'blockchain real estate', 'blockchain insurance',
                            'blockchain authentication', 'blockchain tracking', 'blockchain verification'
                        ],
                        'stocks': {
                            'IBM': 'Enterprise Blockchain Solutions',
                            'ORCL': 'Blockchain Platform',
                            'ACN': 'Blockchain Consulting',
                            'SAP': 'Supply Chain Blockchain',
                            'MSFT': 'Blockchain Infrastructure'
                        }
                    },
                    'Precision Medicine': {
                        'search_terms': [
                            'personalized medicine', 'precision healthcare', 'genomic medicine',
                            'targeted therapy', 'personalized treatment', 'precision diagnostics',
                            'genetic therapy', 'custom medicine', 'biomarker testing',
                            'molecular diagnostics', 'pharmacogenomics', 'precision oncology'
                        ],
                        'stocks': {
                            'ILMN': 'Genomic Sequencing',
                            'EXAS': 'Molecular Diagnostics',
                            'GH': 'Precision Oncology',
                            'NVTA': 'Genetic Testing',
                            'PACB': 'Sequencing Technology'
                        }
                    },
                    'Neuromorphic Tech': {
                        'search_terms': [
                            'neuromorphic computing', 'brain chip computing', 'neural processors',
                            'cognitive computing', 'brain inspired computing', 'neuromorphic AI',
                            'synaptic computing', 'neural hardware', 'neuromorphic engineering',
                            'brain based computing', 'neural processing unit', 'cognitive chips'
                        ],
                        'stocks': {
                            'INTC': 'Neuromorphic Processors',
                            'IBM': 'Cognitive Computing',
                            'NVDA': 'Neural Processing',
                            'BRKS': 'Advanced Computing',
                            'AMD': 'Neural Hardware'
                        }
                    },
                    'Health Wearables': {
                        'search_terms': [
                            'health wearables', 'medical wearables', 'fitness tracking',
                            'smart health devices', 'wearable monitors', 'health sensors',
                            'medical tracking devices', 'smart medical wearables', 'health tracking',
                            'continuous health monitoring', 'wearable diagnostics', 'smart health tech'
                        ],
                        'stocks': {
                            'AAPL': 'Consumer Health Wearables',
                            'FIT': 'Fitness Technology',
                            'GRMN': 'Health Monitoring',
                            'PHG': 'Medical Wearables',
                            'DXCM': 'Continuous Monitoring'
                        }
                    },
                    'Advanced Nanotechnology': {
                        'search_terms': [
                            'nanotechnology advances', 'nanotech materials', 'nano manufacturing',
                            'nano medicine', 'nano electronics', 'quantum dots',
                            'nano sensors', 'nano robotics', 'molecular technology',
                            'nano engineering', 'nano computing', 'nano fabrication'
                        ],
                        'stocks': {
                            'CDNS': 'Nano Design',
                            'LRCX': 'Nano Manufacturing',
                            'AMAT': 'Nano Materials',
                            'ASML': 'Nano Lithography',
                            'TEL': 'Nano Electronics'
                        }
                    },
                    'XR Training Solutions': {
                        'search_terms': [
                            'XR training', 'virtual training', 'augmented training',
                            'mixed reality education', 'VR workplace training', 'AR training',
                            'immersive learning', 'XR simulation', 'virtual skills training',
                            'AR workplace safety', 'VR medical training', 'industrial XR'
                        ],
                        'stocks': {
                            'MSFT': 'Enterprise XR',
                            'META': 'VR Training Platform',
                            'U': 'XR Development',
                            'NVDA': 'XR Hardware',
                            'ADSK': 'Industrial XR'
                        }
                    },
                    'Agentic AI': {
                        'search_terms': [
                            'agentic AI', 'autonomous AI', 'AI agents',
                            'self directed AI', 'AI automation', 'intelligent agents',
                            'AI decision making', 'autonomous systems', 'AI reasoning',
                            'cognitive AI', 'AI problem solving', 'agent based AI'
                        ],
                        'stocks': {
                            'MSFT': 'AI Agent Platform',
                            'GOOGL': 'AI Research',
                            'META': 'AI Systems',
                            'PLTR': 'AI Decision Systems',
                            'AI': 'Enterprise AI Agents'
                        }
                    },
                    'Disinformation Defense': {
                        'search_terms': [
                            'disinformation security', 'fake news detection', 'AI fact checking',
                            'content verification', 'information security', 'deepfake detection',
                            'media authentication', 'truth verification', 'misinformation defense',
                            'digital content verification', 'AI content validation', 'authenticity check'
                        ],
                        'stocks': {
                            'MSFT': 'Content Authentication',
                            'GOOGL': 'Information Verification',
                            'META': 'Content Validation',
                            'ADBE': 'Digital Authentication',
                            'PANW': 'Security Solutions'
                        }
                    },
                    'Ambient Intelligence': {
                        'search_terms': [
                            'ambient intelligence', 'smart environments', 'intelligent spaces',
                            'pervasive computing', 'smart surroundings', 'adaptive environments',
                            'context aware computing', 'intelligent automation', 'smart sensing',
                            'ambient computing', 'environmental AI', 'intelligent environments'
                        ],
                        'stocks': {
                            'AAPL': 'Smart Environment',
                            'GOOGL': 'Ambient Computing',
                            'AME': 'Environmental Sensing',
                            'HON': 'Smart Buildings',
                            'JCI': 'Intelligent Spaces'
                        }
                    }
                }
                
            gc.collect()
            
        except Exception as e:
            logger.error(f"Error in initialize_components: {str(e)}")
            raise

    async def start_app(self):
        """
        Initialize Telegram only
        """
        try:
            logger.info("Loading Telegram components...")
            from telegram.ext import Application
            from telegram.constants import ParseMode
            
            # Initialize Telegram
            self.telegram = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
            await self.telegram.initialize()
            await self.telegram.start()
            logger.info("Telegram application started successfully")
            
        except Exception as e:
            logger.error(f"Error in start_app: {str(e)}")
            raise

    async def run_continuous_scan(self):
        """
        Run continuous scanning with proper 24-hour cycle
        """
        while True:
            try:
                cycle_start_time = datetime.now()
                logger.info(f"\n=== Starting New Scan Cycle at {cycle_start_time.strftime('%Y-%m-%d %H:%M:%S')} ===")
                
                self.last_scan_time = cycle_start_time
                await self.initialize_components()
                
                # Run the category scanning
                results = await self.scan_all_categories()
                
                # Calculate remaining time until next cycle
                time_elapsed = (datetime.now() - cycle_start_time).total_seconds()
                sleep_seconds = max(0, SCAN_INTERVAL_HOURS * 3600 - time_elapsed)
                
                if sleep_seconds > 0:
                    next_scan = cycle_start_time + timedelta(hours=SCAN_INTERVAL_HOURS)
                    logger.info(f"\n=== Scan Cycle Complete ===")
                    logger.info(f"Next scan scheduled for: {next_scan.strftime('%Y-%m-%d %H:%M:%S')}")
                    logger.info(f"Sleeping for {sleep_seconds/3600:.1f} hours")
                    
                    # Break sleep into 15-minute intervals with heartbeat
                    sleep_interval = 900  # 15 minutes
                    intervals = int(sleep_seconds // sleep_interval)
                    
                    for i in range(intervals):
                        await asyncio.sleep(sleep_interval)
                        logger.info("\n❤️ Sleep Period Heartbeat:")
                        logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                        logger.info(f"Sleep progress: {(i+1)}/{intervals} intervals")
                        logger.info(f"Next scan in: {(next_scan - datetime.now()).total_seconds()/3600:.1f} hours")
                        log_memory()
                        gc.collect()
                    
                    # Sleep any remaining seconds
                    remaining_seconds = sleep_seconds % sleep_interval
                    if remaining_seconds > 0:
                        await asyncio.sleep(remaining_seconds)
                
            except Exception as e:
                logger.error(f"Error in scan cycle: {str(e)}", exc_info=True)
                logger.info("Restarting scanner in 60 seconds...")
                await asyncio.sleep(60)
                continue

    async def scan_all_categories(self):
        """
        Scan categories in 15-minute batches with 2-minute rest periods
        """
        all_results = {}
        breakouts = []  # Track all breakouts
        start_time = datetime.now()
        last_heartbeat = datetime.now()
        
        # Get list of categories and divide into batches
        categories_to_scan = list(self.categories.items())
        
        if IS_TEST_MODE:
            # For testing, only use first few categories
            categories_to_scan = categories_to_scan[:6]  # Test with 6 categories
            logger.info(f"\n=== TEST MODE ENABLED ===")
            logger.info(f"Testing with {len(categories_to_scan)} categories")
            logger.info(f"Processing {TEST_BATCH_SIZE} terms per 15-minute batch")
        
        # Process categories in smaller time-boxed batches
        current_index = 0
        batch_number = 1
        terms_in_current_batch = 0
        
        while current_index < len(categories_to_scan):
            logger.info(f"\n=== Starting Batch {batch_number} ===")
            batch_start_time = datetime.now()
            terms_in_current_batch = 0
            
            # Process categories until we hit 15 minutes or term limit
            while current_index < len(categories_to_scan):
                try:
                    # Check batch limits
                    if IS_TEST_MODE and terms_in_current_batch >= TEST_BATCH_SIZE:
                        logger.info(f"Test batch size limit ({TEST_BATCH_SIZE}) reached")
                        break
                    
                    if (datetime.now() - batch_start_time).total_seconds() >= 900:  # 15 minutes
                        logger.info("15-minute batch time limit reached")
                        break
                    
                    category, data = categories_to_scan[current_index]
                    logger.info(f"\n{'-'*50}")
                    logger.info(f"Scanning category: {category}")
                    logger.info(f"Category {current_index + 1} of {len(categories_to_scan)}")
                    logger.info(f"Time in current batch: {(datetime.now() - batch_start_time).total_seconds():.0f} seconds")
                    
                    search_terms = data.get('search_terms', [])
                    if not search_terms:
                        logger.info(f"No search terms for category: {category}")
                        current_index += 1
                        continue
                    
                    # Process the category
                    try:
                        results = await asyncio.wait_for(
                            self.scan_trends_with_notification(search_terms, category),
                            timeout=300
                        )
                        
                        if results:
                            all_results[category] = {
                                'trends': results,
                                'stocks': data.get('stocks', {})
                            }
                            # Track breakouts with category info
                            for result in results:
                                if result.get('is_breakout'):
                                    breakouts.append({
                                        'category': category,
                                        'term': result['term'],
                                        'peak': result['recent_peak'],
                                        'stocks': data.get('stocks', {})
                                    })
                        
                    except asyncio.TimeoutError:
                        logger.error(f"Category {category} scan timed out")
                    except Exception as e:
                        logger.error(f"Error scanning category {category}: {str(e)}")
                    
                    # Clear memory after each category
                    gc.collect()
                    log_memory()
                    
                    terms_in_current_batch += len(search_terms)
                    current_index += 1
                    
                except Exception as e:
                    logger.error(f"Critical error in category scan: {str(e)}", exc_info=True)
                    current_index += 1
                    continue
            
            # After batch completes, take a 2-minute break
            logger.info(f"\n=== Batch {batch_number} Complete ===")
            logger.info(f"Processed {terms_in_current_batch} terms in this batch")
            logger.info(f"Total progress: {current_index}/{len(categories_to_scan)} categories")

            # Add batch summary
            logger.info("\nBatch Summary:")
            logger.info(f"Start time: {batch_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Duration: {(datetime.now() - batch_start_time).total_seconds():.0f} seconds")
            logger.info(f"Terms processed: {terms_in_current_batch}")
            logger.info(f"Average time per term: {(datetime.now() - batch_start_time).total_seconds() / terms_in_current_batch:.1f} seconds")

            logger.info("\nStarting 2-minute rest period...")
            
            # Rest period with heartbeat
            rest_start = datetime.now()
            while (datetime.now() - rest_start).total_seconds() < 120:  # 2 minutes
                await asyncio.sleep(30)  # Check every 30 seconds
                logger.info("\n❤️ Rest Period Heartbeat:")
                logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"Completed {current_index}/{len(categories_to_scan)} categories")
                logger.info("Memory status:")
                log_memory()
            
            batch_number += 1
        
        scan_duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"\nComplete scan cycle took {scan_duration:.0f} seconds")
        logger.info(f"Processed {len(categories_to_scan)} categories")
        
        if breakouts:
            # Log to console
            logger.info("\n🚨 BREAKOUT SUMMARY 🚨")
            logger.info("=" * 50)
            logger.info(f"Found {len(breakouts)} breakouts:")
            
            # Prepare Telegram summary
            summary_message = "🚨 <b>DAILY SCAN SUMMARY</b>\n\n"
            summary_message += f"Found {len(breakouts)} breakouts:\n\n"
            
            for b in breakouts:
                # Console logging
                logger.info(f"\n📊 Category: {b['category']}")
                logger.info(f"🔍 Term: {b['term']}")
                logger.info(f"📈 Peak Interest: {b['peak']:.1f}")
                
                # Add to Telegram message
                summary_message += f"📊 <b>{b['category']}</b>\n"
                summary_message += f"🔍 Term: {b['term']}\n"
                summary_message += f"📈 Peak: {b['peak']:.1f}\n"
                
                if b['stocks']:
                    logger.info("Related Stocks:")
                    summary_message += "💼 Stocks:\n"
                    for symbol, desc in b['stocks'].items():
                        logger.info(f"• ${symbol} - {desc}")
                        summary_message += f"• ${symbol} - {desc}\n"
                summary_message += "\n"
            
            # Send summary to Telegram
            await self.send_telegram_alert(summary_message)
        else:
            logger.info("\nNo breakouts detected in this scan cycle")
            # Optionally send "no breakouts" message to Telegram
            await self.send_telegram_alert("📊 <b>Daily Scan Complete</b>\nNo breakouts detected today.")
        
        logger.info("\n=== Scan Cycle Complete ===")
        logger.info(f"Next scan in {SCAN_INTERVAL_HOURS} hours")
        return all_results

    async def scan_trends_with_notification(self, search_terms, category):
        """
        Two-phase scanning with detailed logging and heartbeat
        """
        results = []
        last_activity = datetime.now()
        last_heartbeat = datetime.now()
        low_data_terms = []
        total_terms = len(search_terms)
        
        for index, term in enumerate(search_terms, 1):
            try:
                current_time = datetime.now()
                
                # Add progress indicator
                logger.info(f"\n{'='*50}")
                logger.info(f"Analyzing term: {term} ({index}/{total_terms})")
                logger.info(f"Progress: {(index/total_terms)*100:.1f}%")
                logger.info(f"Time since last activity: {(current_time - last_activity).total_seconds():.0f} seconds")
                
                # Heartbeat check every 2 minutes
                if (current_time - last_heartbeat).total_seconds() >= 120:
                    logger.info("\n❤️ Term Scanner Heartbeat:")
                    logger.info(f"Current time: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    logger.info(f"Current term: {term}")
                    logger.info(f"Time since last activity: {(current_time - last_activity).total_seconds():.0f} seconds")
                    logger.info("Memory status:")
                    log_memory()
                    last_heartbeat = current_time
                
                # Force restart if stuck for too long
                if (current_time - last_activity).total_seconds() > 180:
                    logger.error(f"❌ Scanner stuck on term: {term} for over 3 minutes")
                    logger.error("Forcing skip to next term")
                    raise Exception("Term processing timeout")
                
                logger.info(f"\n{'='*50}")
                logger.info(f"Analyzing term: {term}")
                logger.info(f"Time since last activity: {(current_time - last_activity).total_seconds():.0f} seconds")
                last_activity = current_time
                
                # Log start of API call
                logger.info(f"Starting Google Trends API call for: {term}")
                logger.info("Memory before API call:")
                log_memory()
                
                # Add timeout for API call specifically
                try:
                    recent_data = await asyncio.wait_for(
                        self.get_recent_trend_data(term),
                        timeout=120  # 2 minute timeout
                    )
                    logger.info("API call completed successfully")
                    last_activity = current_time
                    
                except asyncio.TimeoutError:
                    logger.error(f"❌ API call timed out for term: {term}")
                    logger.info("Memory after timeout:")
                    log_memory()
                    continue
                    
                except Exception as e:
                    logger.error(f"❌ API call failed for term: {term}")
                    logger.error(f"Error type: {type(e).__name__}")
                    logger.error(f"Error details: {str(e)}")
                    logger.info("Memory after error:")
                    log_memory()
                    continue
                
                logger.info("Processing API response...")
                
                if recent_data is None:
                    logger.info(f"No data available for: {term}")
                    low_data_terms.append(term)
                    continue
                
                # Process the data
                try:
                    max_value = recent_data[term].max()
                    max_interest = float(max_value.item() if hasattr(max_value, 'item') else max_value)
                    max_time = recent_data[term].idxmax()
                    
                    logger.info(f"\nInterest Analysis for {term}:")
                    logger.info(f"Maximum Interest: {max_interest:.1f}")
                    logger.info(f"Time of Peak: {max_time}")
                    
                    # First check if today's interest is high enough
                    if max_interest < 90:
                        logger.info(f"\n❌ Today's interest ({max_interest:.1f}) is below threshold of 90")
                        continue
                    
                    # Get 90-day historical data for validation
                    try:
                        ninety_day_timeframe = f"{(datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')} {datetime.now().strftime('%Y-%m-%d')}"
                        self.pytrends.build_payload([term], timeframe=ninety_day_timeframe, geo='US')
                        ninety_day_data = self.pytrends.interest_over_time()
                        
                        if ninety_day_data is not None and not ninety_day_data.empty:
                            # Check if today's value is still over 90 in 90-day context
                            today_value_90d = float(ninety_day_data[term].iloc[-1])
                            logger.info(f"\n90-Day Context Check:")
                            logger.info(f"Today's value in 90-day context: {today_value_90d:.1f}")
                            
                            if today_value_90d < 90:
                                logger.info("❌ Today's interest is below 90 in 90-day context")
                                continue
                            
                            # Calculate 90-day statistics
                            ninety_day_avg = float(ninety_day_data[term].mean())
                            ninety_day_max = float(ninety_day_data[term].max())
                            logger.info(f"90-day Average: {ninety_day_avg:.1f}")
                            logger.info(f"90-day Maximum: {ninety_day_max:.1f}")
                            
                            # Only proceed if today's value is near the 90-day maximum
                            if today_value_90d < ninety_day_max * 0.9:  # Within 90% of 90-day max
                                logger.info("❌ Today's interest is not near the 90-day maximum")
                                continue
                        
                    except Exception as e:
                        logger.error(f"Error getting 90-day data: {str(e)}")
                        continue
                    
                    # Get 60-day data for detailed analysis
                    historical_data = await self.get_historical_trend_data(term)
                    if historical_data is not None:
                        baseline_avg = float(historical_data[:-1].mean())
                        baseline_std = float(historical_data[:-1].std())
                        baseline_max = float(historical_data[:-1].max())
                        
                        logger.info("\nHistorical Statistics:")
                        logger.info(f"60-day Average: {baseline_avg:.1f}")
                        logger.info(f"60-day Std Dev: {baseline_std:.1f}")
                        logger.info(f"60-day Maximum: {baseline_max:.1f}")
                        
                        z_score = (max_interest - baseline_avg) / baseline_std if baseline_std > 0 else 0
                        logger.info(f"Z-Score: {z_score:.1f}")
                        
                        # Stricter breakout conditions
                        is_breakout = (
                            max_interest >= 90 and  # Must be at least 90
                            max_interest > baseline_max and  # Must be higher than previous max
                            (
                                (max_interest >= baseline_avg * 2.5) or  # Must be 2.5x the average
                                (z_score >= 3.0 and max_interest >= baseline_max * 1.3)  # Or significant statistical deviation
                            )
                        )
                        
                        if is_breakout:
                            logger.info("\n🚨 BREAKOUT DETECTED!")
                            logger.info(f"Current: {max_interest:.1f} vs Baseline: {baseline_avg:.1f}")
                            logger.info(f"Previous Max: {baseline_max:.1f}")
                            
                            # Additional validation
                            if len(historical_data) >= 7:  # At least a week of data
                                recent_week_avg = float(historical_data[-7:].mean())
                                if recent_week_avg > baseline_avg * 1.5:
                                    logger.info("⚠️ Recent elevated activity detected - might be a false signal")
                                    continue
                            
                            analysis = {
                                'term': term,
                                'recent_peak': max_interest,
                                'peak_time': max_time,
                                'baseline_avg': baseline_avg,
                                'z_score': z_score,
                                'is_breakout': True
                            }
                            results.append(analysis)
                            
                            # Send notification
                            message = await self.format_breakout_message(
                                term=term,
                                max_interest=max_interest,
                                max_time=max_time,
                                baseline_avg=baseline_avg,
                                category=category,
                                stocks=self.categories[category]['stocks']
                            )
                            await self.send_telegram_alert(message)
                        else:
                            logger.info("\nNo breakout detected")
                
                except Exception as e:
                    logger.error(f"Error processing data for {term}: {str(e)}")
                    logger.error("Stack trace:", exc_info=True)
                    continue
                
                finally:
                    # Always update activity timestamp and log completion
                    last_activity = datetime.now()
                    logger.info(f"Completed analysis for: {term}")
                    logger.info("Memory after analysis:")
                    log_memory()
                
                # Brief delay between terms
                await asyncio.sleep(20)
                
            except Exception as e:
                logger.error(f"❌ Outer error analyzing {term}")
                logger.error(f"Error type: {type(e).__name__}")
                logger.error(f"Error details: {str(e)}")
                logger.error("Stack trace:", exc_info=True)
                logger.info("Memory after error:")
                log_memory()
                continue
        
        return results

    async def make_trends_request(self, func, *args, **kwargs):
        """
        Make a rate-limited request to Google Trends with adaptive delays
        """
        current_time = time.time()
        
        # Reset counters if a minute has passed
        if current_time - self.last_request_time >= 60:
            self.request_count = 0
            self.consecutive_429s = max(0, self.consecutive_429s - 1)  # Gradually reduce
            self.last_request_time = current_time
            
            # Adjust base delay based on success
            if self.consecutive_429s == 0:
                self.base_delay = max(5, self.base_delay - 1)
        
        # Adaptive delay based on recent failures
        current_delay = self.base_delay * (1 + self.consecutive_429s)
        await asyncio.sleep(random.uniform(current_delay, current_delay * 1.5))
        
        try:
            wrapped_func = lambda: func(*args, **kwargs)
            result = await asyncio.get_event_loop().run_in_executor(None, wrapped_func)
            self.request_count += 1
            return result
            
        except Exception as e:
            if "429" in str(e):
                self.consecutive_429s += 1
                self.base_delay = min(30, self.base_delay + 5)  # Increase base delay
                logger.warning(f"Rate limit hit (429 count: {self.consecutive_429s})")
                logger.warning(f"Adjusting base delay to {self.base_delay} seconds")
                await asyncio.sleep(self.cooldown_minutes * 60)
                raise
            raise

    async def get_recent_trend_data(self, term):
        """
        Get recent trend data with improved rate limiting
        """
        max_retries = 3
        base_delay = 60  # Increased from 30 to 60
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    delay = base_delay * (2 ** attempt)
                    logger.info(f"Retry attempt {attempt + 1}/{max_retries}, waiting {delay} seconds...")
                    await asyncio.sleep(delay)
                
                ninety_day_timeframe = f"{(datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')} {datetime.now().strftime('%Y-%m-%d')}"
                logger.info(f"Getting 90-day data for validation: {ninety_day_timeframe}")
                
                # Reset pytrends if needed
                if attempt > 0:
                    self.pytrends = TrendReq(
                        hl='en-US',
                        tz=360,
                        timeout=(30,60),
                        retries=2,
                        backoff_factor=2
                    )
                
                # Use rate-limited request wrapper
                await self.make_trends_request(
                    self.pytrends.build_payload,
                    [term],
                    timeframe=ninety_day_timeframe,
                    geo='US',
                    gprop=''
                )
                
                trend_data = await self.make_trends_request(
                    self.pytrends.interest_over_time
                )
                
                # Add detailed logging for data inspection
                if trend_data is None:
                    logger.info(f"Google Trends returned None for term: {term}")
                    return None
                elif trend_data.empty:
                    logger.info(f"Google Trends returned empty DataFrame for term: {term}")
                    return None
                
                logger.info(f"Raw data shape: {trend_data.shape}")
                logger.info(f"Date range: {trend_data.index.min()} to {trend_data.index.max()}")
                logger.info(f"Available columns: {trend_data.columns.tolist()}")
                
                # Get today's date for comparison
                today = datetime.now().date()
                today_data = trend_data[trend_data.index.date == today]
                
                if today_data.empty:
                    # Try yesterday's data since Google Trends might be a day behind
                    yesterday = today - timedelta(days=1)
                    yesterday_data = trend_data[trend_data.index.date == yesterday]
                    
                    if yesterday_data.empty:
                        logger.info(f"No data available for today ({today}) or yesterday ({yesterday})")
                        logger.info("Available dates in data:")
                        unique_dates = pd.Series(trend_data.index.date).unique()
                        logger.info(unique_dates.tolist())
                        return None
                    else:
                        logger.info(f"Using yesterday's data ({yesterday}) as Google Trends is one day behind")
                        max_yesterday = float(yesterday_data[term].max())
                        
                        # Log detailed data for verification
                        logger.info("\n=== Yesterday's Data Analysis ===")
                        logger.info(f"Yesterday's date: {yesterday}")
                        logger.info(f"Yesterday's maximum interest: {max_yesterday:.1f}")
                        logger.info(f"Yesterday's data points: {len(yesterday_data)}")
                        logger.info(f"Time of max interest: {yesterday_data[term].idxmax()}")
                        logger.info(f"All yesterday's values: {yesterday_data[term].tolist()}")
                        
                        return yesterday_data[[term]]
                
                # Get today's maximum interest
                max_today = float(today_data[term].max())
                
                # Log detailed data for verification
                logger.info("\n=== Today's Data Analysis ===")
                logger.info(f"Today's date: {today}")
                logger.info(f"Today's maximum interest: {max_today:.1f}")
                logger.info(f"Today's data points: {len(today_data)}")
                logger.info(f"Time of max interest: {today_data[term].idxmax()}")
                logger.info(f"All today's values: {today_data[term].tolist()}")
                
                return today_data[[term]]
                
            except Exception as e:
                logger.error(f"Error in Google Trends API call: {str(e)}")
                logger.error("Stack trace:", exc_info=True)
                if attempt < max_retries - 1:
                    continue
                return None
            
        logger.error(f"All retries failed for term: {term}")
        return None

    async def get_hourly_data(self, term):
        """
        Get hourly data for the last 24 hours
        """
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(days=1)
            
            timeframe = f"{start_time.strftime('%Y-%m-%d')}T{start_time.strftime('%H')} {end_time.strftime('%Y-%m-%d')}T{end_time.strftime('%H')}"
            logger.info(f"Getting hourly data: {timeframe}")
            
            self.pytrends.build_payload(
                [term], 
                timeframe=timeframe,
                geo='US',
                gprop=''
            )
            
            data = self.pytrends.interest_over_time()
            if data is None or data.empty:
                return None
            
            hourly_data = data.resample('H').mean()
            
            logger.info(f"Got {len(hourly_data)} hourly datapoints")
            logger.info(f"Time range: {hourly_data.index[0]} to {hourly_data.index[-1]}")
            
            max_value = hourly_data[term].max()
            max_interest = float(max_value.item() if hasattr(max_value, 'item') else max_value)
            logger.info(f"Hourly max interest: {max_interest}")
            
            return hourly_data[[term]]
            
        except Exception as e:
            logger.error(f"Error getting hourly data: {str(e)}")
            return None

    async def get_historical_trend_data(self, term):
        """
        Get 60 days of historical data with rate limiting and retries
        """
        max_retries = 3
        base_delay = 60  # Start with 60 second delay
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    logger.info(f"Retry attempt {attempt + 1}/{max_retries}, waiting {delay} seconds...")
                    await asyncio.sleep(delay)
                
                timeframe = f"{(datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')} {datetime.now().strftime('%Y-%m-%d')}"
                
                # Add random delay between requests
                await asyncio.sleep(random.uniform(20, 30))
                
                self.pytrends.build_payload([term], timeframe=timeframe, geo='US')
                data = self.pytrends.interest_over_time()
                
                if data is not None and not data.empty:
                    logger.info(f"Successfully retrieved historical data for {term}")
                    return data[[term]]
                
            except Exception as e:
                if "429" in str(e):
                    if attempt < max_retries - 1:
                        logger.warning(f"Rate limit hit, will retry... ({str(e)})")
                        continue
                    else:
                        logger.error("Max retries reached for rate limit")
                else:
                    logger.error(f"Error getting historical data: {str(e)}")
                return None
        
        return None

    async def send_telegram_alert(self, message):
        """
        Send Telegram notification with group chat support
        """
        try:
            if self.telegram and TELEGRAM_CHAT_IDS:
                chat_ids = TELEGRAM_CHAT_IDS.split(',')
                for chat_id in chat_ids:
                    try:
                        # Convert chat_id to integer, ensuring negative for group chats
                        chat_id = chat_id.strip()
                        chat_id = int(chat_id)
                        if chat_id > 0:  # If it's positive, make it negative for group chats
                            chat_id = -chat_id
                            
                        logger.info(f"Sending notification to group chat ID: {chat_id}")
                        
                        await self.telegram.bot.send_message(
                            chat_id=chat_id,
                            text=message,
                            parse_mode='HTML'
                        )
                        logger.info("Telegram notification sent successfully")
                    except ValueError:
                        logger.error(f"Invalid chat ID format: {chat_id}")
                    except Exception as e:
                        logger.error(f"Failed to send to chat {chat_id}: {str(e)}")
                        continue
        except Exception as e:
            logger.error(f"Error sending Telegram notification: {str(e)}")

    async def format_breakout_message(self, term, max_interest, max_time, baseline_avg, category, stocks):
        """
        Format breakout notification message with 90-day comparison
        """
        try:
            # Get 90-day historical data
            ninety_day_timeframe = f"{(datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')} {datetime.now().strftime('%Y-%m-%d')}"
            self.pytrends.build_payload([term], timeframe=ninety_day_timeframe, geo='US')
            historical_data = self.pytrends.interest_over_time()
            
            if historical_data is not None and not historical_data.empty:
                ninety_day_avg = float(historical_data[term].mean())
            else:
                ninety_day_avg = 0

            message = (
                f"🚨 <b>TREND BREAKOUT DETECTED</b>\n\n"
                f"📊 Category: <b>{category}</b>\n"
                f"🔍 Subcategory: <b>{term}</b>\n\n"
                f"📅 Breakout Date: {max_time.strftime('%Y-%m-%d')}\n"
                f"⏰ Peak Time: {max_time.strftime('%H:%M')} UTC\n\n"
                f"📈 Interest Levels:\n"
                f"• Today's Peak: {max_interest:.1f}\n"
                f"• 90-Day Average: {ninety_day_avg:.1f}\n"
                f"• Increase: {((max_interest/ninety_day_avg) - 1)*100:.1f}%\n\n"
                f"💼 Related Stocks:\n"
            )
            
            # Add stock info with categories
            for symbol, description in stocks.items():
                message += f"• ${symbol} - {description}\n"
            
            # Add footer with time
            message += f"\n⌚ Alert Time: {datetime.now().strftime('%Y-%m-%d %H:%M')} UTC"
            
            return message
            
        except Exception as e:
            logger.error(f"Error formatting breakout message: {str(e)}")
            # Return basic message if error occurs
            return (
                f"🚨 <b>TREND BREAKOUT</b>\n"
                f"Category: {category}\n"
                f"Term: {term}\n"
                f"Peak Interest: {max_interest:.1f}"
            )

async def main():
    """Main application entry point with error recovery"""
    logger.info("=== Tech Trend Scanner ===")
    log_memory()
    
    global scanner
    restart_count = 0
    
    while True:
        try:
            restart_count += 1
            logger.info(f"\n=== Scanner Start (Attempt {restart_count}) ===")
            
            scanner = TrendScanner()
            await scanner.start_app()
            
            # Start scanning with monitoring
            await scanner.run_continuous_scan()
            
        except KeyboardInterrupt:
            logger.info("Received shutdown signal, exiting...")
            break
            
        except Exception as e:
            logger.error(f"Critical error in main loop: {str(e)}", exc_info=True)
            logger.info("Restarting scanner in 60 seconds...")
            await asyncio.sleep(60)
            continue

if __name__ == "__main__":
    try:
        logger.info("Starting main...")
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application shutdown requested")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
    finally:
        logger.info("Application shutdown complete")
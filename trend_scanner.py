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
        # Set timezone to US Eastern and add user agent
        self.pytrends = TrendReq(
            hl='en-US',  # Language set to US English
            tz=240,      # Eastern Time (ET)
            timeout=(10,25),  # Connect and read timeout
            retries=2,   # Number of retries
            backoff_factor=0.1,
            requests_args={
                'headers': {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',  # Do Not Track
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1'
                }
            }
        )
        self.last_scan_time = None
        self.request_count = 0
        self.last_request_time = time.time()
        self.request_limit = 5  # Reduced from 10
        self.cooldown_minutes = 2  # Increased from 1
        self.consecutive_429s = 0
        self.base_delay = 10  # Increased from 5
        
        # Initialize categories with term-specific stocks
        self.categories = {
            'AI and Machine Learning': {
                'search_terms': {
                    'artificial intelligence': {
                        'AI': 'C3.ai - Enterprise AI platform',
                        'PLTR': 'Palantir - AI analytics',
                        'BBAI': 'BigBear.ai - AI defense solutions',
                        'SOUN': 'SoundHound - Voice AI',
                        'AGFY': 'Agrify - AI cultivation tech',
                        'MTTR': 'Matterport - AI spatial data',
                        'THNX': 'THINKsmart - AI fintech'
                    },
                    'machine learning': {
                        'AI': 'C3.ai - ML solutions',
                        'THNX': 'THINKsmart - ML fintech',
                        'CELU': 'Celularity - ML biotech',
                        'DMTK': 'DermTech - ML diagnostics',
                        'MARK': 'Remark - ML analytics',
                        'AITX': 'Artificial Intel Tech - ML robotics',
                        'VTSI': 'VirTra - ML training'
                    },
                    'deep learning': {
                        'AITX': 'Artificial Intel Tech - Robotics',
                        'VTSI': 'VirTra - DL training systems',
                        'CGNT': 'Cognyte - DL security',
                        'AUVI': 'Applied UV - DL disinfection',
                        'BBAI': 'BigBear.ai - DL analytics',
                        'SOUN': 'SoundHound - DL voice',
                        'CELU': 'Celularity - DL biotech'
                    }
                }
            },
            'Cloud Computing': {
                'search_terms': {
                    'cloud computing': {
                        'DOMO': 'Domo - Cloud BI platform',
                        'NCNO': 'nCino - Banking cloud',
                        'SUMO': 'Sumo Logic - Cloud analytics',
                        'CLDR': 'Cloudera - Data cloud',
                        'ESTC': 'Elastic - Search cloud',
                        'DCBO': 'Docebo - Learning cloud',
                        'BIGC': 'BigCommerce - E-com cloud'
                    },
                    'cloud security': {
                        'NET': 'Cloudflare - Edge security',
                        'ZS': 'Zscaler - Cloud security',
                        'TENB': 'Tenable - Vulnerability mgmt',
                        'SAIL': 'SailPoint - Identity security',
                        'RDWR': 'Radware - Cloud protection',
                        'SFET': 'Safe-T Group - Zero trust',
                        'SCWX': 'SecureWorks - Security services'
                    }
                }
            },
            'Cybersecurity': {
                'search_terms': {
                    'cybersecurity': {
                        'VRNS': 'Varonis - Data security',
                        'SCWX': 'SecureWorks - Security services',
                        'CYBE': 'CyberOptics - Inspection tech',
                        'CFBK': 'CF Bankshares - Cyber banking',
                        'SFET': 'Safe-T Group - Zero trust',
                        'CSPI': 'CSP Inc - Security solutions',
                        'HACK': 'ETFMG Cyber Security ETF'
                    },
                    'ransomware protection': {
                        'S': 'SentinelOne - AI security',
                        'VRNS': 'Varonis - Data protection',
                        'OSPN': 'OneSpan - Digital security',
                        'SFET': 'Safe-T Group - Zero trust',
                        'CSPI': 'CSP Inc - Security solutions',
                        'SCWX': 'SecureWorks - Security',
                        'CYBE': 'CyberOptics - Protection'
                    }
                }
            },
            'Quantum Computing': {
                'search_terms': {
                    'quantum computing': {
                        'IONQ': 'IonQ - Trapped ion quantum',
                        'RGTI': 'Rigetti - Superconducting quantum',
                        'QUBT': 'Quantum Computing - Software',
                        'ARQQ': 'Arqit Quantum - Encryption',
                        'QTUM': 'Defiance Quantum ETF',
                        'QTWO': 'Q2 Holdings - Quantum finance',
                        'QMCO': 'Quantum Corp - Storage'
                    },
                    'quantum encryption': {
                        'ARQQ': 'Arqit Quantum - Post-quantum crypto',
                        'QUBT': 'Quantum Computing - Encryption',
                        'IRNT': 'IronNet - Quantum defense',
                        'QMCO': 'Quantum Corp - Security storage',
                        'CEVA': 'CEVA - Quantum IoT security',
                        'QTWO': 'Q2 Holdings - Quantum security',
                        'RGTI': 'Rigetti - Quantum solutions'
                    }
                }
            },
            'Semiconductor Industry': {
                'search_terms': {
                    'semiconductor shortage': {
                        'POWI': 'Power Integrations - Power chips',
                        'DIOD': 'Diodes Inc - Components',
                        'IMOS': 'ChipMOS - Testing services',
                        'MPWR': 'Monolithic Power - Power solutions',
                        'LSCC': 'Lattice Semi - FPGA',
                        'CRUS': 'Cirrus Logic - Mixed-signal',
                        'SIMO': 'Silicon Motion - Controllers'
                    },
                    'chip manufacturing': {
                        'ACLS': 'Axcelis - Ion implantation',
                        'UCTT': 'Ultra Clean - Support systems',
                        'CCMP': 'CMC Materials - Materials',
                        'KLIC': 'Kulicke & Soffa - Packaging',
                        'FORM': 'FormFactor - Testing',
                        'ICHR': 'Ichor - Fluid delivery',
                        'MKSI': 'MKS Instruments - Process control'
                    }
                }
            },
            'DevOps and SRE': {
                'search_terms': {
                    'DevOps': {
                        'PD': 'PagerDuty - Incident response',
                        'NEWR': 'New Relic - Observability',
                        'APPN': 'Appian - Low-code platform',
                        'PRGS': 'Progress Software - DevOps tools',
                        'FROG': 'JFrog - DevOps platform',
                        'ESTC': 'Elastic - Search platform',
                        'PCTY': 'Paylocity - HR DevOps'
                    },
                    'Site Reliability Engineering': {
                        'PD': 'PagerDuty - SRE platform',
                        'DT': 'Dynatrace - Application monitoring',
                        'FROG': 'JFrog - Artifact management',
                        'RPD': 'Rapid7 - Security operations',
                        'MNTV': 'Momentive - User feedback',
                        'SCWX': 'SecureWorks - Security ops',
                        'NEWR': 'New Relic - Performance'
                    }
                }
            },
            'Edge Computing': {
                'search_terms': {
                    'edge computing': {
                        'FSLY': 'Fastly - Edge cloud platform',
                        'LLNW': 'Limelight - Edge services',
                        'GLBE': 'Global-E - Edge commerce',
                        'DCBO': 'Docebo - Edge learning',
                        'CLPS': 'CLPS Inc - Edge solutions',
                        'ALOT': 'AstroNova - Edge systems',
                        'DGII': 'Digi International - IoT edge'
                    },
                    'edge AI': {
                        'CEVA': 'CEVA - Edge AI IP',
                        'LSCC': 'Lattice Semi - Edge FPGA',
                        'QUIK': 'QuickLogic - Edge solutions',
                        'MTSI': 'MACOM - Edge RF',
                        'SWIR': 'Sierra Wireless - Edge IoT',
                        'DGII': 'Digi International - Edge AI',
                        'AMBA': 'Ambarella - Edge vision'
                    },
                    'IoT edge': {
                        'SWIR': 'Sierra Wireless - IoT modules',
                        'IOTC': 'IoTecha - EV charging',
                        'DGII': 'Digi International - IoT solutions',
                        'ATEN': 'A10 Networks - Edge security',
                        'ALOT': 'AstroNova - IoT systems',
                        'KTOS': 'Kratos - IoT defense',
                        'CAMP': 'CalAmp - IoT platforms'
                    }
                }
            },
            'Robotics and Automation': {
                'search_terms': {
                    'industrial robotics': {
                        'STRC': 'Sarcos - Robotic systems',
                        'BKSY': 'BlackSky - Space robotics',
                        'AVAV': 'AeroVironment - Drone systems',
                        'NNDM': 'Nano Dimension - 3D robotics',
                        'CGNX': 'Cognex - Machine vision',
                        'BRQS': 'Borqs - Robotics solutions',
                        'BWXT': 'BWX Technologies - Nuclear robotics'
                    },
                    'warehouse robotics': {
                        'BKNG': 'Berkshire Grey - Fulfillment',
                        'STRC': 'Sarcos Technology - Logistics',
                        'RGDX': 'Righthand Robotics - Picking',
                        'VRRM': 'Verra Mobility - Transport',
                        'THNK': 'Think Robotics - Automation',
                        'BWXT': 'BWX Tech - Material handling',
                        'CGNX': 'Cognex - Vision systems'
                    },
                    'medical robotics': {
                        'ASXC': 'Asensus - Digital surgery',
                        'RBOT': 'Vicarious - Surgical AI',
                        'TRXC': 'TransEnterix - Minimally invasive',
                        'NVRO': 'Nevro - Neural robotics',
                        'STXS': 'Stereotaxis - Surgical robots',
                        'RMTI': 'Rockwell Medical - Medical automation',
                        'BIOL': 'Biolase - Dental robotics'
                    }
                }
            },
            'Electric Vehicles': {
                'search_terms': {
                    'electric vehicles': {
                        'FSR': 'Fisker - EV design',
                        'GOEV': 'Canoo - Lifestyle EVs',
                        'WKHS': 'Workhorse - Delivery EVs',
                        'SOLO': 'ElectraMeccanica - Urban EVs',
                        'MULN': 'Mullen - EV manufacturer',
                        'AYRO': 'AYRO - Light EVs',
                        'IDEX': 'Ideanomics - EV adoption'
                    },
                    'EV charging': {
                        'CHPT': 'ChargePoint - Charging network',
                        'BLNK': 'Blink - Charging stations',
                        'EVGO': 'EVgo - Fast charging',
                        'VLTA': 'Volta - Ad-supported charging',
                        'NXGN': 'NexGen - Power solutions',
                        'DCFC': 'Tritium - DC charging',
                        'ZVIA': 'Zevia - Fleet charging'
                    },
                    'EV battery': {
                        'QS': 'QuantumScape - Solid state',
                        'FREY': 'FREYR - Clean batteries',
                        'MVST': 'Microvast - Fast charging',
                        'SLDP': 'Solid Power - Next-gen',
                        'ENVX': 'Enovix - Silicon batteries',
                        'AMPX': 'Amprius - Battery tech',
                        'IVAN': 'Ivanhoe Electric - Battery materials'
                    }
                }
            },
            'Space Technology': {
                'search_terms': {
                    'space technology': {
                        'SPCE': 'Virgin Galactic - Space tourism',
                        'MNTS': 'Momentus - Space transport',
                        'ASTR': 'Astra - Launch services',
                        'BKSY': 'BlackSky - Earth imaging',
                        'IRDM': 'Iridium - Satellite comms',
                        'RKT': 'Rocket Lab - Launch services',
                        'SATL': 'Satellogic - Earth observation'
                    },
                    'satellite internet': {
                        'ASTS': 'AST SpaceMobile - Space 5G',
                        'GSAT': 'Globalstar - IoT comms',
                        'OSAT': 'Orbsat - Satellite IoT',
                        'GILT': 'Gilat - VSAT networks',
                        'MAXR': 'Maxar - Earth intelligence',
                        'LLAP': 'Terran Orbital - Satellites',
                        'SPIR': 'Spire Global - Space data'
                    }
                }
            },
            'Healthcare Technology': {
                'search_terms': {
                    'digital health': {
                        'AMWL': 'Amwell - Virtual care',
                        'DOCS': 'Doximity - Medical network',
                        'ONEM': '1Life - Primary care',
                        'PHIC': 'Population Health - Analytics',
                        'PSTX': 'Poseida - Digital therapeutics',
                        'RXRX': 'Recursion - AI drug discovery',
                        'CERT': 'Certara - Bio simulation'
                    },
                    'telemedicine': {
                        'AMWL': 'Amwell - Telehealth',
                        'ONEM': '1Life - Digital health',
                        'SGFY': 'Signify - Home health',
                        'OPRX': 'OptimizeRx - Digital health',
                        'WELL': 'Well Health - Virtual care',
                        'TALK': 'Talkspace - Mental health',
                        'LFMD': 'LifeMD - Telehealth platform'
                    }
                }
            },
            'Biotech Innovation': {
                'search_terms': {
                    'gene therapy': {
                        'CRSP': 'CRISPR - Gene editing',
                        'NTLA': 'Intellia - CRISPR tech',
                        'EDIT': 'Editas - Gene editing',
                        'BEAM': 'Beam - Base editing',
                        'VERV': 'Verve - Cardiovascular',
                        'BLUE': 'Bluebird - Gene therapy',
                        'SGMO': 'Sangamo - Genomic medicine'
                    },
                    'CRISPR technology': {
                        'CRSP': 'CRISPR - Core tech',
                        'NTLA': 'Intellia - In vivo editing',
                        'BEAM': 'Beam - Base editing',
                        'VERV': 'Verve - Heart disease',
                        'GRPH': 'Graphite - Cell therapy',
                        'PRME': 'Prime - Gene editing',
                        'DTIL': 'Precision - Gene editing'
                    }
                }
            },
            'Fintech and Digital Payments': {
                'search_terms': {
                    'digital payments': {
                        'SQ': 'Block - Payment solutions',
                        'PYPL': 'PayPal - Digital payments',
                        'V': 'Visa - Payment network',
                        'FOUR': 'Shift4 - Payment tech',
                        'EVLV': 'Evolve - Payment platform',
                        'RPAY': 'Repay - Payment software',
                        'FLYW': 'Flywire - Global payments'
                    },
                    'cryptocurrency': {
                        'COIN': 'Coinbase - Crypto exchange',
                        'MARA': 'Marathon - Bitcoin mining',
                        'RIOT': 'Riot - Blockchain infra',
                        'BITF': 'Bitfarms - Mining ops',
                        'HUT': 'Hut 8 - Digital assets',
                        'NCTY': 'The9 - Crypto mining',
                        'CIFR': 'Cipher - Mining tech'
                    },
                    'digital banking': {
                        'SQ': 'Block - Banking platform',
                        'SOFI': 'SoFi - Digital finance',
                        'AFRM': 'Affirm - BNPL leader',
                        'UPST': 'Upstart - AI lending',
                        'LC': 'LendingClub - P2P finance',
                        'OPFI': 'OppFi - Fintech lending',
                        'MQ': 'Marqeta - Card issuing'
                    }
                }
            },
            'Metaverse and AR/VR': {
                'search_terms': {
                    'metaverse': {
                        'RBLX': 'Roblox - Gaming metaverse',
                        'MTTR': 'Matterport - Digital twins',
                        'IMMR': 'Immersion - Haptics',
                        'VRAR': 'VR/AR ETF',
                        'PLTK': 'Playtika - Virtual worlds',
                        'SLGG': 'Super League - Gaming',
                        'WIMI': 'WiMi - Hologram AR'
                    },
                    'virtual reality': {
                        'VUZI': 'Vuzix - AR glasses',
                        'KOPN': 'Kopin - VR displays',
                        'HIMX': 'Himax - VR chips',
                        'IMAX': 'IMAX - VR experiences',
                        'LKCO': 'Luokung - VR mapping',
                        'WAVX': 'WaveBridge - VR tech',
                        'VRAR': 'Proshares VR ETF'
                    },
                    'augmented reality': {
                        'VUZI': 'Vuzix - Smart glasses',
                        'WAVX': 'WaveBridge - AR tech',
                        'KOPN': 'Kopin - AR displays',
                        'WIMI': 'WiMi - AR platform',
                        'LKCO': 'Luokung - AR mapping',
                        'IMMR': 'Immersion - AR haptics',
                        'MTTR': 'Matterport - AR spaces'
                    }
                }
            },
            'Clean Energy Tech': {
                'search_terms': {
                    'renewable energy': {
                        'NOVA': 'Sunnova - Solar service',
                        'SPWR': 'SunPower - Solar systems',
                        'CSIQ': 'Canadian Solar',
                        'RUN': 'Sunrun - Solar install',
                        'MAXN': 'Maxeon - Solar tech',
                        'ARRY': 'Array Tech - Solar tracking',
                        'AZRE': 'Azure Power - Solar dev'
                    },
                    'green hydrogen': {
                        'BE': 'Bloom Energy - Fuel cells',
                        'FCEL': 'FuelCell - Clean power',
                        'BLDP': 'Ballard - Transport H2',
                        'HTOO': 'Fusion Fuel - Green H2',
                        'HYON': 'HYON - H2 fueling',
                        'HYSR': 'SunHydrogen - Solar H2',
                        'PCELL': 'PowerCell - H2 solutions'
                    },
                    'energy storage': {
                        'STEM': 'Stem Inc - AI storage',
                        'FLUX': 'Flux Power - Batteries',
                        'EOSE': 'Eos Energy - Storage',
                        'FREYR': 'FREYR - Battery cells',
                        'MVST': 'Microvast - Fast charge',
                        'GEVO': 'Gevo - Renewable storage',
                        'BLDP': 'Ballard - H2 storage'
                    }
                }
            },
            'Smart Manufacturing': {
                'search_terms': {
                    'industrial IoT': {
                        'PTC': 'PTC Inc - IoT software',
                        'THNX': 'THINKsmart - IoT solutions',
                        'ONTO': 'Onto Innovation - IoT semi',
                        'SWIR': 'Sierra - IoT networking',
                        'IOTC': 'IoTecha - Industrial IoT',
                        'KTOS': 'Kratos - Defense IoT',
                        'DGII': 'Digi Int - IoT connectivity'
                    },
                    '3D printing': {
                        'DDD': '3D Systems - Printing',
                        'SSYS': 'Stratasys - 3D solutions',
                        'MTLS': 'Materialise - 3D software',
                        'VJET': 'Voxeljet - Industrial 3D',
                        'NNDM': 'Nano Dimension - PCB 3D',
                        'XONE': 'ExOne - Metal 3D',
                        'PRLB': 'Proto Labs - Custom parts'
                    },
                    'predictive maintenance': {
                        'AZPN': 'Aspen Tech - Asset opt',
                        'VRNS': 'Varonis - Data analysis',
                        'PRCP': 'Perceptron - Measurement',
                        'SWKS': 'Skyworks - Sensors',
                        'LUNA': 'Luna - Fiber sensing',
                        'CAMP': 'CalAmp - IoT monitoring',
                        'DGII': 'Digi Int - Remote monitoring'
                    }
                }
            },
            '5G and Connectivity': {
                'search_terms': {
                    '5G network': {
                        'LITE': 'Lumentum - Optical solutions',
                        'INFN': 'Infinera - Network systems',
                        'AVNW': 'Aviat - Wireless transport',
                        'CLFD': 'Clearfield - Fiber connectivity',
                        'DZSI': 'DZS Inc - 5G solutions',
                        'VIAV': 'Viavi - Network test',
                        'CAMP': 'CalAmp - 5G IoT'
                    },
                    'wireless infrastructure': {
                        'UNIT': 'Uniti Group - Fiber networks',
                        'CMBM': 'Cambium - Wireless solutions',
                        'GILT': 'Gilat - Satellite comms',
                        'ATEX': 'Anterix - Private networks',
                        'AVNW': 'Aviat Networks - Transport',
                        'DZSI': 'DZS Inc - Infrastructure',
                        'CLFD': 'Clearfield - Fiber'
                    },
                    'mobile edge computing': {
                        'FSLY': 'Fastly - Edge compute',
                        'GLBE': 'Global-E - Edge commerce',
                        'LLNW': 'Limelight - Edge delivery',
                        'DDOG': 'Datadog - Edge monitoring',
                        'ALOT': 'AstroNova - Edge systems',
                        'CLPS': 'CLPS Inc - Edge solutions',
                        'DGII': 'Digi Int - Edge IoT'
                    }
                }
            },
            'Data Analytics': {
                'search_terms': {
                    'big data analytics': {
                        'SNOW': 'Snowflake - Data cloud',
                        'PLTR': 'Palantir - Data analytics',
                        'SPLK': 'Splunk - Data platform',
                        'TYL': 'Tyler Tech - Gov analytics',
                        'CLDR': 'Cloudera - Big data',
                        'ALTR': 'Altair - Engineering analytics',
                        'SVMX': 'ServiceMax - Field analytics'
                    },
                    'data science': {
                        'PLTR': 'Palantir - AI analytics',
                        'SNOW': 'Snowflake - Data warehouse',
                        'WDAY': 'Workday - HR analytics',
                        'AYX': 'Alteryx - Data science',
                        'TIGR': 'TigerGraph - Graph analytics',
                        'SCPL': 'SciPlay - Gaming analytics',
                        'BIGC': 'BigCommerce - E-com analytics'
                    },
                    'predictive analytics': {
                        'AYX': 'Alteryx - Analytics platform',
                        'DDOG': 'Datadog - Performance analytics',
                        'SPLK': 'Splunk - Operational analytics',
                        'VRNS': 'Varonis - Data analytics',
                        'SPSC': 'SPS Commerce - Supply chain',
                        'PEGA': 'Pegasystems - Decision analytics',
                        'MDLA': 'Medallia - Experience analytics'
                    },
                    'business intelligence': {
                        'CRM': 'Salesforce - CRM analytics',
                        'MSFT': 'Microsoft - Power BI',
                        'PLAN': 'Anaplan - Business planning',
                        'DOMO': 'Domo - BI platform',
                        'SSTI': 'ShotSpotter - Crime analytics',
                        'CCSI': 'Consensus - Decision intel',
                        'MSTR': 'MicroStrategy - BI tools'
                    },
                    'data visualization': {
                        'DATA': 'Tableau - Viz platform',
                        'QLIK': 'Qlik - Visual analytics',
                        'TIBX': 'TIBCO - Visual tools',
                        'LSPD': 'Lightspeed - Retail analytics',
                        'NCNO': 'nCino - Banking analytics',
                        'AVID': 'Avid - Media analytics',
                        'EGHT': '8x8 - Communications analytics'
                    },
                    'real-time analytics': {
                        'DDOG': 'Datadog - Real-time monitoring',
                        'ESTC': 'Elastic - Search analytics',
                        'NEWR': 'New Relic - Performance monitoring',
                        'CLDR': 'Cloudera - Stream analytics',
                        'FSLY': 'Fastly - Edge analytics',
                        'SVMX': 'ServiceMax - Field analytics',
                        'EVBG': 'Everbridge - Critical analytics'
                    }
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
            
            # Reduced delays for rate limiting
            max_retries = 3
            base_delay = 20  # Reduced from 120 to 20 seconds
            
            for attempt in range(max_retries):
                try:
                    # Add shorter delay between requests
                    delay = base_delay * (1.5 ** attempt)  # Reduced exponential factor
                    jitter = random.uniform(0.8, 1.2)  # Reduced jitter range
                    final_delay = delay * jitter
                    
                    logger.info(f"Waiting {final_delay:.1f} seconds before request...")
                    await asyncio.sleep(final_delay)
                    
                    # Set geo to 'US' for United States
                    self.pytrends.build_payload(
                        [term],
                        timeframe=ninety_day_timeframe,
                        geo='US',
                        gprop=''
                    )
                    historical_data = self.pytrends.interest_over_time()
                    
                    if historical_data is not None:
                        logger.info("Request successful")
                        # Reduced successful request delay
                        await asyncio.sleep(random.uniform(5, 10))  # Reduced from 30-60
                        return historical_data
                        
                except Exception as e:
                    if '429' in str(e):
                        if attempt < max_retries - 1:
                            logger.warning(f"Rate limit hit, retrying in {final_delay} seconds...")
                            continue
                    raise
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting trend data: {str(e)}")
            return None

    async def check_stock_trends(self, term, stocks_dict):
        """Check trends for related stocks"""
        stock_results = []
        
        for symbol, description in stocks_dict.items():
            try:
                # Extract company name from description (before the dash)
                company_name = description.split(' - ')[0]
                logger.info(f"Checking trend for {company_name} ({symbol})")
                
                stock_data = await self.get_recent_trend_data(company_name)
                
                if stock_data is not None:
                    stock_value = float(stock_data[company_name].iloc[-1])
                    stock_mean = stock_data[company_name][:-1].mean()
                    stock_std = stock_data[company_name][:-1].std()
                    z_score = (stock_value - stock_mean) / stock_std if stock_std > 0 else 0
                    
                    logger.info(f"Trend stats for {company_name}:")
                    logger.info(f"Current value: {stock_value:.1f}")
                    logger.info(f"Mean: {stock_mean:.1f}")
                    logger.info(f"Z-score: {z_score:.1f}")
                    
                    # Check if company has similar breakout pattern
                    if stock_value >= 75 and z_score >= 1.5:  # Lower thresholds for stocks
                        stock_results.append({
                            'symbol': symbol,
                            'description': description,
                            'company': company_name,
                            'value': stock_value,
                            'z_score': z_score
                        })
                
            except Exception as e:
                logger.error(f"Error checking {company_name} ({symbol}): {str(e)}")
                continue
                
        return stock_results

    async def scan_trends_with_notification(self, search_terms, category):
        """Scan trends and send notifications with stock validation"""
        results = []
        
        for term in search_terms:
            try:
                logger.info(f"\nAnalyzing term: {term}")
                trend_data = await self.get_recent_trend_data(term)
                
                if trend_data is not None:
                    max_value = trend_data[term].max()
                    if max_value >= 90:
                        logger.info(f"Breakout detected for {term}: {max_value}")
                        
                        # Check related stocks
                        logger.info(f"Checking related stocks for {term}...")
                        stock_results = await self.check_stock_trends(
                            term,
                            self.categories[category]['search_terms'][term]
                        )
                        
                        results.append({
                            'term': term,
                            'value': max_value,
                            'category': category,
                            'stocks': stock_results
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
                
                # Reduced delay between categories
                await asyncio.sleep(60)  # Reduced from 300 to 60 seconds
                
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
                        
                        # Add stocks with breakout patterns
                        if r['stocks']:
                            message += "\n💼 Related Companies with High Interest:\n"
                            for stock in r['stocks']:
                                message += f"${stock['symbol']} ({stock['company']})\n"
                                message += f"Interest: {stock['value']:.1f} (Z-score: {stock['z_score']:.1f})\n"
                        else:
                            message += "\n💼 Related Companies (No significant patterns):\n"
                            term_stocks = data['search_terms'][term]
                            for symbol, company in term_stocks.items():
                                company_name = company.split(' - ')[0]
                                message += f"${symbol} ({company_name})\n"
                        
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
        
        # Initialize Telegram first
        logger.info("Initializing Telegram...")
        await scanner.start_app()
        logger.info("Telegram initialized successfully")
        
        # Create scheduler
        scheduler = AsyncIOScheduler()
        
        # Schedule the scan to run at 12:30 UTC daily
        scheduler.add_job(
            scanner.run_continuous_scan,
            CronTrigger(hour=19, minute=30),
            name='daily_scan'
        )
        
        # Start the scheduler
        scheduler.start()
        logger.info("Scheduler started. Waiting for next scan time...")
        
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
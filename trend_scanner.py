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
                        'NVDA': 'NVIDIA - AI chips & computing',
                        'AI': 'C3.ai - Enterprise AI platform',
                        'PLTR': 'Palantir - AI analytics',
                        'BBAI': 'BigBear.ai - AI defense solutions',
                        'SOUN': 'SoundHound - Voice AI',
                        'AGFY': 'Agrify - AI cultivation tech',
                        'MTTR': 'Matterport - AI spatial data'
                    },
                    'machine learning': {
                        'NVDA': 'NVIDIA - ML hardware',
                        'AI': 'C3.ai - ML solutions',
                        'MSFT': 'Microsoft - Azure ML',
                        'THNX': 'THINKsmart - ML fintech',
                        'CELU': 'Celularity - ML biotech',
                        'DMTK': 'DermTech - ML diagnostics',
                        'MARK': 'Remark - ML analytics'
                    },
                    'deep learning': {
                        'NVDA': 'NVIDIA - DL hardware',
                        'AMD': 'AMD - DL processors',
                        'GOOGL': 'Google - DL research',
                        'AITX': 'Artificial Intel Tech - Robotics',
                        'VTSI': 'VirTra - DL training systems',
                        'CGNT': 'Cognyte - DL security',
                        'AUVI': 'Applied UV - DL disinfection'
                    }
                }
            },
            'Cloud Computing': {
                'search_terms': {
                    'cloud computing': {
                        'AMZN': 'Amazon - AWS leader',
                        'MSFT': 'Microsoft - Azure platform',
                        'GOOGL': 'Google - GCP provider',
                        'DOMO': 'Domo - Cloud BI platform',
                        'NCNO': 'nCino - Banking cloud',
                        'SUMO': 'Sumo Logic - Cloud analytics',
                        'CLDR': 'Cloudera - Data cloud'
                    },
                    'cloud security': {
                        'NET': 'Cloudflare - Edge security',
                        'PANW': 'Palo Alto - Security platform',
                        'CRWD': 'CrowdStrike - Endpoint security',
                        'ZS': 'Zscaler - Cloud security',
                        'TENB': 'Tenable - Vulnerability mgmt',
                        'SAIL': 'SailPoint - Identity security',
                        'RDWR': 'Radware - Cloud protection'
                    }
                }
            },
            'Cybersecurity': {
                'search_terms': {
                    'cybersecurity': {
                        'CRWD': 'CrowdStrike - Endpoint protection',
                        'PANW': 'Palo Alto - Network security',
                        'FTNT': 'Fortinet - Security solutions',
                        'VRNS': 'Varonis - Data security',
                        'SCWX': 'SecureWorks - Security services',
                        'CYBE': 'CyberOptics - Inspection tech',
                        'CFBK': 'CF Bankshares - Cyber banking'
                    },
                    'ransomware protection': {
                        'CRWD': 'CrowdStrike - Ransomware defense',
                        'S': 'SentinelOne - AI security',
                        'VRNS': 'Varonis - Data protection',
                        'OSPN': 'OneSpan - Digital security',
                        'CYBR': 'CyberArk - Access security',
                        'SFET': 'Safe-T Group - Zero trust',
                        'CSPI': 'CSP Inc - Security solutions'
                    }
                }
            },
            'Quantum Computing': {
                'search_terms': {
                    'quantum computing': {
                        'IBM': 'IBM - Quantum research leader',
                        'IONQ': 'IonQ - Trapped ion quantum',
                        'RGTI': 'Rigetti - Superconducting quantum',
                        'QUBT': 'Quantum Computing - Software',
                        'ARQQ': 'Arqit Quantum - Encryption',
                        'QTUM': 'Defiance Quantum ETF',
                        'SNPS': 'Synopsys - Quantum design'
                    },
                    'quantum encryption': {
                        'ARQQ': 'Arqit Quantum - Post-quantum crypto',
                        'IBM': 'IBM - Quantum security',
                        'QUBT': 'Quantum Computing - Encryption',
                        'IRNT': 'IronNet - Quantum defense',
                        'QMCO': 'Quantum Corp - Security storage',
                        'CEVA': 'CEVA - Quantum IoT security',
                        'QLYS': 'Qualys - Security platform'
                    }
                }
            },
            'Semiconductor Industry': {
                'search_terms': {
                    'semiconductor shortage': {
                        'TSM': 'TSMC - Leading foundry',
                        'INTC': 'Intel - US chipmaker',
                        'UMC': 'United Micro - Foundry services',
                        'POWI': 'Power Integrations - Power chips',
                        'DIOD': 'Diodes Inc - Components',
                        'IMOS': 'ChipMOS - Testing services',
                        'MPWR': 'Monolithic Power - Power solutions'
                    },
                    'chip manufacturing': {
                        'AMAT': 'Applied Materials - Equipment',
                        'ASML': 'ASML - Lithography systems',
                        'LRCX': 'Lam Research - Fabrication',
                        'ACLS': 'Axcelis - Ion implantation',
                        'UCTT': 'Ultra Clean - Support systems',
                        'CCMP': 'CMC Materials - Materials',
                        'KLIC': 'Kulicke & Soffa - Packaging'
                    }
                }
            },
            'DevOps and SRE': {
                'search_terms': {
                    'DevOps': {
                        'TEAM': 'Atlassian - Collaboration tools',
                        'DDOG': 'Datadog - Monitoring',
                        'PD': 'PagerDuty - Incident response',
                        'ESTC': 'Elastic - Search & analytics',
                        'NEWR': 'New Relic - Observability',
                        'APPN': 'Appian - Low-code platform',
                        'PRGS': 'Progress Software - DevOps tools'
                    },
                    'Site Reliability Engineering': {
                        'DDOG': 'Datadog - SRE platform',
                        'NOW': 'ServiceNow - Workflow automation',
                        'DT': 'Dynatrace - Application monitoring',
                        'FROG': 'JFrog - Artifact management',
                        'RPD': 'Rapid7 - Security operations',
                        'MNTV': 'Momentive - User feedback',
                        'SCWX': 'SecureWorks - Security ops'
                    }
                }
            },
            'Edge Computing': {
                'search_terms': {
                    'edge computing': {
                        'FSLY': 'Fastly - Edge cloud platform',
                        'NET': 'Cloudflare - Edge network',
                        'AKAM': 'Akamai - Edge delivery',
                        'LLNW': 'Limelight - Edge services',
                        'GLBE': 'Global-E - Edge commerce',
                        'EQIX': 'Equinix - Edge data centers',
                        'DCBO': 'Docebo - Edge learning'
                    },
                    'edge AI': {
                        'NVDA': 'NVIDIA - Edge AI chips',
                        'INTC': 'Intel - Edge processors',
                        'XLNX': 'Xilinx - Edge FPGA',
                        'CEVA': 'CEVA - Edge AI IP',
                        'LSCC': 'Lattice Semi - Edge FPGA',
                        'QUIK': 'QuickLogic - Edge solutions',
                        'MTSI': 'MACOM - Edge RF'
                    },
                    'IoT edge': {
                        'CSCO': 'Cisco - Network edge',
                        'DELL': 'Dell - Edge computing',
                        'HPE': 'HP Enterprise - Edge systems',
                        'SWIR': 'Sierra Wireless - IoT modules',
                        'IOTC': 'IoTecha - EV charging',
                        'DGII': 'Digi International - IoT solutions',
                        'ATEN': 'A10 Networks - Edge security'
                    }
                }
            },
            'Robotics and Automation': {
                'search_terms': {
                    'industrial robotics': {
                        'ABB': 'ABB Ltd - Industrial automation',
                        'FANUY': 'Fanuc - Robot manufacturing',
                        'SIEGY': 'Siemens - Factory automation',
                        'STRC': 'Sarcos - Robotic systems',
                        'BKSY': 'BlackSky - Space robotics',
                        'AVAV': 'AeroVironment - Drone systems',
                        'NNDM': 'Nano Dimension - 3D robotics'
                    },
                    'warehouse robotics': {
                        'AMZN': 'Amazon - Warehouse automation',
                        'KION': 'KION Group - Material handling',
                        'THNKY': 'THK Co - Motion control',
                        'BKNG': 'Berkshire Grey - Fulfillment',
                        'STRC': 'Sarcos Technology - Logistics',
                        'RGDX': 'Righthand Robotics - Picking',
                        'VRRM': 'Verra Mobility - Transport'
                    },
                    'medical robotics': {
                        'ISRG': 'Intuitive Surgical - Surgery',
                        'MASI': 'Masimo - Patient monitoring',
                        'STXS': 'Stereotaxis - Surgical robots',
                        'ASXC': 'Asensus - Digital surgery',
                        'RBOT': 'Vicarious - Surgical AI',
                        'TRXC': 'TransEnterix - Minimally invasive',
                        'NVRO': 'Nevro - Neural robotics'
                    }
                }
            },
            'Electric Vehicles': {
                'search_terms': {
                    'electric vehicles': {
                        'TSLA': 'Tesla - EV leader',
                        'NIO': 'NIO - Chinese EVs',
                        'XPEV': 'XPeng - Smart EVs',
                        'FSR': 'Fisker - EV design',
                        'GOEV': 'Canoo - Lifestyle EVs',
                        'WKHS': 'Workhorse - Delivery EVs',
                        'SOLO': 'ElectraMeccanica - Urban EVs'
                    },
                    'EV charging': {
                        'CHPT': 'ChargePoint - Charging network',
                        'BLNK': 'Blink - Charging stations',
                        'EVGO': 'EVgo - Fast charging',
                        'VLTA': 'Volta - Ad-supported charging',
                        'NXGN': 'NexGen - Power solutions',
                        'SBE': 'Switchback - Infrastructure',
                        'ZVIA': 'Zevia - Fleet charging'
                    },
                    'EV battery': {
                        'QS': 'QuantumScape - Solid state',
                        'FREY': 'FREYR - Clean batteries',
                        'MVST': 'Microvast - Fast charging',
                        'SLDP': 'Solid Power - Next-gen',
                        'ENVX': 'Enovix - Silicon batteries',
                        'DCFC': 'Tritium - DC charging',
                        'BATT': 'Battery Tech ETF'
                    }
                }
            },
            'Space Technology': {
                'search_terms': {
                    'space technology': {
                        'SPCE': 'Virgin Galactic - Space tourism',
                        'BA': 'Boeing - Space systems',
                        'LMT': 'Lockheed Martin - Satellites',
                        'MNTS': 'Momentus - Space transport',
                        'ASTR': 'Astra - Launch services',
                        'BKSY': 'BlackSky - Earth imaging',
                        'IRDM': 'Iridium - Satellite comms'
                    },
                    'satellite internet': {
                        'STRL': 'Starlink - Global internet',
                        'VSAT': 'Viasat - Broadband',
                        'MAXR': 'Maxar - Earth intelligence',
                        'ASTS': 'AST SpaceMobile - Space 5G',
                        'GSAT': 'Globalstar - IoT comms',
                        'OSAT': 'Orbsat - Satellite IoT',
                        'GILT': 'Gilat - VSAT networks'
                    },
                    'space exploration': {
                        'RKLB': 'Rocket Lab - Launch provider',
                        'SPCE': 'Virgin Galactic - Tourism',
                        'MNTS': 'Momentus - In-space transport',
                        'VORB': 'Virgin Orbit - Air launch',
                        'NSE': 'NanoAvionics - Small sats',
                        'VACQ': 'Vector - Small rockets',
                        'SRAC': 'Stable Road - Space tech'
                    }
                }
            },
            'Healthcare Technology': {
                'search_terms': {
                    'digital health': {
                        'TDOC': 'Teladoc - Telehealth',
                        'AMWL': 'Amwell - Virtual care',
                        'DOCS': 'Doximity - Medical network',
                        'ONEM': '1Life - Primary care',
                        'PHIC': 'Population Health - Analytics',
                        'PSTX': 'Poseida - Digital therapeutics',
                        'RXRX': 'Recursion - AI drug discovery'
                    },
                    'telemedicine': {
                        'TDOC': 'Teladoc - Virtual care',
                        'AMWL': 'Amwell - Telehealth',
                        'ONEM': '1Life - Digital health',
                        'SGFY': 'Signify - Home health',
                        'OPRX': 'OptimizeRx - Digital health',
                        'WELL': 'Well Health - Virtual care',
                        'TALK': 'Talkspace - Mental health'
                    },
                    'medical AI': {
                        'ISRG': 'Intuitive - Robotic surgery',
                        'NVTA': 'Invitae - Genetic AI',
                        'SDGR': 'Schrödinger - Drug discovery',
                        'RXRX': 'Recursion - AI biotech',
                        'SEER': 'Seer - Proteomics AI',
                        'DMTK': 'DermTech - Skin AI',
                        'OTRK': 'Ontrak - Behavioral AI'
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
                    },
                    'mRNA technology': {
                        'MRNA': 'Moderna - mRNA platform',
                        'BNTX': 'BioNTech - mRNA therapy',
                        'ARCT': 'Arcturus - mRNA delivery',
                        'TBIO': 'Translate Bio - mRNA tech',
                        'PCVX': 'Vaxcyte - Cell-free tech',
                        'CVAC': 'CureVac - mRNA platform',
                        'RWLK': 'ReWalk - RNA therapy'
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
                        'META': 'Meta - VR platform',
                        'RBLX': 'Roblox - Gaming metaverse',
                        'U': 'Unity - 3D platform',
                        'MTTR': 'Matterport - Digital twins',
                        'IMMR': 'Immersion - Haptics',
                        'VRAR': 'VR/AR ETF',
                        'PLTK': 'Playtika - Virtual worlds'
                    },
                    'virtual reality': {
                        'META': 'Meta - Quest VR',
                        'SONY': 'Sony - PSVR',
                        'MSFT': 'Microsoft - Mixed reality',
                        'VUZI': 'Vuzix - AR glasses',
                        'KOPN': 'Kopin - VR displays',
                        'HIMX': 'Himax - VR chips',
                        'IMAX': 'IMAX - VR experiences'
                    },
                    'augmented reality': {
                        'SNAP': 'Snap - AR platform',
                        'MSFT': 'Microsoft - HoloLens',
                        'AAPL': 'Apple - AR development',
                        'VUZI': 'Vuzix - Smart glasses',
                        'WAVX': 'WaveBridge - AR tech',
                        'MAXR': 'Maxar - AR mapping',
                        'KOPN': 'Kopin - AR displays'
                    }
                }
            },
            'Clean Energy Tech': {
                'search_terms': {
                    'renewable energy': {
                        'NEE': 'NextEra - Clean power',
                        'ENPH': 'Enphase - Solar tech',
                        'SEDG': 'SolarEdge - Power opt',
                        'NOVA': 'Sunnova - Solar service',
                        'SPWR': 'SunPower - Solar systems',
                        'CSIQ': 'Canadian Solar',
                        'RUN': 'Sunrun - Solar install'
                    },
                    'green hydrogen': {
                        'PLUG': 'Plug Power - H2 solutions',
                        'BE': 'Bloom Energy - Fuel cells',
                        'FCEL': 'FuelCell - Clean power',
                        'BLDP': 'Ballard - Transport H2',
                        'HTOO': 'Fusion Fuel - Green H2',
                        'HYON': 'HYON - H2 fueling',
                        'HYSR': 'SunHydrogen - Solar H2'
                    },
                    'energy storage': {
                        'TSLA': 'Tesla - Battery tech',
                        'STEM': 'Stem Inc - AI storage',
                        'FLUX': 'Flux Power - Batteries',
                        'EOSE': 'Eos Energy - Storage',
                        'BATT': 'Battery ETF',
                        'FREYR': 'FREYR - Battery cells',
                        'MVST': 'Microvast - Fast charge'
                    }
                }
            },
            'Smart Manufacturing': {
                'search_terms': {
                    'industrial IoT': {
                        'HON': 'Honeywell - IIoT platform',
                        'ROK': 'Rockwell - Factory auto',
                        'PTC': 'PTC Inc - IoT software',
                        'THNX': 'THINKsmart - IoT solutions',
                        'ONTO': 'Onto Innovation - IoT semi',
                        'SWIR': 'Sierra - IoT networking',
                        'IOTC': 'IoTecha - Industrial IoT'
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
                        'PTC': 'PTC - IoT platform',
                        'ADSK': 'Autodesk - Digital twin',
                        'DDOG': 'Datadog - Monitoring',
                        'AZPN': 'Aspen Tech - Asset opt',
                        'VRNS': 'Varonis - Data analysis',
                        'PRCP': 'Perceptron - Measurement',
                        'SWKS': 'Skyworks - Sensors'
                    }
                }
            },
            '5G and Connectivity': {
                'search_terms': {
                    '5G network': {
                        'ERIC': 'Ericsson - Network equipment',
                        'NOK': 'Nokia - 5G infrastructure',
                        'QCOM': 'Qualcomm - 5G chips',
                        'LITE': 'Lumentum - Optical solutions',
                        'INFN': 'Infinera - Network systems',
                        'AVNW': 'Aviat - Wireless transport',
                        'CLFD': 'Clearfield - Fiber connectivity'
                    },
                    '5G technology': {
                        'QCOM': 'Qualcomm - 5G modems',
                        'ERIC': 'Ericsson - 5G solutions',
                        'TMUS': 'T-Mobile - 5G carrier',
                        'COMM': 'CommScope - Network infra',
                        'IDCC': 'InterDigital - 5G patents',
                        'SOL': 'Renesola - 5G power',
                        'PCTI': 'PCTEL - 5G antennas'
                    },
                    'wireless infrastructure': {
                        'AMT': 'American Tower - Cell towers',
                        'CCI': 'Crown Castle - Fiber/towers',
                        'SBAC': 'SBA Comm - Tower operator',
                        'UNIT': 'Uniti Group - Fiber networks',
                        'CMBM': 'Cambium - Wireless solutions',
                        'GILT': 'Gilat - Satellite comms',
                        'ATEX': 'Anterix - Private networks'
                    },
                    'network virtualization': {
                        'VMW': 'VMware - Virtualization',
                        'CSCO': 'Cisco - Network solutions',
                        'RBBN': 'Ribbon - Cloud networking',
                        'ANET': 'Arista - Cloud networking',
                        'FFIV': 'F5 - App delivery',
                        'RVBD': 'Riverbed - WAN opt',
                        'CWAN': 'Clearway - Network services'
                    },
                    '5G applications': {
                        'QCOM': 'Qualcomm - Mobile platforms',
                        'SWKS': 'Skyworks - RF solutions',
                        'KEYS': 'Keysight - Test equipment',
                        'VIAV': 'Viavi - Network test',
                        'MTSI': 'MACOM - RF components',
                        'RESN': 'Resonant - RF filters',
                        'AIRG': 'Airgain - Antenna systems'
                    },
                    'mobile edge computing': {
                        'AKAM': 'Akamai - Edge platform',
                        'FSLY': 'Fastly - Edge compute',
                        'NET': 'Cloudflare - Edge security',
                        'DDOG': 'Datadog - Edge monitoring',
                        'GLBE': 'Global-E - Edge commerce',
                        'LLNW': 'Limelight - Edge delivery',
                        'EQIX': 'Equinix - Edge data centers'
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
            
            # Exponential backoff for rate limiting
            max_retries = 3
            base_delay = 120  # Increased to 120 seconds (2 minutes)
            
            for attempt in range(max_retries):
                try:
                    # Add longer delay between requests
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    jitter = random.uniform(0.5, 1.5)  # Add random jitter
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
                        # Add successful request delay
                        await asyncio.sleep(random.uniform(30, 60))
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
                
                # Add delay between categories
                await asyncio.sleep(300)  # 5 minutes between categories
                
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
        
        # Initialize Telegram first
        logger.info("Initializing Telegram...")
        await scanner.start_app()
        logger.info("Telegram initialized successfully")
        
        # Create scheduler
        scheduler = AsyncIOScheduler()
        
        # Schedule the scan to run at 12:30 UTC daily
        scheduler.add_job(
            scanner.run_continuous_scan,
            CronTrigger(hour=17, minute=15),
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
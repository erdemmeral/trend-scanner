import warnings
warnings.filterwarnings('ignore')

import yfinance as yf
import pandas as pd
from pytrends.request import TrendReq
import time
from datetime import datetime, timedelta

class TrendAnalyzer:
    def __init__(self):
        # Initialize with US geo location
        self.pytrends = TrendReq(hl='en-US', 
                                tz=360,  # Central Time (US)
                                geo='US',  # Set geography to United States
                                timeout=(10,25))
        self.categories = {
            'Quantum Computing': {
                'search_terms': [
                    'quantum computing', 'quantum computer', 'quantum technology',
                    'quantum processor', 'quantum supremacy'
                ],
                'stocks': {
                    'RGTI': 'Quantum Computing - Superconducting Processors',
                    'IONQ': 'Quantum Computing - Trapped Ion Technology',
                    'QUBT': 'Quantum Computing - Software and Algorithms',
                    'IBM': 'Quantum Computing - Hybrid Systems',
                    'GOOGL': 'Quantum Computing - Research and Development'
                }
            },
            'AI and Machine Learning': {
                'search_terms': [
                    'artificial intelligence', 'machine learning', 'AI technology',
                    'neural networks', 'deep learning', 'large language models'
                ],
                'stocks': {
                    'NVDA': 'AI Hardware',
                    'AI': 'AI Software',
                    'PLTR': 'AI Analytics',
                    'META': 'AI Applications'
                }
            },
            'Healthcare Technology': {
                'search_terms': [
                    'digital health', 'telemedicine', 'health tech',
                    'medical AI', 'remote patient monitoring', 'digital therapeutics',
                    'precision medicine', 'genomics technology'
                ],
                'stocks': {
                    'TDOC': 'Telemedicine Platforms',
                    'AMWL': 'Virtual Care Solutions',
                    'DOCS': 'Healthcare Cloud Software',
                    'RXRX': 'AI-Driven Drug Discovery',
                    'DNA': 'Synthetic Biology',
                    'PACB': 'Gene Sequencing Technology',
                    'ILMN': 'Genomics Research Tools',
                    'CRSP': 'Gene Editing Technology',
                    'NVTA': 'Genetic Testing Services',
                    'BEAM': 'Precision Genetic Medicines'
                }
            },
            'Medical Devices Tech': {
                'search_terms': [
                    'wearable medical devices', 'medical robotics',
                    'smart medical devices', 'robotic surgery',
                    'AI diagnostics', 'medical imaging technology'
                ],
                'stocks': {
                    'ISRG': 'Robotic Surgery Systems',
                    'HOLX': 'Diagnostic & Imaging',
                    'INMD': 'Minimally Invasive Devices',
                    'DXCM': 'Continuous Glucose Monitoring',
                    'ABMD': 'Heart Pump Technology',
                    'SWAV': 'Cardiovascular Treatment Tech',
                    'IRTC': 'Cardiac Monitoring Devices',
                    'AXNX': 'Neuromodulation Devices'
                }
            },
            'Digital Healthcare': {
                'search_terms': [
                    'healthcare analytics', 'electronic health records',
                    'healthcare cloud', 'medical data analytics',
                    'healthcare cybersecurity', 'patient data platform'
                ],
                'stocks': {
                    'VEEV': 'Healthcare Cloud Solutions',
                    'CERN': 'Healthcare Information Systems',
                    'CPSI': 'Healthcare IT Services',
                    'HMSY': 'Healthcare Data Analytics',
                    'MDRX': 'Electronic Health Records',
                    'HCAT': 'Healthcare Analytics Platform',
                    'PHR': 'Patient Engagement Solutions',
                    'ONEM': 'Digital Health Membership'
                }
            },
            'Web3 Technology': {
                'search_terms': [
                    'web3', 'decentralized internet', 'blockchain platforms',
                    'NFT technology', 'dao technology', 'decentralized apps'
                ],
                'stocks': {
                    'COIN': 'Crypto Infrastructure',
                    'MARA': 'Blockchain Mining',
                    'RIOT': 'Digital Infrastructure',
                    'MSTR': 'Bitcoin Holdings',
                    'BITF': 'Mining Technology',
                    'HUT': 'Digital Assets',
                    'SI': 'Crypto Banking'
                }
            },
            'Neurotechnology': {
                'search_terms': [
                    'neurotechnology', 'neuroscience', 'neuroengineering',
                    'neuroinformatics', 'neuropharmacology', 'neuropsychology'
                ],
                'stocks': {
                    'NTRN': 'Neurotechnology',
                    'NTRS': 'Neurotechnology',
                    'NTR': 'Neurotechnology',
                    'NTLA': 'Neurotechnology',
                    'NTRK': 'Neurotechnology',
                    'NTRP': 'Neurotechnology',
                    'NTRB': 'Neurotechnology',
                    'NTRC': 'Neurotechnology'
                }
            },
            'Autonomous Systems': {
                'search_terms': [
                    'autonomous systems', 'autonomous vehicles', 'autonomous robots',
                    'autonomous platforms', 'autonomous infrastructure', 'autonomous networks'
                ],
                'stocks': {
                    'TSLA': 'Autonomous Vehicles',
                    'GM': 'Autonomous Vehicles',
                    'F': 'Autonomous Vehicles',
                    'TSM': 'Autonomous Vehicles',
                    'NIO': 'Autonomous Vehicles',
                    'XPEV': 'Autonomous Vehicles',
                    'RCL': 'Autonomous Vehicles',
                    'LAD': 'Autonomous Vehicles'
                }
            },
            'Synthetic Biology': {
                'search_terms': [
                    'synthetic biology', 'synthetic organisms', 'synthetic cells',
                    'synthetic organisms', 'synthetic organisms', 'synthetic organisms'
                ],
                'stocks': {
                    'DNA': 'Synthetic Biology',
                    'PACB': 'Gene Sequencing Technology',
                    'ILMN': 'Genomics Research Tools',
                    'CRSP': 'Gene Editing Technology',
                    'NVTA': 'Genetic Testing Services',
                    'BEAM': 'Precision Genetic Medicines'
                }
            },
            'Edge Computing': {
                'search_terms': [
                    'edge computing', 'edge infrastructure', 'edge networks',
                    'edge platforms', 'edge technologies', 'edge applications'
                ],
                'stocks': {
                    'AMD': 'Edge Computing',
                    'NVDA': 'Edge Computing',
                    'INMD': 'Edge Computing',
                    'DXCM': 'Edge Computing',
                    'ABMD': 'Edge Computing',
                    'SWAV': 'Edge Computing',
                    'IRTC': 'Edge Computing',
                    'AXNX': 'Edge Computing'
                }
            },
            'Advanced Energy Storage': {
                'search_terms': [
                    'advanced energy storage', 'energy storage technologies', 'energy storage systems',
                    'energy storage solutions', 'energy storage applications', 'energy storage innovations'
                ],
                'stocks': {
                    'NIO': 'Advanced Energy Storage',
                    'TSLA': 'Advanced Energy Storage',
                    'GM': 'Advanced Energy Storage',
                    'F': 'Advanced Energy Storage',
                    'TSM': 'Advanced Energy Storage',
                    'NTR': 'Advanced Energy Storage',
                    'NTRP': 'Advanced Energy Storage',
                    'NTRB': 'Advanced Energy Storage'
                }
            },
            'Smart Sensors': {
                'search_terms': [
                    'smart sensors', 'sensor technologies', 'sensor networks',
                    'sensor platforms', 'sensor applications', 'sensor innovations'
                ],
                'stocks': {
                    'ISRG': 'Smart Sensors',
                    'HOLX': 'Smart Sensors',
                    'INMD': 'Smart Sensors',
                    'DXCM': 'Smart Sensors',
                    'ABMD': 'Smart Sensors',
                    'SWAV': 'Smart Sensors',
                    'IRTC': 'Smart Sensors',
                    'AXNX': 'Smart Sensors'
                }
            },
            'Quantum Networking': {
                'search_terms': [
                    'quantum networking', 'quantum communication', 'quantum networks',
                    'quantum communication technologies', 'quantum communication systems', 'quantum communication platforms'
                ],
                'stocks': {
                    'RGTI': 'Quantum Networking',
                    'IONQ': 'Quantum Networking',
                    'QUBT': 'Quantum Networking',
                    'IBM': 'Quantum Networking',
                    'GOOGL': 'Quantum Networking'
                }
            },
            'Digital Biology': {
                'search_terms': [
                    'digital biology', 'biological data', 'biological systems',
                    'biological technologies', 'biological applications', 'biological innovations'
                ],
                'stocks': {
                    'DNA': 'Digital Biology',
                    'PACB': 'Gene Sequencing Technology',
                    'ILMN': 'Genomics Research Tools',
                    'CRSP': 'Gene Editing Technology',
                    'NVTA': 'Genetic Testing Services',
                    'BEAM': 'Precision Genetic Medicines'
                }
            }
        }

    def get_trend_data(self, search_term):
        """
        Get Google Trends data for a search term (US only)
        """
        try:
            # Build payload with US geo restriction
            self.pytrends.build_payload(
                [search_term], 
                timeframe='today 90-d',
                geo='US'  # Explicitly set US geography
            )
            
            # Get interest over time
            trend_data = self.pytrends.interest_over_time()
            
            if trend_data.empty:
                return None
            
            return trend_data[search_term]
        except Exception as e:
            print(f"Error fetching trends for {search_term}: {str(e)}")
            return None

    def analyze_trend_breakout(self, trend_data):
        """
        Analyze if trend has broken out from low interest
        """
        if trend_data is None or len(trend_data) < 30:  # Need at least 30 days of data
            return False, 0, 0, None, None
        
        # Get date ranges
        current_date = trend_data.index[-1]  # Most recent date
        historical_start = current_date - pd.Timedelta(days=90)  # 90 days ago
        historical_end = current_date - pd.Timedelta(days=1)  # Up to yesterday
        
        # Get last 90 days and current data
        historical = trend_data[historical_start:historical_end]
        current = trend_data.iloc[-1]  # Get only the most recent value
        
        # Calculate averages
        historical_avg = historical.mean()
        current_value = current  # Single value, not an average
        
        # Check if historical interest was low and current is high
        is_breakout = historical_avg <= 25 and current_value >= 90
        
        return is_breakout, historical_avg, current_value, current_date.strftime('%Y-%m-%d'), current_date.strftime('%Y-%m-%d')

    def find_emerging_trends(self):
        """
        Find tech categories with emerging trends
        """
        emerging_categories = []
        
        for category, data in self.categories.items():
            print(f"\n{'-'*50}")
            print(f"Analyzing trends for {category}...")
            category_breakout = False
            
            # Process search terms in batches of 5
            search_terms = data['search_terms']
            for i in range(0, len(search_terms), 5):
                batch = search_terms[i:i+5]
                
                # Process each term in the current batch
                for term in batch:
                    print(f"\nChecking '{term}'...")
                    
                    # Get trend data
                    trend_data = self.get_trend_data(term)
                    if trend_data is None:
                        print("No data available")
                        continue
                    
                    # Analyze trend
                    is_breakout, hist_avg, current, current_start, current_end = self.analyze_trend_breakout(trend_data)
                    
                    # Print detailed scores
                    print(f"90-day average: {hist_avg:.1f}")
                    print(f"Current score: {current:.1f} (Date: {current_start})")
                    
                    if is_breakout:
                        print(f"BREAKOUT DETECTED! ({hist_avg:.1f} → {current:.1f})")
                        category_breakout = True
                    else:
                        if hist_avg <= 25:
                            print(f"Low interest but no breakout yet (needs current > 90)")
                        elif current >= 90:
                            print(f"High current interest but historical too high (needs 90d avg < 25)")
                        else:
                            print("No significant trend pattern")
                    
                    # Small sleep between terms
                    time.sleep(1)
                
                # Longer sleep after each batch of 5
                if i + 5 < len(search_terms):
                    print("\nPausing for rate limit...")
                    time.sleep(10)  # 10 second pause between batches
            
            if category_breakout:
                emerging_categories.append({
                    'category': category,
                    'stocks': data['stocks']
                })
        
        return emerging_categories

    def get_categories(self):
        """
        Get the tech categories
        """
        return self.categories

def main():
    print("\nTech Trend Analyzer")
    print("Analyzing Google Trends data for emerging tech categories...")
    print("\nCriteria:")
    print("- 90-day average must be below 25")
    print("- Current interest must be above 90")
    print("-" * 50)
    
    analyzer = TrendAnalyzer()
    emerging = analyzer.find_emerging_trends()
    
    if not emerging:
        print("\nNo emerging trends found meeting the criteria")
        print("(Requires: 12 months below 25% interest followed by 90%+ current interest)")
        return
    
    print("\nEMERGING TECH CATEGORIES:")
    for category in emerging:
        print(f"\n{category['category'].upper()}:")
        print("Related Stocks:")
        for symbol, subcategory in category['stocks'].items():
            try:
                stock = yf.Ticker(symbol)
                name = stock.info.get('longName', symbol)
                print(f"- {symbol} ({name})")
                print(f"  Subcategory: {subcategory}")
            except:
                print(f"- {symbol}: {subcategory}")

if __name__ == "__main__":
    main() 
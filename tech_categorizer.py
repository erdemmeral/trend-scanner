import yfinance as yf
import pandas as pd
import requests
from bs4 import BeautifulSoup

class TechCategorizer:
    def __init__(self):
        # Detailed tech subcategories mapping
        self.tech_subcategories = {
            'Quantum Computing': {
                'keywords': [
                    'quantum', 'qubit', 'quantum computing', 'quantum technology',
                    'quantum processor', 'quantum software', 'quantum encryption'
                ],
                'companies': {
                    'RGTI': 'Quantum Computing - Superconducting Processors',
                    'IONQ': 'Quantum Computing - Trapped Ion Technology',
                    'QUBT': 'Quantum Computing - Software and Algorithms',
                    'IBM': 'Quantum Computing - Hybrid Systems',
                    'GOOGL': 'Quantum Computing - Research and Development'
                }
            },
            'Artificial Intelligence': {
                'keywords': [
                    'artificial intelligence', 'machine learning', 'deep learning',
                    'neural networks', 'AI chips', 'AI software', 'language models'
                ],
                'companies': {
                    'NVDA': 'AI - Hardware and Computing Infrastructure',
                    'AI': 'AI - Enterprise Software Solutions',
                    'PLTR': 'AI - Data Analytics and Integration',
                    'META': 'AI - Large Language Models and Applications'
                }
            },
            'Semiconductor': {
                'keywords': [
                    'semiconductor', 'chips', 'integrated circuits', 'processor',
                    'foundry', 'chip design', 'microprocessor'
                ],
                'companies': {
                    'NVDA': 'Semiconductors - GPU and AI Chips',
                    'AMD': 'Semiconductors - CPUs and GPUs',
                    'INTC': 'Semiconductors - Integrated Manufacturing',
                    'TSM': 'Semiconductors - Pure-play Foundry',
                    'AMAT': 'Semiconductors - Manufacturing Equipment'
                }
            },
            # Add more categories as needed
        }
        
        # Additional data sources
        self.data_sources = {
            'finviz': 'https://finviz.com/quote.ashx?t={}',
            'yahoo': 'https://finance.yahoo.com/quote/{}'
        }

    def get_detailed_category(self, symbol):
        """
        Get detailed technology subcategory for a given stock symbol
        """
        try:
            # Get stock info from yfinance
            stock = yf.Ticker(symbol)
            info = stock.info
            
            # Basic info
            sector = info.get('sector', '')
            industry = info.get('industry', '')
            business_summary = info.get('longBusinessSummary', '').lower()
            
            # If not a tech stock, return early
            if sector != 'Technology' and 'technology' not in industry.lower():
                return {
                    'symbol': symbol,
                    'name': info.get('longName', ''),
                    'is_tech': False,
                    'sector': sector,
                    'industry': industry,
                    'category': 'Non-Technology'
                }
            
            # Check if company is directly mapped
            for category, data in self.tech_subcategories.items():
                if symbol in data['companies']:
                    return {
                        'symbol': symbol,
                        'name': info.get('longName', ''),
                        'is_tech': True,
                        'sector': sector,
                        'industry': industry,
                        'category': category,
                        'subcategory': data['companies'][symbol]
                    }
            
            # Analyze business summary for keywords
            matched_categories = []
            for category, data in self.tech_subcategories.items():
                for keyword in data['keywords']:
                    if keyword in business_summary:
                        matched_categories.append(category)
            
            if matched_categories:
                primary_category = max(set(matched_categories), key=matched_categories.count)
                return {
                    'symbol': symbol,
                    'name': info.get('longName', ''),
                    'is_tech': True,
                    'sector': sector,
                    'industry': industry,
                    'category': primary_category,
                    'subcategory': f"{primary_category} - General"
                }
            
            # If no specific category found, return the industry
            return {
                'symbol': symbol,
                'name': info.get('longName', ''),
                'is_tech': True,
                'sector': sector,
                'industry': industry,
                'category': 'Technology - Other',
                'subcategory': industry
            }
            
        except Exception as e:
            return {
                'symbol': symbol,
                'error': str(e),
                'category': 'Unknown'
            }

    def get_peer_companies(self, symbol):
        """
        Get peer companies in the same subcategory
        """
        category_info = self.get_detailed_category(symbol)
        if 'category' not in category_info:
            return []
        
        category = category_info['category']
        if category in self.tech_subcategories:
            return list(self.tech_subcategories[category]['companies'].keys())
        return []

def main():
    categorizer = TechCategorizer()
    
    print("\nTechnology Stock Categorizer")
    print("Enter stock symbols (one per line)")
    print("Press Enter twice when done, or type 'quit' to exit")
    
    while True:
        symbols = []
        while True:
            symbol = input().upper()
            if symbol == 'QUIT':
                return
            if symbol == '':
                break
            symbols.append(symbol)
        
        if not symbols:
            continue
            
        for symbol in symbols:
            print(f"\nAnalyzing {symbol}...")
            category_info = categorizer.get_detailed_category(symbol)
            
            if 'error' in category_info:
                print(f"Error analyzing {symbol}: {category_info['error']}")
                continue
                
            print(f"Company: {category_info['name']}")
            print(f"Sector: {category_info['sector']}")
            print(f"Industry: {category_info['industry']}")
            print(f"Technology Category: {category_info['category']}")
            print(f"Specific Subcategory: {category_info['subcategory']}")
            
            if category_info['is_tech']:
                peers = categorizer.get_peer_companies(symbol)
                if peers:
                    print(f"Peer Companies: {', '.join(peers)}")

if __name__ == "__main__":
    main() 
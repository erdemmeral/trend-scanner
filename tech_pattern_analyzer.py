import yfinance as yf
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import requests
from bs4 import BeautifulSoup

class TechCategorizer:
    # (Keep the existing TechCategorizer class as is)
    # ... (previous code remains the same until get_peer_companies method)

    def get_all_subcategory_stocks(self, symbol):
        """
        Get all stocks in the same subcategory, including ones not explicitly listed
        """
        category_info = self.get_detailed_category(symbol)
        if not category_info.get('is_tech'):
            return []

        category = category_info['category']
        subcategory = category_info['subcategory']
        
        # Start with known peers
        stocks = set(self.get_peer_companies(symbol))
        
        # Add the input symbol
        stocks.add(symbol)
        
        try:
            # Search for additional companies using Finviz industry/sector
            url = self.data_sources['finviz'].format(symbol)
            response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract industry peers from Finviz
            peer_elements = soup.find_all('a', {'class': 'tab-link'})
            for element in peer_elements:
                if element.get('href', '').startswith('/screener.ashx?'):
                    stocks.add(element.text)
        except:
            pass
        
        return list(stocks)

def analyze_stock_patterns(symbols, lookback_days=20, min_spike_percent=35):
    """
    Analyze stock patterns for a list of symbols
    """
    patterns = []
    for symbol in symbols:
        try:
            stock = yf.Ticker(symbol)
            df = stock.history(period='6mo')
            
            # Calculate price changes
            df['Price_Change'] = df['Close'].pct_change()
            
            # Identify spikes
            spike_dates = df[df['Price_Change'] > min_spike_percent/100].index
            
            for spike_date in spike_dates:
                if spike_date - pd.Timedelta(days=lookback_days) in df.index:
                    pre_spike_data = df.loc[spike_date - pd.Timedelta(days=lookback_days):spike_date]
                    patterns.append({
                        'symbol': symbol,
                        'date': spike_date,
                        'magnitude': df.loc[spike_date, 'Price_Change'] * 100,
                        'pattern': extract_pattern_features(pre_spike_data)
                    })
        except Exception as e:
            print(f"Error analyzing {symbol}: {str(e)}")
            continue
    
    return patterns

def find_similar_patterns(reference_patterns, candidate_stocks, min_similarity=0.5):
    """
    Find stocks with similar patterns to the reference patterns
    """
    potential_candidates = {}
    
    for stock in candidate_stocks:
        try:
            # Get current pattern
            current_data = yf.Ticker(stock).history(period='1mo')
            if len(current_data) < 20:
                continue
                
            current_pattern = extract_pattern_features(current_data.tail(20))
            
            # Find best match among reference patterns
            best_similarity = 0
            for ref_pattern in reference_patterns:
                similarity = calculate_pattern_similarity(ref_pattern['pattern'], current_pattern)
                best_similarity = max(best_similarity, similarity)
            
            if best_similarity > min_similarity:
                potential_candidates[stock] = best_similarity
        except:
            continue
    
    return potential_candidates

def main():
    categorizer = TechCategorizer()
    
    print("\nTechnology Stock Pattern Analyzer")
    print("Enter reference stock symbols (one per line)")
    print("Press Enter twice when done, or type 'quit' to exit")
    
    while True:
        reference_stocks = []
        while True:
            symbol = input().upper()
            if symbol == 'QUIT':
                return
            if symbol == '':
                break
            reference_stocks.append(symbol)
        
        if not reference_stocks:
            continue
        
        # Step 1: Analyze categories and get peer stocks
        all_candidates = set()
        for symbol in reference_stocks:
            print(f"\nAnalyzing category for {symbol}...")
            category_info = categorizer.get_detailed_category(symbol)
            
            if 'error' in category_info:
                print(f"Error: {category_info['error']}")
                continue
                
            print(f"Company: {category_info['name']}")
            print(f"Technology Category: {category_info['category']}")
            print(f"Specific Subcategory: {category_info['subcategory']}")
            
            # Get all stocks in same subcategory
            peers = categorizer.get_all_subcategory_stocks(symbol)
            all_candidates.update(peers)
            print(f"Found {len(peers)} peer companies in same subcategory")
        
        # Remove reference stocks from candidates
        all_candidates = all_candidates - set(reference_stocks)
        
        # Step 2: Analyze patterns
        print("\nAnalyzing spike patterns in reference stocks...")
        reference_patterns = analyze_stock_patterns(reference_stocks)
        
        if not reference_patterns:
            print("No significant spikes found in reference stocks")
            continue
        
        print(f"\nFound {len(reference_patterns)} spike patterns")
        for pattern in reference_patterns:
            print(f"{pattern['symbol']}: +{pattern['magnitude']:.1f}% on {pattern['date'].strftime('%Y-%m-%d')}")
        
        # Step 3: Find similar patterns
        print(f"\nScanning {len(all_candidates)} peer stocks for similar patterns...")
        candidates = find_similar_patterns(reference_patterns, all_candidates)
        
        if not candidates:
            print("No stocks found with similar patterns")
            continue
        
        # Display results
        print("\nTop matching stocks:")
        sorted_candidates = sorted(candidates.items(), key=lambda x: x[1], reverse=True)[:5]
        for stock, similarity in sorted_candidates:
            try:
                info = yf.Ticker(stock).info
                print(f"\n{stock} ({info.get('longName', '')})")
                print(f"Industry: {info.get('industry', 'Unknown')}")
                print(f"Pattern match: {similarity*100:.1f}%")
            except:
                print(f"\n{stock}: {similarity*100:.1f}%")
        
        print("\nRemember: Past patterns don't guarantee future performance")
        print("\nPress Enter to analyze new stocks, or type 'quit' to exit")

if __name__ == "__main__":
    main() 
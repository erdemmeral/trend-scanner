import yfinance as yf
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

def get_stock_data(symbol, period='6mo'):
    """
    Fetch stock data using yfinance
    """
    try:
        stock = yf.Ticker(symbol)
        df = stock.history(period=period)
        return df, stock.info['sector']
    except:
        return None, None

def calculate_rsi(prices, period=14):
    """
    Calculate Relative Strength Index without ta-lib
    """
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_macd(prices, fast=12, slow=26, signal=9):
    """
    Calculate MACD without ta-lib
    """
    exp1 = prices.ewm(span=fast, adjust=False).mean()
    exp2 = prices.ewm(span=slow, adjust=False).mean()
    macd = exp1 - exp2
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    return macd, signal_line

def calculate_bollinger_bands(prices, window=20):
    """
    Calculate Bollinger Bands without ta-lib
    """
    sma = prices.rolling(window=window).mean()
    std = prices.rolling(window=window).std()
    upper_band = sma + (std * 2)
    lower_band = sma - (std * 2)
    return upper_band, sma, lower_band

def analyze_pre_spike_pattern(df, days_before=20):
    """
    Analyze the pattern that occurred before spikes in the last 6 months
    """
    df['Price_Change'] = df['Close'].pct_change()
    df['Volume_Change'] = df['Volume'].pct_change()
    
    # Calculate technical indicators
    df['RSI'] = calculate_rsi(df['Close'])
    df['MACD'], df['MACD_Signal'] = calculate_macd(df['Close'])
    df['BB_Upper'], df['BB_Middle'], df['BB_Lower'] = calculate_bollinger_bands(df['Close'])
    
    # Identify significant spikes (35% or more increase)
    df['Is_Spike'] = df['Price_Change'] > 0.35  # Changed from 0.15 to 0.35
    
    # Find all spikes in the period
    spike_patterns = []
    for spike_date in df[df['Is_Spike']].index:
        if spike_date - pd.Timedelta(days=days_before) in df.index:
            pre_spike_data = df.loc[spike_date - pd.Timedelta(days=days_before):spike_date]
            spike_patterns.append({
                'date': spike_date,
                'magnitude': df.loc[spike_date, 'Price_Change'] * 100,  # Convert to percentage
                'pre_spike_pattern': extract_pattern_features(pre_spike_data)
            })
    
    # Sort by date (most recent first)
    spike_patterns.sort(key=lambda x: x['date'], reverse=True)
    return spike_patterns

def extract_pattern_features(df):
    """
    Extract key features from pre-spike pattern
    """
    return {
        'price_volatility': df['Price_Change'].std(),
        'volume_trend': df['Volume'].pct_change().mean(),
        'rsi_avg': df['RSI'].mean(),
        'macd_cross': (df['MACD'] > df['MACD_Signal']).sum() / len(df),
        'bb_position': ((df['Close'] - df['BB_Lower']) / (df['BB_Upper'] - df['BB_Lower'])).mean(),
        'volume_price_correlation': df['Volume_Change'].corr(df['Price_Change'])
    }

def calculate_pattern_similarity(pattern1, pattern2):
    """
    Calculate similarity between two patterns
    """
    features = ['price_volatility', 'volume_trend', 'rsi_avg', 'macd_cross', 
               'bb_position', 'volume_price_correlation']
    
    similarity_scores = []
    for feature in features:
        v1, v2 = pattern1[feature], pattern2[feature]
        if v1 != 0 or v2 != 0:
            similarity = 1 - abs(v1 - v2) / max(abs(v1), abs(v2))
        else:
            similarity = 1
        similarity_scores.append(similarity)
    
    # Weight the features
    weights = {
        'price_volatility': 0.25,
        'volume_trend': 0.2,
        'rsi_avg': 0.15,
        'macd_cross': 0.15,
        'bb_position': 0.15,
        'volume_price_correlation': 0.1
    }
    
    return sum(s * weights[f] for s, f in zip(similarity_scores, features))

def get_sector_peers(symbol):
    """
    Get peer companies in the same sector/industry
    """
    try:
        stock = yf.Ticker(symbol)
        info = stock.info
        sector = info.get('sector', '')
        industry = info.get('industry', '')
        
        # Get similar companies
        peers = set(stock.info.get('recommendationKey', []))  # Direct peers
        
        # Search for companies in same industry
        if industry:
            industry_search = yf.Tickers(f"{{industry}}")
            for ticker in industry_search.tickers:
                if hasattr(ticker, 'info') and ticker.info.get('industry') == industry:
                    peers.add(ticker.symbol)
        
        return list(peers), sector, industry
    except:
        return [], '', ''

def main():
    print("\nStock Pattern Analyzer (Last 6 Months)")
    print("Enter stock symbols that had significant spikes (>35%) (one per line)")
    print("Press Enter twice when done, or type 'quit' to exit")
    
    while True:
        reference_stocks = []
        print("\nEnter reference stock symbols:")
        while True:
            symbol = input().upper()
            if symbol == 'QUIT':
                return
            if symbol == '':
                break
            if symbol not in reference_stocks:
                reference_stocks.append(symbol)
        
        if not reference_stocks:
            continue

        print(f"\nAnalyzing {len(reference_stocks)} stocks: {', '.join(reference_stocks)}...")
        
        # Collect patterns and build sector/industry focused stock list
        all_patterns = []
        scan_stocks = set()
        sectors = set()
        industries = set()

        for symbol in reference_stocks:
            # Get patterns
            target_data, _ = get_stock_data(symbol)
            if target_data is None:
                print(f"Warning: Could not fetch data for {symbol}")
                continue
                
            patterns = analyze_pre_spike_pattern(target_data)
            if patterns:
                print(f"\nFound {len(patterns)} spike(s) in {symbol}'s last 6 months:")
                for pattern in patterns:
                    spike_date = pattern['date'].strftime('%Y-%m-%d')
                    spike_magnitude = pattern['magnitude']
                    print(f"  {spike_date}: +{spike_magnitude:.1f}%")
                all_patterns.extend(patterns)
            else:
                print(f"No significant spikes found in {symbol}'s last 6 months")
            
            # Get peer companies
            peers, sector, industry = get_sector_peers(symbol)
            if sector:
                sectors.add(sector)
            if industry:
                industries.add(industry)
            scan_stocks.update(peers)

        if not all_patterns:
            print("No usable spike patterns found in any of the reference stocks")
            continue

        print(f"\nAnalyzing patterns from {len(all_patterns)} spikes")
        print(f"Sectors: {', '.join(sectors)}")
        print(f"Industries: {', '.join(industries)}")
        print(f"Number of peer stocks to scan: {len(scan_stocks)}")
        print("\nScanning stocks for similar pre-spike patterns...")

        # Scan for stocks with similar patterns
        potential_candidates = {}

        for stock in scan_stocks:
            if stock in reference_stocks:
                continue

            stock_data, _ = get_stock_data(stock)
            if stock_data is not None:
                try:
                    # Calculate indicators for current stock data
                    stock_data['Price_Change'] = stock_data['Close'].pct_change()
                    stock_data['Volume_Change'] = stock_data['Volume'].pct_change()
                    stock_data['RSI'] = calculate_rsi(stock_data['Close'])
                    stock_data['MACD'], stock_data['MACD_Signal'] = calculate_macd(stock_data['Close'])
                    stock_data['BB_Upper'], stock_data['BB_Middle'], stock_data['BB_Lower'] = calculate_bollinger_bands(stock_data['Close'])
                    
                    # Get current pattern
                    current_pattern = extract_pattern_features(stock_data.tail(20))
                    
                    # Find best match among all reference patterns
                    best_similarity = 0
                    for pattern in all_patterns:
                        similarity = calculate_pattern_similarity(pattern['pre_spike_pattern'], current_pattern)
                        best_similarity = max(best_similarity, similarity)
                    
                    if best_similarity > 0.5:  # Threshold of 50%
                        potential_candidates[stock] = best_similarity
                except Exception as e:
                    print(f"Warning: Error processing {stock}: {str(e)}")

        if not potential_candidates:
            print("No stocks found with similar patterns (minimum 50% similarity required)")
            continue

        # Sort and display results
        sorted_candidates = sorted(potential_candidates.items(), key=lambda x: x[1], reverse=True)[:5]
        print(f"\nTop 5 stocks showing similar pre-spike patterns to reference stocks:")
        for stock, similarity in sorted_candidates:
            try:
                ticker = yf.Ticker(stock)
                company_name = ticker.info.get('longName', stock)
                industry = ticker.info.get('industry', 'Unknown Industry')
                similarity_pct = similarity * 100
                print(f"{stock} ({company_name})")
                print(f"Industry: {industry}")
                print(f"Pattern match: {similarity_pct:.1f}%\n")
            except:
                print(f"{stock}: Pattern match: {similarity_pct:.1f}%")
        
        print("\nNote: These stocks currently show similar patterns to what the reference stocks showed before their spikes")
        print("Remember: Past patterns don't guarantee future performance")
        print("\nPress Enter to analyze new stocks, or type 'quit' to exit")

if __name__ == "__main__":
    main() 
import warnings
warnings.filterwarnings('ignore')

import yfinance as yf
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import requests
from bs4 import BeautifulSoup
from pytrends.request import TrendReq
import time
from datetime import datetime, timedelta

# Import our existing classes
from tech_categorizer import TechCategorizer
from tech_trend_analyzer import TrendAnalyzer

def run_categorizer():
    """
    Run the tech categorization analysis
    """
    categorizer = TechCategorizer()
    
    print("\nTechnology Stock Categorizer")
    print("Enter stock symbols (one per line)")
    print("Press Enter twice when done, or type 'back' to return to main menu")
    
    while True:
        symbols = []
        while True:
            symbol = input().upper()
            if symbol == 'BACK':
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
        
        print("\nPress Enter to analyze more stocks, or type 'back' to return to main menu")

def run_trend_analyzer():
    """
    Run the Google Trends analysis
    """
    print("\nTech Trend Analyzer")
    print("Analyzing Google Trends data for emerging tech categories...")
    
    analyzer = TrendAnalyzer()
    emerging = analyzer.find_emerging_trends()
    
    if not emerging:
        print("\nNo emerging trends found meeting the criteria")
        print("(Requires: 6 months below 25% interest followed by 95%+ current interest)")
        return
    
    print("\nEmerging Tech Categories:")
    for category in emerging:
        print(f"\n{category['category']}:")
        print("Related Stocks:")
        for symbol, subcategory in category['stocks'].items():
            try:
                stock = yf.Ticker(symbol)
                name = stock.info.get('longName', symbol)
                print(f"- {symbol} ({name})")
                print(f"  Subcategory: {subcategory}")
            except:
                print(f"- {symbol}: {subcategory}")
    
    input("\nPress Enter to return to main menu...")

def display_menu():
    """
    Display the main menu
    """
    print("\n=== Tech Stock Analysis Tool ===")
    print("1. Analyze Stock Categories")
    print("2. Find Emerging Tech Trends")
    print("3. Exit")
    return input("Select an option (1-3): ")

def main():
    print("Welcome to Tech Stock Analysis Tool")
    
    while True:
        choice = display_menu()
        
        if choice == '1':
            run_categorizer()
        elif choice == '2':
            run_trend_analyzer()
        elif choice == '3':
            print("\nThank you for using Tech Stock Analysis Tool!")
            break
        else:
            print("\nInvalid option. Please select 1-3.")

if __name__ == "__main__":
    main() 
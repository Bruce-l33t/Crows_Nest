import sys
import pandas as pd
import requests
import time
from datetime import datetime
from dataclasses import dataclass
import logging
import os
sys.path.append('..')
import dontshare as d
import config  # Add this import

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class TraderMetrics:
    """Stores processed trader metrics"""
    address: str
    pnl: float
    volume: float
    trade_count: int
    efficiency_score: float
    trading_score: float
    top_holdings: list = None

class WalletAnalyzer:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {
            "X-API-KEY": api_key,
            "accept": "application/json",
            "x-chain": "solana"
        }
        self.last_call_time = 0
        self.min_call_interval = 0.5  # 500ms between calls
        self.holdings_cache = {}  # Cache for wallet holdings
        self.backoff_time = self.min_call_interval
        self.max_retries = 3

    def _rate_limit(self):
        """Implement rate limiting between API calls"""
        current_time = time.time()
        time_since_last_call = current_time - self.last_call_time
        if time_since_last_call < self.backoff_time:
            time.sleep(self.backoff_time - time_since_last_call)
        self.last_call_time = time.time()

    def _handle_rate_limit(self):
        """Handle rate limit by increasing backoff time"""
        self.backoff_time *= 2
        logger.warning(f"Rate limited. Increasing backoff to {self.backoff_time}s")
        time.sleep(self.backoff_time)

    def _reset_backoff(self):
        """Reset backoff time after successful calls"""
        self.backoff_time = self.min_call_interval

    def get_top_traders(self, limit: int = 2500) -> list:
        """Fetch top traders from Birdeye API"""
        url = "https://public-api.birdeye.so/trader/gainers-losers"
        traders = []
        
        for offset in range(0, limit, 10):
            retry_count = 0
            while retry_count < self.max_retries:
                try:
                    self._rate_limit()
                    params = {
                        "type": "1W",
                        "offset": offset,
                        "limit": 10
                    }
                    
                    response = requests.get(url, headers=self.headers, params=params)
                    if response.status_code == 429:
                        self._handle_rate_limit()
                        retry_count += 1
                        continue
                    
                    if response.status_code != 200:
                        logger.error(f"API error: {response.status_code}")
                        break
                        
                    data = response.json()
                    if not data.get("success"):
                        logger.error("API request failed")
                        break
                    
                    batch = data.get("data", {}).get("items", [])
                    if not batch:
                        break
                        
                    traders.extend(batch)
                    logger.info(f"Fetched {len(traders)} traders so far...")
                    self._reset_backoff()
                    break
                    
                except Exception as e:
                    logger.error(f"Error fetching traders: {e}")
                    retry_count += 1
                    time.sleep(1)
            
            if retry_count == self.max_retries:
                logger.error(f"Max retries reached for offset {offset}")
                
        return traders

    def get_wallet_holdings(self, wallet: str) -> list:
        """Get holdings for a wallet with value above threshold"""
        # Check cache first
        if wallet in self.holdings_cache:
            return self.holdings_cache[wallet]

        retry_count = 0
        while retry_count < self.max_retries:
            try:
                self._rate_limit()
                url = "https://public-api.birdeye.so/v1/wallet/token_list"
                params = {"wallet": wallet}
                
                logger.info(f"Fetching holdings for wallet: {wallet}")
                response = requests.get(url, headers=self.headers, params=params, timeout=10)
                
                if response.status_code == 429:
                    self._handle_rate_limit()
                    retry_count += 1
                    continue
                
                if response.status_code != 200:
                    logger.error(f"API error for wallet {wallet}: {response.status_code}")
                    break
                    
                data = response.json()
                if not data.get("success"):
                    logger.error(f"API request failed for wallet {wallet}")
                    break
                
                # Get items array from the correct path
                holdings = data.get("data", {}).get("items", [])
                if not holdings:
                    logger.info(f"No holdings found for wallet {wallet}")
                    self.holdings_cache[wallet] = []
                    return []
                
                # Filter and sort holdings by value
                significant_holdings = [
                    h for h in holdings 
                    if h.get("valueUsd", 0) >= config.SIGNIFICANT_HOLDING_THRESHOLD 
                    and h.get("symbol") and h.get("valueUsd")
                    and h.get("symbol") not in ["USDC", "SOL"]
                ]
                significant_holdings.sort(key=lambda x: float(x.get("valueUsd", 0)), reverse=True)
                
                # Format holdings with more decimals
                formatted_holdings = []
                for holding in significant_holdings:
                    value_millions = float(holding.get("valueUsd", 0)) / 1_000_000
                    formatted = f"{holding.get('symbol')} {value_millions:.2f}m"
                    formatted_holdings.append(formatted)
                
                logger.info(f"Found {len(formatted_holdings)} significant holdings for {wallet}")
                
                # Cache the results
                self.holdings_cache[wallet] = formatted_holdings
                self._reset_backoff()
                return formatted_holdings
                
            except requests.exceptions.Timeout:
                logger.error(f"Timeout fetching holdings for wallet {wallet}")
                retry_count += 1
                time.sleep(1)
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error fetching holdings for wallet {wallet}: {e}")
                retry_count += 1
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error fetching holdings for {wallet}: {e}")
                retry_count += 1
                time.sleep(1)
        
        if retry_count == self.max_retries:
            logger.error(f"Max retries reached for wallet {wallet}")
            return []

    def calculate_trading_score(self, trader_data: dict) -> TraderMetrics:
        """Calculate trading metrics and score"""
        pnl = float(trader_data.get('pnl', 0))
        volume = float(trader_data.get('volume', 0))
        trade_count = int(trader_data.get('trade_count', 0))
        
        if trade_count > config.BOT_TRANSACTION_THRESHOLD or trade_count == 0:
            return None
            
        # Calculate efficiency score
        efficiency = (pnl / volume * 100) if volume > 0 else 0
        efficiency_score = min(100, (pnl / volume) * 200) if volume > 0 else 0
        
        # Calculate trading score components
        pnl_score = min(100, pnl / 100000)
        profit_per_trade = pnl / trade_count
        activity_score = min(100, max(0, (profit_per_trade - 1000) / 19000 * 100))
        
        # Final trading score
        trading_score = (
            efficiency_score * 0.33 +
            pnl_score * 0.33 +
            activity_score * 0.34
        )
        
        return TraderMetrics(
            address=trader_data.get('address'),
            pnl=pnl,
            volume=volume,
            trade_count=trade_count,
            efficiency_score=efficiency_score,
            trading_score=trading_score
        )

def main():
    # Ensure output directory exists
    os.makedirs("output", exist_ok=True)
    
    analyzer = WalletAnalyzer(d.birdeye_api_key)
    
    # 1. Get top 2.5k traders
    logger.info(f"Fetching top {config.TOP_GAINERS_LIMIT} traders...")
    traders = analyzer.get_top_traders(limit=config.TOP_GAINERS_LIMIT)
    
    # 2. Process and filter traders, removing duplicates and bots
    logger.info("Processing trader metrics...")
    seen_addresses = set()
    processed_traders = []
    for trader in traders:
        metrics = analyzer.calculate_trading_score(trader)
        if metrics and metrics.address not in seen_addresses:  # Only add if not seen before
            seen_addresses.add(metrics.address)
            processed_traders.append(metrics)
    
    # 3. Sort by trading score
    processed_traders.sort(key=lambda x: x.trading_score, reverse=True)
    logger.info(f"Found {len(processed_traders)} unique qualified traders after filtering")
    
    # 4. Get holdings for all qualified traders
    logger.info("Fetching holdings for all qualified traders...")
    for i, trader in enumerate(processed_traders, 1):
        logger.info(f"Processing trader {i}/{len(processed_traders)}")
        try:
            trader.top_holdings = analyzer.get_wallet_holdings(trader.address)
        except Exception as e:
            logger.error(f"Error processing trader {trader.address}: {e}")
            continue
    
    # 5. Filter for traders with at least 2 significant token positions
    qualified_traders = [
        trader for trader in processed_traders
        if trader.top_holdings and len(trader.top_holdings) >= config.MIN_SIGNIFICANT_HOLDINGS
    ]
    logger.info(f"Found {len(qualified_traders)} traders with {config.MIN_SIGNIFICANT_HOLDINGS}+ significant token positions")
    
    # 6. Save final results with fixed filename
    output_file = "output/wallet_holdings.csv"
    
    # Prepare data for CSV
    data = []
    for trader in qualified_traders:
        row = {
            'Wallet': trader.address,
            'PnL': trader.pnl,
            'Volume': trader.volume,
            'Trade_Count': trader.trade_count,
            'Efficiency_Score': trader.efficiency_score,
            'Trading_Score': trader.trading_score
        }
        # Add top holdings
        for i, holding in enumerate(trader.top_holdings[:5] if trader.top_holdings else [], 1):
            row[f'Top_Holding_{i}'] = holding
        data.append(row)
    
    # Save to CSV, overwriting any existing file
    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False)
    logger.info(f"Results saved to {output_file}")

if __name__ == "__main__":
    main() 
import pandas as pd
import os
from datetime import datetime
import glob

def load_historical_data():
    """Load historical data if exists, otherwise create new"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    historical_dir = os.path.join(script_dir, "historical")
    historical_file = os.path.join(historical_dir, "all_wallets.csv")
    
    if not os.path.exists(historical_dir):
        os.makedirs(historical_dir)
        
    if os.path.exists(historical_file):
        historical_df = pd.read_csv(historical_file)
        print(f"Loaded {len(historical_df)} wallets from historical records")
    else:
        historical_df = pd.DataFrame(columns=['Wallet_Address', 'composite_score', 'last_seen', 'appearances'])
        print("Created new historical tracking file")
    
    return historical_df

def load_latest_analysis():
    """Load the most recent wallet analysis results"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    analysis_file = os.path.join(script_dir, "output/wallet_holdings.csv")
    
    if not os.path.exists(analysis_file):
        raise FileNotFoundError("No wallet analysis file found!")
    
    print(f"Loading analysis from {analysis_file}")
    return pd.read_csv(analysis_file)

def format_for_crystalized(df):
    """Format dataframe into crystalized wallet format"""
    crystalized_data = []
    
    for _, row in df.iterrows():
        crystalized_row = {
            'Appearances': row.get('appearances', 1.0),
            'Wallet': f"https://gmgn.ai/sol/address/{row['Wallet_Address']}",
            'Wallet_Address': row['Wallet_Address'],
            'SOL_Balance': 0.0,
            'Token_Value_SOL': 0.0,
            'Token_Count': 0.0,
            'Win_Rate_30d': 0.0,
            'pnl_7d_percent': '+0.00%',
            'pnl_7d_usd': f"+${row.get('PnL', 0):,.0f}",
            'win_rate': '0.00%',
            'score': row['composite_score']
        }
        crystalized_data.append(crystalized_row)
    
    return pd.DataFrame(crystalized_data)

def update_scores():
    """Main process to update scores and generate new crystalized list"""
    # Load data
    historical_df = load_historical_data()
    current_df = load_latest_analysis()
    
    # Initialize tracking metrics
    wallets_updated = 0
    wallets_new = 0
    significant_changes = []
    
    # Get current top 300 before updates
    previous_top_300 = set(historical_df.nlargest(300, 'composite_score')['Wallet_Address']) if not historical_df.empty else set()
    
    # Process each wallet in current analysis
    for _, row in current_df.iterrows():
        wallet = row['Wallet']
        current_score = row['Trading_Score']
        
        if wallet in historical_df['Wallet_Address'].values:
            # Update existing wallet
            hist_score = historical_df.loc[historical_df['Wallet_Address'] == wallet, 'composite_score'].iloc[0]
            appearances = historical_df.loc[historical_df['Wallet_Address'] == wallet, 'appearances'].iloc[0] + 1
            new_score = (hist_score * 0.7) + (current_score * 0.3)
            
            # Track significant score changes (>20% change)
            score_change_pct = ((new_score - hist_score) / hist_score * 100) if hist_score > 0 else 100
            if abs(score_change_pct) > 20:
                significant_changes.append({
                    'wallet': wallet,
                    'old_score': hist_score,
                    'new_score': new_score,
                    'change_pct': score_change_pct
                })
            
            # Update historical record
            historical_df.loc[historical_df['Wallet_Address'] == wallet, 'composite_score'] = new_score
            historical_df.loc[historical_df['Wallet_Address'] == wallet, 'appearances'] = appearances
            wallets_updated += 1
        else:
            # Add new wallet
            new_row = {
                'Wallet_Address': wallet,
                'composite_score': current_score,
                'appearances': 1,
                'last_seen': datetime.now().strftime("%Y-%m-%d")
            }
            historical_df = pd.concat([historical_df, pd.DataFrame([new_row])], ignore_index=True)
            wallets_new += 1
    
    # Update last_seen for all current wallets
    current_wallets = current_df['Wallet'].values
    historical_df.loc[historical_df['Wallet_Address'].isin(current_wallets), 'last_seen'] = datetime.now().strftime("%Y-%m-%d")
    
    # Sort by composite score
    historical_df = historical_df.sort_values('composite_score', ascending=False)
    
    # Get new top 300 and analyze changes
    new_top_300 = set(historical_df.head(300)['Wallet_Address'])
    dropped_from_top = previous_top_300 - new_top_300
    added_to_top = new_top_300 - previous_top_300
    
    # Save full historical data
    historical_df.to_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "historical/all_wallets.csv"), index=False)
    
    # Print summary statistics
    print("\n=== Update Summary ===")
    print(f"Total wallets processed: {len(current_df)}")
    print(f"Wallets updated: {wallets_updated}")
    print(f"New wallets added: {wallets_new}")
    print(f"\nSignificant Score Changes (>20%):")
    for change in significant_changes:
        print(f"Wallet: {change['wallet'][:8]}...")
        print(f"  Score: {change['old_score']:.2f} → {change['new_score']:.2f} ({change['change_pct']:+.1f}%)")
    
    print(f"\nTop 300 Changes:")
    print(f"Dropped from top 300: {len(dropped_from_top)} wallets")
    if dropped_from_top:
        print("Notable drops (top 100 → out):")
        for wallet in dropped_from_top:
            old_rank = list(previous_top_300).index(wallet) + 1
            if old_rank <= 100:
                print(f"  {wallet[:8]}... (was rank {old_rank})")
    
    print(f"New to top 300: {len(added_to_top)} wallets")
    if added_to_top:
        print("Notable additions (→ top 100):")
        for wallet in added_to_top:
            new_rank = list(new_top_300).index(wallet) + 1
            if new_rank <= 100:
                print(f"  {wallet[:8]}... (now rank {new_rank})")
    
    # Format and save new crystalized list
    top_300 = historical_df.head(300)
    crystalized_df = format_for_crystalized(top_300)
    
    # Save new crystalized list with fixed filename
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "output")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    crystalized_file = os.path.join(output_dir, "crystalized_wallets.csv")
    crystalized_df.to_csv(crystalized_file, index=False)
    print(f"\nSaved top 300 wallets to {crystalized_file}")

def main():
    print("Starting wallet score update process...")
    update_scores()
    print("Process completed!")

if __name__ == "__main__":
    main() 
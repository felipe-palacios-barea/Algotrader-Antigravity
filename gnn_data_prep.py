import pandas as pd
import numpy as np
import torch
from torch_geometric.data import HeteroData
from pathlib import Path

# Paths
DATA_DIR = Path(__file__).parent
ENRICHED_TRADES_PATH = DATA_DIR / 'enriched_trades.csv'
COMMITTEES_PATH = DATA_DIR / 'committees.csv'
METRICS_PATH = DATA_DIR / 'politician_alpha_metrics.csv'
OUTPUT_PATH = DATA_DIR / 'gnn_data.pt'

def calculate_success_tier(row):
    """
    Calculates Success Tier based on Alpha (Pct_Change - Sector_Momentum).
    Tiers:
    0: Alpha > 10 (Jackpot)
    1: Alpha 2-10 (Solid)
    2: Alpha -2 to 2 (Neutral)
    3: Alpha < -2 (Loss)
    """
    pct = row.get('Pct_Change', 0)
    mom = row.get('Sector_Momentum', 0)
    
    # Handle NaNs (common in empty or erroneous rows)
    try:
        pct = float(pct) if pd.notna(pct) else 0.0
        mom = float(mom) if pd.notna(mom) else 0.0
    except:
        return 2
        
    alpha = pct - mom
    
    if alpha > 10: return 0
    if alpha > 2: return 1
    if alpha > -2: return 2
    return 3

def prepare_gnn_data():
    print("Loading data...")
    trades_df = pd.read_csv(ENRICHED_TRADES_PATH)
    comm_df = pd.read_csv(COMMITTEES_PATH)
    
    # Load politician alpha metrics if available
    alpha_map = {}
    if METRICS_PATH.exists():
        metrics_df = pd.read_csv(METRICS_PATH)
        alpha_map = dict(zip(metrics_df['Investor Name'], metrics_df['Alpha_Score_0_to_1']))
    
    # 1. Label Trades
    print("Calculating success tiers...")
    trades_df['label'] = trades_df.apply(calculate_success_tier, axis=1)
    
    # 2. Build Mappings
    # Filter out empty names or tickers
    trades_df = trades_df[trades_df['Investor Name'].notna() & trades_df['Ticker'].notna()]
    
    all_politicians = sorted(trades_df['Investor Name'].unique())
    all_tickers = sorted(trades_df['Ticker'].unique())
    all_committees = sorted(comm_df['Committee'].unique())
    
    pol_id_map = {name: i for i, name in enumerate(all_politicians)}
    ticker_id_map = {ticker: i for i, ticker in enumerate(all_tickers)}
    comm_id_map = {name: i for i, name in enumerate(all_committees)}
    
    # 3. Create HeteroData Object
    data = HeteroData()
    
    # 3a. Node Features: Politician
    pol_features = []
    for name in all_politicians:
        # Use Alpha_Score if available, else 0.5
        val = alpha_map.get(name, 0.5)
        pol_features.append([float(val)])
    data['politician'].x = torch.tensor(pol_features, dtype=torch.float)
    
    # 3b. Node Features: Stock
    stock_features = []
    for ticker in all_tickers:
        subset = trades_df[trades_df['Ticker'] == ticker]
        avg_pct = subset['Pct_Change'].mean()
        avg_std = subset['Std_Dev'].mean()
        # Fallback for NaNs
        avg_pct = avg_pct if pd.notna(avg_pct) else 0.0
        avg_std = avg_std if pd.notna(avg_std) else 0.0
        stock_features.append([float(avg_pct), float(avg_std)])
    data['stock'].x = torch.tensor(stock_features, dtype=torch.float)
    
    # 3c. Node Features: Committee
    data['committee'].x = torch.eye(len(all_committees))
    
    # 4. Edge Indices
    edge_index_traded = []
    edge_attr_traded = []
    y_labels = []
    
    for _, row in trades_df.iterrows():
        p_name = row['Investor Name']
        ticker = row['Ticker']
        
        if p_name in pol_id_map and ticker in ticker_id_map:
            p_idx = pol_id_map[p_name]
            s_idx = ticker_id_map[ticker]
            edge_index_traded.append([p_idx, s_idx])
            
            # Edge attributes: trade size, conviction
            # Handle possible missing columns in some versions of enriched_trades
            conv = row.get('trade_size_conviction', row.get('probability', 0.5))
            amt = row.get('High Amount', 0)
            
            try:
                conv = float(conv) if pd.notna(conv) else 0.5
                amt = float(amt) if pd.notna(amt) else 0.0
            except:
                conv, amt = 0.5, 0.0
                
            edge_attr_traded.append([conv, amt / 1e6])
            y_labels.append(row['label'])
            
    if not edge_index_traded:
        print("Error: No valid trade edges found!")
        return

    data['politician', 'traded', 'stock'].edge_index = torch.tensor(edge_index_traded).t().contiguous()
    data['politician', 'traded', 'stock'].edge_attr = torch.tensor(edge_attr_traded, dtype=torch.float)
    
    # We'll treat this as a link-property prediction or node prediction on a "Trade" node?
    # For now, let's keep it simple: we want to predict the LABEL of the trade edge.
    # PyG doesn't have a direct 'y' on edges by default for all tasks, but we can store it.
    data['politician', 'traded', 'stock'].y = torch.tensor(y_labels, dtype=torch.long)
    
    # 4b. Politician -[MEMBER_OF]-> Committee
    edge_index_member = []
    for _, row in comm_df.iterrows():
        p_name = row['Full_Name']
        comm = row['Committee']
        if p_name in pol_id_map:
            p_idx = pol_id_map[p_name]
            c_idx = comm_id_map[comm]
            edge_index_member.append([p_idx, c_idx])
            
    if edge_index_member:
        data['politician', 'member_of', 'committee'].edge_index = torch.tensor(edge_index_member).t().contiguous()
    
    print(f"Graph stats: {data}")
    print(f"Data saved to {OUTPUT_PATH}")
    torch.save(data, OUTPUT_PATH)
    
    metadata = {
        'pol_id_map': pol_id_map,
        'ticker_id_map': ticker_id_map,
        'comm_id_map': comm_id_map
    }
    torch.save(metadata, DATA_DIR / 'gnn_metadata.pt')

if __name__ == "__main__":
    prepare_gnn_data()

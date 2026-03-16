import torch
import torch.nn.functional as F
from torch_geometric.data import HeteroData
from torch_geometric.transforms import ToUndirected
from gnn_model import create_model
from pathlib import Path
import pandas as pd
import numpy as np

# Fix for PyTorch 2.6+
torch.serialization.add_safe_globals([HeteroData])

# Paths
DATA_DIR = Path(__file__).parent
DATA_PATH = DATA_DIR / 'gnn_data.pt'
MODEL_PATH = DATA_DIR / 'gnn_model.pth'

def integrated_gradients(model, x_dict, edge_index_dict, trade_edge_index, trade_edge_attr, target_class=0, steps=50):
    """
    Simplified Integrated Gradients for HeteroGNN.
    Attribute success to input features of the trade edges and nodes.
    """
    # We want to attribute to:
    # 1. Politician features (x_dict['politician'])
    # 2. Stock features (x_dict['stock'])
    # 3. Trade edge attributes (trade_edge_attr)
    
    # Baselines (all zeros)
    pol_x = x_dict['politician'].clone().detach().requires_grad_(True)
    stock_x = x_dict['stock'].clone().detach().requires_grad_(True)
    attr = trade_edge_attr.clone().detach().requires_grad_(True)
    
    pol_baseline = torch.zeros_like(pol_x)
    stock_baseline = torch.zeros_like(stock_x)
    attr_baseline = torch.zeros_like(attr)
    
    total_pol_grads = torch.zeros_like(pol_x)
    total_stock_grads = torch.zeros_like(stock_x)
    total_attr_grads = torch.zeros_like(attr)
    
    for i in range(1, steps + 1):
        alpha = i / steps
        
        # Interpolate
        curr_pol = pol_baseline + alpha * (pol_x - pol_baseline)
        curr_stock = stock_baseline + alpha * (stock_x - stock_baseline)
        curr_attr = attr_baseline + alpha * (attr - attr_baseline)
        
        temp_x_dict = {
            'politician': curr_pol,
            'stock': curr_stock,
            'committee': x_dict['committee'].clone()
        }
        
        # Forward pass
        out = model(temp_x_dict, edge_index_dict, trade_edge_index, curr_attr)
        
        # Target: Score for 'Jackpot' (class 0) - sum over all trades
        # Or we can do it for a specific trade. Let's do it for all trades.
        score = out[:, target_class].sum()
        
        # Backward pass
        grads = torch.autograd.grad(score, [curr_pol, curr_stock, curr_attr])
        total_pol_grads += grads[0]
        total_stock_grads += grads[1]
        total_attr_grads += grads[2]
        
    avg_pol_grads = total_pol_grads / steps
    avg_stock_grads = total_stock_grads / steps
    avg_attr_grads = total_attr_grads / steps
    
    pol_ig = (pol_x - pol_baseline) * avg_pol_grads
    stock_ig = (stock_x - stock_baseline) * avg_stock_grads
    attr_ig = (attr - attr_baseline) * avg_attr_grads
    
    return pol_ig, stock_ig, attr_ig

def interpret():
    print("Loading model and data...")
    data = torch.load(DATA_PATH, weights_only=False)
    data = ToUndirected()(data)
    
    model = create_model(data, hidden_channels=64)
    model.load_state_dict(torch.load(MODEL_PATH, weights_only=False))
    model.eval()
    
    # Run IG
    print("Calculating Integrated Gradients...")
    pol_ig, stock_ig, attr_ig = integrated_gradients(
        model, data.x_dict, data.edge_index_dict, 
        data['politician', 'traded', 'stock'].edge_index,
        data['politician', 'traded', 'stock'].edge_attr,
        target_class=0 # Predicting "Jackpot"
    )
    
    # Aggregate importance (absolute IG values)
    # Features:
    # Politician: [Alpha_Score]
    # Stock: [Avg_Pct, Avg_Std]
    # Trade: [Conviction, Amount]
    
    pol_imp = pol_ig.abs().mean(dim=0).tolist()
    stock_imp = stock_ig.abs().mean(dim=0).tolist()
    trade_imp = attr_ig.abs().mean(dim=0).tolist()
    
    total_weights = {
        "Politician Alpha History": pol_imp[0],
        "Stock Recent Performance (Avg Pct)": stock_imp[0],
        "Stock Volatility (Avg Std)": stock_imp[1],
        "Trade Conviction (LLM/Prob)": trade_imp[0],
        "Trade Size (Amount)": trade_imp[1]
    }
    
    # Normalize to 100%
    total_sum = sum(total_weights.values())
    normalized = {k: (v / total_sum) * 100 for k, v in total_weights.items()}
    
    print("\n--- Feature Contribution Weights (Variable Importance) ---")
    for k, v in sorted(normalized.items(), key=lambda x: x[1], reverse=True):
        print(f"{k}: {v:.2f}%")
        
    # Save to file
    with open(DATA_DIR / 'contribution_weights.txt', 'w') as f:
        f.write("Feature Contribution Weights for Stock Success Prediction\n")
        f.write("========================================================\n\n")
        for k, v in sorted(normalized.items(), key=lambda x: x[1], reverse=True):
            f.write(f"{k}: {v:.2f}%\n")

if __name__ == "__main__":
    interpret()

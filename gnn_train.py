import torch
import torch.nn.functional as F
from torch_geometric.transforms import ToUndirected
from torch_geometric.data import HeteroData
from gnn_model import create_model
from pathlib import Path

# Fix for PyTorch 2.6+ loading HeteroData
torch.serialization.add_safe_globals([HeteroData])

# Paths
DATA_DIR = Path(__file__).parent
DATA_PATH = DATA_DIR / 'gnn_data.pt'
MODEL_SAVE_PATH = DATA_DIR / 'gnn_model.pth'

def train():
    print("Loading graph data...")
    # Using weights_only=False because HeteroData is a custom class
    data = torch.load(DATA_PATH, weights_only=False)
    
    # Add reverse edges for bidirectional information flow
    data = ToUndirected()(data)
    
    # Class weights for Cross-Entropy (handling imbalance)
    # Counts: {0: 54, 1: 39, 2: 153, 3: 100}
    y = data['politician', 'traded', 'stock'].y
    _, counts = torch.unique(y, return_counts=True)
    weights = 1.0 / counts.float()
    weights = weights / weights.sum()
    
    # Split: Train on 80%, Val on 20% (assuming temporal order in the CSV)
    num_trades = y.size(0)
    train_size = int(0.8 * num_trades)
    train_mask = torch.zeros(num_trades, dtype=torch.bool)
    train_mask[:train_size] = True
    val_mask = ~train_mask
    
    # Initialize Model
    model = create_model(data, hidden_channels=64)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01, weight_decay=5e-4)
    criterion = torch.nn.CrossEntropyLoss(weight=weights)

    # Edge indices for message passing (excluding the reverse relations for simplicity in Decoder)
    # and identifying the trade edges for prediction
    
    # PyG ToUndirected creates 'rev_traded' and 'rev_member_of'
    # Forward pass logic in gnn_model.py handles specific keys.
    
    trade_edge_index = data['politician', 'traded', 'stock'].edge_index
    trade_edge_attr = data['politician', 'traded', 'stock'].edge_attr
    
    print("Starting training...")
    for epoch in range(1, 201):
        model.train()
        optimizer.zero_grad()
        
        # Pass the whole dict for GNN, then specific info for prediction
        out = model(data.x_dict, data.edge_index_dict, trade_edge_index, trade_edge_attr)
        
        loss = criterion(out[train_mask], y[train_mask])
        loss.backward()
        optimizer.step()
        
        if epoch % 20 == 0:
            model.eval()
            with torch.no_grad():
                val_out = model(data.x_dict, data.edge_index_dict, trade_edge_index, trade_edge_attr)
                pred = val_out.argmax(dim=-1)
                acc = (pred[val_mask] == y[val_mask]).sum().item() / val_mask.sum().item()
                print(f"Epoch {epoch:03d}, Loss: {loss.item():.4f}, Val Acc: {acc:.4f}")

    print(f"Training complete. Saving model to {MODEL_SAVE_PATH}")
    torch.save(model.state_dict(), MODEL_SAVE_PATH)

if __name__ == "__main__":
    train()

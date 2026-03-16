import torch
import torch.nn.functional as F
from torch_geometric.nn import SAGEConv, HeteroConv, Linear
from torch.nn import Sequential, ReLU, Dropout

class HeteroGNN(torch.nn.Module):
    def __init__(self, metadata, hidden_channels, out_channels, num_classes):
        super().__init__()
        
        self.lin_dict = torch.nn.ModuleDict()
        for node_type in metadata[0]:
            self.lin_dict[node_type] = Linear(-1, hidden_channels)

        self.conv1 = HeteroConv({
            rel: SAGEConv(hidden_channels, hidden_channels)
            for rel in metadata[1]
        }, aggr='sum')
        
        self.conv2 = HeteroConv({
            rel: SAGEConv(hidden_channels, hidden_channels)
            for rel in metadata[1]
        }, aggr='sum')

        # 3. Edge Decoder: Takes (pol_emb, stock_emb) -> Predicted Class
        self.decoder = Sequential(
            torch.nn.Linear(hidden_channels * 2 + 2, hidden_channels), # +2 for edge attributes: conviction, amount
            ReLU(),
            Dropout(0.2),
            torch.nn.Linear(hidden_channels, num_classes)
        )

    def forward(self, x_dict, edge_index_dict, trade_edge_index, trade_edge_attr):
        # Initial projection
        x_dict = {node_type: self.lin_dict[node_type](x) for node_type, x in x_dict.items()}
        x_dict = {node_type: F.relu(x) for node_type, x in x_dict.items()}

        # GNN layers
        x_dict = self.conv1(x_dict, edge_index_dict)
        x_dict = {node_type: F.relu(x) for node_type, x in x_dict.items()}
        
        x_dict = self.conv2(x_dict, edge_index_dict)
        x_dict = {node_type: F.relu(x) for node_type, x in x_dict.items()}

        # Decoding: predict on the 'traded' edges
        row, col = trade_edge_index
        pol_emb = x_dict['politician'][row]
        stock_emb = x_dict['stock'][col]
        
        # Concatenate embeddings and edge attributes
        out = torch.cat([pol_emb, stock_emb, trade_edge_attr], dim=-1)
        return self.decoder(out)

def create_model(data, hidden_channels=64):
    num_classes = 4 # Success Tiers A, B, C, D
    model = HeteroGNN(data.metadata(), hidden_channels, hidden_channels, num_classes)
    return model

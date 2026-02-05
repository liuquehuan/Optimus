import torch # type: ignore
import torch.nn as nn # type: ignore
import torch.nn.functional as F # type: ignore
from layers import GraphAttentionLayer


class GAT(nn.Module): ## 三层gat，alpha是leakyrelu的参数
    def __init__(self, nfeat, nclass, dropout, alpha, nhid=32, nheads=8):
        """Dense version of GAT."""
        super(GAT, self).__init__()
        self.dropout = dropout

        ## concat为True表示这是中间层，需要加一个elu
        ## 有nheads个头
        self.attentions = [GraphAttentionLayer(nfeat, nhid, dropout=dropout, alpha=alpha, concat=True) for _ in range(nheads)]
        for i, attention in enumerate(self.attentions):
            self.add_module('attention_{}'.format(i), attention)

        self.mid_attentions = [GraphAttentionLayer(nheads * nhid, 32, dropout=dropout, alpha=alpha, concat=True) for _ in range(4)]
        for i, attention in enumerate(self.mid_attentions):
            self.add_module('mid_attention_{}'.format(i), attention)

        self.out_attentions = [GraphAttentionLayer(32 * 4, 16, dropout=dropout, alpha=alpha, concat=True) for _ in range(4)]
        for i, attention in enumerate(self.out_attentions):
            self.add_module('out_attention_{}'.format(i), attention)

        self.linear = nn.Linear(16 * 4, nclass)
        
    def forward(self, x, adj):
        ## 一上来就dropout？？？？？
        x = F.dropout(x, self.dropout, training=self.training)
        x = torch.cat([att(x, adj) for att in self.attentions], dim=1)
        x = F.dropout(x, self.dropout, training=self.training)
        x = torch.cat([att(x, adj) for att in self.mid_attentions], dim=1)
        x = F.dropout(x, self.dropout, training=self.training)
        x = torch.cat([att(x, adj) for att in self.out_attentions], dim=1)
        x = F.dropout(x, self.dropout, training=self.training)

        x = torch.max(x, dim=0).values
        x = self.linear(x)
        return x


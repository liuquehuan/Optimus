import torch # type: ignore
import torch.nn as nn # type: ignore
import torch.nn.functional as F # type: ignore
from layers import GraphAttentionLayer


class GAT(nn.Module): ## 两层gat，alpha是leakyrelu的参数
    def __init__(self, nfeat, nhid, nclass, dropout, alpha, nheads):
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

        # self.out_att = GraphAttentionLayer(32, 16, dropout=dropout, alpha=alpha, concat=False)
        # self.out_att = GraphAttentionLayer(nheads * nhid, 16, dropout=dropout, alpha=alpha, concat=False)
        # self.out_att = GraphAttentionLayer(128, 64, dropout=dropout, alpha=alpha, concat=False)

        self.out_attentions = [GraphAttentionLayer(32 * 4, 16, dropout=dropout, alpha=alpha, concat=True) for _ in range(4)]
        for i, attention in enumerate(self.out_attentions):
            self.add_module('out_attention_{}'.format(i), attention)

        # self.linear = nn.Linear(16, nclass)
        self.linear = nn.Linear(16 * 4, nclass)
        
    def forward(self, x, adj):
        ## 一上来就dropout？？？？？
        org_x = x
        x = F.dropout(x, self.dropout, training=self.training)
        x = torch.cat([att(x, adj) for att in self.attentions], dim=1)
        x = F.dropout(x, self.dropout, training=self.training)
        x = torch.cat([att(x, adj) for att in self.mid_attentions], dim=1)
        x = F.dropout(x, self.dropout, training=self.training)
        x = torch.cat([att(x, adj) for att in self.out_attentions], dim=1)
        x = F.dropout(x, self.dropout, training=self.training)
        # x = F.elu(self.out_att(x, adj))
        # x = F.elu(self.out_att(x, adj)) + org_x
        x = self.linear(x)
        return F.log_softmax(x, dim=1)


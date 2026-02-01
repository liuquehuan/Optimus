import math
import torch ## type: ignore
from torch import optim ## type: ignore
from torch.nn.parameter import Parameter ## type: ignore
from torch.nn.modules.module import Module ## type: ignore
import torch.nn.functional as F ## type: ignore


class GraphConvolution(Module):
    """
    Simple GCN layer, similar to https://arxiv.org/abs/1609.02907
    """

    def __init__(self, in_features, out_features, bias=True):
        super(GraphConvolution, self).__init__()
        self.in_features = in_features
        self.out_features = out_features
        self.weight = Parameter(torch.FloatTensor(in_features, out_features))
        if bias:
            self.bias = Parameter(torch.FloatTensor(out_features))
        else:
            self.register_parameter('bias', None)
        self.reset_parameters()

    def reset_parameters(self):
        stdv = 1. / math.sqrt(self.weight.size(1))
        self.weight.data.uniform_(-stdv, stdv)
        if self.bias is not None:
            self.bias.data.uniform_(-stdv, stdv)

    def forward(self, input, adj):
        support = torch.mm(input, self.weight)

        ## adj是邻接矩阵，这一步实现了聚合（message passing）
        output = torch.spmm(adj, support) ## sparse matrix multiplication
        if self.bias is not None:
            return output + self.bias
        else:
            return output

    def __repr__(self):
        return self.__class__.__name__ + ' (' + str(self.in_features) + ' -> ' + str(self.out_features) + ')'


import torch.nn as nn ## type: ignore

class GCN(nn.Module):
    def __init__(self, nfeat, nhid, nclass, dropout):
        super(GCN, self).__init__()

        self.gc1 = GraphConvolution(nfeat, nhid)
        self.gc2 = GraphConvolution(nhid, nclass)
        self.fc = nn.Linear(nclass, 5)
        self.dropout = dropout

    def forward(self, x, adj, dh=None, embed=False):
        ## 为什么论文里说三层，这里只有两层
        x = F.relu(self.gc1(x, adj))
        x = F.dropout(x, self.dropout, training=self.training)
        x = F.relu(self.gc2(x, adj))
        if embed: ## 只要embedding？
            return x
        if dh is not None: ## 似乎是一个残差连接
            x = x + dh
        x = self.fc(x)

        return F.log_softmax(x, dim=1)
        # return x

def get_model(feature_num, hidden, nclass, dropout):
    model = GCN(nfeat=feature_num,
                nhid=hidden,
                nclass=nclass,
                dropout=dropout)
    return model
def get_optimizer(model, lr, weight_decay):
    optimizer = optim.Adam(model.parameters(),
                           lr=lr, weight_decay=weight_decay)
    return optimizer
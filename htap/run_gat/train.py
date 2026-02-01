import time

import numpy as np # type: ignore
import torch.nn.functional as F # type: ignore
import torch.optim as optim # type: ignore
import torch.nn as nn # type: ignore
import torch ## type: ignore
import argparse
import random
from models import GAT
parser = argparse.ArgumentParser()
parser.add_argument('--no-cuda', action='store_true', default=False, help='Disables CUDA training.')
parser.add_argument('--fastmode', action='store_true', default=False, help='Validate during training pass.')
parser.add_argument('--sparse', action='store_true', default=False, help='GAT with sparse version or not.')
parser.add_argument('--seed', type=int, default=72, help='Random seed.')
parser.add_argument('--epochs', type=int, default=10, help='Number of epochs to train.')
parser.add_argument('--lr', type=float, default=0.005, help='Initial learning rate.')
parser.add_argument('--weight_decay', type=float, default=5e-4, help='Weight decay (L2 loss on parameters).')
parser.add_argument('--hidden', type=int, default=32, help='Number of hidden units.')
parser.add_argument('--nb_heads', type=int, default=8, help='Number of head attentions.')
parser.add_argument('--dropout', type=float, default=0, help='Dropout rate (1 - keep probability).')
parser.add_argument('--alpha', type=float, default=0.2, help='Alpha for the leaky_relu.')
parser.add_argument('--patience', type=int, default=100, help='Patience')

args = parser.parse_args()
args.cuda = not args.no_cuda and torch.cuda.is_available()

random.seed(args.seed)
np.random.seed(args.seed)
torch.manual_seed(args.seed)
if args.cuda:
    torch.cuda.manual_seed(args.seed)

import sys
sys.path.append(r".")
from graphembedding import load_data_from_matrix
from nodeutils import extract_plan, add_across_plan_relations


label_count = np.zeros(4)
def train(labels, features, adj, model, optimizer):
    model.train()
    optimizer.zero_grad()
    output = model(features, adj)

    leaf = []
    for i in range(len(output)):
        assert(labels[i] < 5)
        if labels[i] < 4:
            leaf.append(i)
            label_count[labels[i]] += 1

    # counts = torch.tensor([333, 288, 995, 94], dtype=torch.float)
    # weights = 1.0 / counts
    # loss_train = F.nll_loss(output[leaf], labels[leaf], weight=weights)
    loss_train = F.nll_loss(output[leaf], labels[leaf])
    
    loss_train.backward()
    optimizer.step()

    return round(loss_train.item(), 4)


def run_train_upd(DATAPATH, num_sample, model=None):
    feature_num = 2 + 3 + 6
    _labels, _features, _adj = [], [], []

    for sample_id in range(0, num_sample):
        oid = 0
        vmatrix = []
        ematrix = []
        conflict_operators = {}

        with open(DATAPATH + "/" + str(sample_id) + ".txt", "r") as f:

            id = 0
            for sample in f.readlines():
                sample = eval(sample)

                ## node_matrix的元素是node_feature，node_feature第一维是oid
                node_matrix, edge_matrix, conflict_operators, oid, _ = extract_plan(sample, conflict_operators, oid, id == 10)
                vmatrix = vmatrix + node_matrix
                ematrix = ematrix + edge_matrix

                id += 1

        # ematrix = add_across_plan_relations(conflict_operators, ematrix)

        ## 这里实际是把数据分为了训练集和验证集两部分。但感觉验证集不是必要的，先去掉了
        if len(ematrix) == 0:
            ematrix = [[0, 0, 0]]
        # print(ematrix)
        adj, features, labels, _, _, _, _ = load_data_from_matrix(np.array(vmatrix, dtype=np.float32), np.array(ematrix, dtype=np.float32))
        _labels.append(labels)
        _features.append(features)
        _adj.append(adj)

    if model is None:
        model = GAT(nfeat=feature_num, 
                nhid=args.hidden, 
                nclass=4, 
                dropout=args.dropout, 
                nheads=args.nb_heads, 
                alpha=args.alpha)
    optimizer = optim.Adam(model.parameters(), lr=args.lr, weight_decay=args.weight_decay)

    # if args.cuda:
    #     model.cuda()
    #     features = features.cuda()
    #     adj = adj.cuda()
    #     labels = labels.cuda()
    #     idx_train = idx_train.cuda()
    #     idx_val = idx_val.cuda()
    #     idx_test = idx_test.cuda()

    losses = []
    for epoch in range(args.epochs):
        loss_accum = 0
        t = time.time()
        for labels, features, adj in zip(_labels, _features, _adj):
            loss_train = train(labels, features, adj, model=model, optimizer=optimizer)
            loss_accum += loss_train
        loss_accum /= num_sample
        print('Epoch: {:04d}'.format(epoch + 1),
          'loss_train: {:.4f}'.format(loss_accum),
          'time: {:.4f}s'.format(time.time() - t))
        losses.append(loss_accum)
        if len(losses) > 10 and losses[-1] < 0.1:
            last_two = np.min(losses[-2:])
            if last_two > losses[-10] or (losses[-10] - last_two < 0.0001):
                print("Stopped training from convergence condition at epoch", epoch)
                break

    return model, label_count


def run_test_upd(model):

    oid = 0
    vmatrix = []
    ematrix = []
    conflict_operators = {}
    leaf = []

    with open("plan.txt", "r") as f:

        id = 0
        for sample in f.readlines():
            sample = eval(sample)

            ## node_matrix的元素是node_feature，node_feature第一维是oid
            node_matrix, edge_matrix, conflict_operators, oid, leaf_matrix = extract_plan(sample, conflict_operators, oid, id == 10)

            vmatrix = vmatrix + node_matrix
            ematrix = ematrix + edge_matrix
            if id == 10:
                leaf = leaf_matrix

            # ematrix = add_across_plan_relations(conflict_operators, ematrix)
            id += 1

    if len(ematrix) == 0:
            ematrix = [[0, 0, 0]]
    adj, features, _, _, _, _, idx_map = load_data_from_matrix(np.array(vmatrix, dtype=np.float32), np.array(ematrix, dtype=np.float32))

    model.eval()
    output = model(features, adj)

    res = {}
    for node in leaf:
        node[0] = idx_map[node[0]]
        node[-2] = output[node[0]].argmax()
        if node[-1] not in res or node[2] > res[node[-1]][2]:
            res[node[-1]] = node

    return res

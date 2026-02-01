import time

import numpy as np
import torch.nn.functional as F # type: ignore
import torch.optim as optim # type: ignore
import torch.nn as nn # type: ignore
import torch ## type: ignore

from GCN import get_model, get_optimizer
from constants import args, NODE_DIM, DATAPATH
from graphembedding import load_data_from_matrix
from nodeutils import extract_plan, add_across_plan_relations


def train(labels, features, adj, model, optimizer):
    model.train()
    optimizer.zero_grad()
    output = model(features, adj)
    labels = labels.long()

    leaf = []
    for i in range(len(output)):
        assert(labels[i] < 5)
        if labels[i] < 4:
            leaf.append(i)
    
    # 创建损失函数实例
    criterion = nn.CrossEntropyLoss()
    
    # 使用损失函数
    loss_train = criterion(output[leaf], labels[leaf])
    
    loss_train.backward()
    optimizer.step()

    if not args.fastmode:
        # Evaluate validation set performance separately,
        # deactivates dropout during validation run.
        model.eval()
        output = model(features, adj)

    return round(loss_train.item(), 4)


def run_train_upd():
    # TODO more features, more complicated model
    ## 为什么第二个gcn层输出维度要取node_dim？
    feature_num = 2
    model = get_model(feature_num=feature_num, hidden=args.hidden, nclass=NODE_DIM, dropout=args.dropout)
    optimizer = get_optimizer(model=model, lr=args.lr, weight_decay=args.weight_decay)
    _labels, _features, _adj = [], [], []

    for sample_id in range(0, 440):
    ## 不同于gpredictor的实现，我们每个batch只有一个样本。
        oid = 0
        vmatrix = []
        ematrix = []
        conflict_operators = {}

        with open(DATAPATH + "/" + str(sample_id) + ".txt", "r") as f:

            for sample in f.readlines():
                sample = eval(sample)

                ## node_matrix的元素是node_feature，node_feature第一维是oid
                _, node_matrix, edge_matrix, conflict_operators, _, oid, _ = extract_plan(sample, conflict_operators, oid)
                vmatrix = vmatrix + node_matrix
                ematrix = ematrix + edge_matrix

        ematrix = add_across_plan_relations(conflict_operators, ematrix)

        ## 这里实际是把数据分为了训练集和验证集两部分。但感觉验证集不是必要的，先去掉了
        adj, features, labels, _, _, _, _ = load_data_from_matrix(np.array(vmatrix, dtype=np.float32), np.array(ematrix, dtype=np.float32))
        _labels.append(labels)
        _features.append(features)
        _adj.append(adj)

    losses = []
    for epoch in range(args.epochs):
        loss_accum = 0
        t = time.time()
        for labels, features, adj in zip(_labels, _features, _adj):
            loss_train = train(labels, features, adj, model=model, optimizer=optimizer)
            loss_accum += loss_train
        loss_accum /= 440 ## 440 = num_sample
        print('Epoch: {:04d}'.format(epoch + 1),
          'loss_train: {:.4f}'.format(loss_accum),
          'time: {:.4f}s'.format(time.time() - t))
        losses.append(loss_accum)
        if len(losses) > 10 and losses[-1] < 0.1:
            last_two = np.min(losses[-2:])
            if last_two > losses[-10] or (losses[-10] - last_two < 0.0001):
                print("Stopped training from convergence condition at epoch", epoch)
                break

    return model


def run_test_upd(model):
    # def predict(labels, features, adj, dh):
    #     model.eval()
    #     output = model(features, adj, dh)
    #     # loss_test = F.mse_loss(output, labels)
    #     # print("Test set results:",
    #     #       "loss= {:.4f}".format(loss_test.item()))
    #     return output

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
            _, node_matrix, edge_matrix, conflict_operators, _, oid, leaf_matrix = \
                extract_plan(sample, conflict_operators, oid)

            vmatrix = vmatrix + node_matrix
            ematrix = ematrix + edge_matrix
            if id == 10:
                leaf = leaf_matrix

            ematrix = add_across_plan_relations(conflict_operators, ematrix)
            id += 1

    adj, features, _, _, _, _, idx_map = load_data_from_matrix(np.array(vmatrix, dtype=np.float32),
                                                                            np.array(ematrix, dtype=np.float32))

    model.eval()
    # dh = model(dfeatures, dadj, None, True)

    # output = predict(dlabels, dfeatures, adj, dh)
    output = model(features, adj)

    res = {}
    for node in leaf:
        node[0] = idx_map[node[0]]
        node[3] = output[node[0]].argmax()
        if node[4] not in res or node[2] > res[node[4]][2]:
            res[node[4]] = node

    return res

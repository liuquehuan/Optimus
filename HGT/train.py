import time

import numpy as np # type: ignore
import torch.optim as optim # type: ignore
import torch.nn as nn # type: ignore
import torch ## type: ignore
import argparse

# 创建默认参数对象，用于模块导入时
class DefaultArgs:
    def __init__(self):
        self.epochs = 10
        self.lr = 0.005
        self.weight_decay = 5e-4
        self.dropout = 0
        self.alpha = 0.2

# 默认参数对象
args = DefaultArgs()

# 只在直接运行时解析命令行参数
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--epochs', type=int, default=10, help='Number of epochs to train.')
    parser.add_argument('--lr', type=float, default=0.005, help='Initial learning rate.')
    parser.add_argument('--weight_decay', type=float, default=5e-4, help='Weight decay (L2 loss on parameters).')
    parser.add_argument('--dropout', type=float, default=0, help='Dropout rate (1 - keep probability).')
    parser.add_argument('--alpha', type=float, default=0.2, help='Alpha for the leaky_relu.')
    args = parser.parse_args()

# args.cuda = not args.no_cuda and torch.cuda.is_available()

import joblib
import os
from sklearn import preprocessing
from sklearn.pipeline import Pipeline
from models import HGT
from featurize import TreeFeaturizer

CUDA = torch.cuda.is_available()
print("CUDA:", CUDA)

def _nn_path(base):
    return os.path.join(base, "nn_weights")

def _x_transform_path(base):
    return os.path.join(base, "x_transform")

def _y_transform_path(base):
    return os.path.join(base, "y_transform")

def _channels_path(base):
    return os.path.join(base, "channels")

def _n_path(base):
    return os.path.join(base, "n")

def _node_level_path(base):
    return os.path.join(base, "node_level")

def _inv_log1p(x):
    return np.exp(x) - 1

# random.seed(args.seed)
# np.random.seed(args.seed)
# torch.manual_seed(args.seed)
# if args.cuda:
#     torch.cuda.manual_seed(args.seed)

import sys
sys.path.append(r".")
from graphembedding import load_data_from_matrix

def collate(x):
    trees = []
    targets = []

    for tree, target in x:
        trees.append(tree)
        targets.append(target)

    targets = torch.tensor(targets)
    return trees, targets


class Optimus:
    def __init__(self, node_level, verbose=False):
        self.__verbose = verbose
        self.__node_level = node_level

        ## log1p：加1取对数
        ## 第二个参数是log1p的逆变换
        log_transformer = preprocessing.FunctionTransformer(
            np.log1p, _inv_log1p,
            validate=True)
        ## 归一化
        scale_transformer = preprocessing.MinMaxScaler()

        self.__pipeline = Pipeline([("log", log_transformer),
                                    ("scale", scale_transformer)])
        
        self.__tree_transform = TreeFeaturizer()
        # self.__have_cache_data = have_cache_data
        # self.__in_channels = self.__tree_transform.num_operators() + 2
        self.__in_channels = 3 + 13

        if self.__node_level:
            self.__nclass = 3
        else:
            self.__nclass = 35

        self.__net = HGT(nfeat=self.__in_channels, 
                nclass=self.__nclass, 
                dropout=args.dropout, 
                alpha=args.alpha)
        self.__n = 0
        

    def __log(self, *args):
        if self.__verbose:
            print(*args)

    def num_items_trained_on(self):
        return self.__n
            
    def load(self, path):
        with open(_n_path(path), "rb") as f:
            self.__n = joblib.load(f)
        with open(_channels_path(path), "rb") as f:
            self.__in_channels = joblib.load(f)
        with open(_node_level_path(path), "rb") as f:
            self.__node_level = joblib.load(f)
        self.__nclass = 3 if self.__node_level else 35
            
        self.__net = HGT(nfeat=self.__in_channels, 
                nclass=self.__nclass, 
                dropout=args.dropout, 
                alpha=args.alpha)
        self.__net.load_state_dict(torch.load(_nn_path(path)))
        self.__net.eval()
        
        with open(_y_transform_path(path), "rb") as f:
            self.__pipeline = joblib.load(f)
        with open(_x_transform_path(path), "rb") as f:
            self.__tree_transform = joblib.load(f)

    def save(self, path):
        # try to create a directory here
        os.makedirs(path, exist_ok=True)
        
        torch.save(self.__net.state_dict(), _nn_path(path))
        with open(_y_transform_path(path), "wb") as f:
            joblib.dump(self.__pipeline, f)
        with open(_x_transform_path(path), "wb") as f:
            joblib.dump(self.__tree_transform, f)
        with open(_channels_path(path), "wb") as f:
            joblib.dump(self.__in_channels, f)
        with open(_n_path(path), "wb") as f:
            joblib.dump(self.__n, f)
        with open(_node_level_path(path), "wb") as f:
            joblib.dump(self.__node_level, f)


    def train(self, labels, features, adj, model, optimizer):
        if isinstance(labels, int) or isinstance(labels, np.int64):
            labels = torch.tensor([labels], dtype=torch.long)

        # if CUDA:
        #     labels = labels.cuda()

        output = model(features, adj)
        if output.dim() == 1:
            output = output.unsqueeze(0)
        
        loss_fn = nn.CrossEntropyLoss()
        loss_train = loss_fn(output, labels)

        optimizer.zero_grad()
        loss_train.backward()
        optimizer.step()

        return round(loss_train.item(), 4)


    def extract_subtree(self, plan, conflict_operators, oid):
        node_matrix = []
        edge_matrix = []

        tree = self.__tree_transform.transform(plan, oid)

        def recurse(tree):
            nonlocal node_matrix
            nonlocal edge_matrix
            ## leaf
            if len(tree) == 3:
                node_feature, max_oid, rels = tree
                node_matrix = [node_feature] + node_matrix
                return
            
            node_feature, left, right, join_type, max_oid, rels = tree
            root_oid = node_feature[0]

            node_matrix = [node_feature] + node_matrix
            edge_matrix = [[left[0][0], root_oid, 1], [right[0][0], root_oid, 1]] + edge_matrix

            recurse(left)
            recurse(right)

        recurse(tree)

        ## 这里返回的oid需要是下一个可用的oid
        return node_matrix, edge_matrix, conflict_operators, tree[-2] + 1
    

    def extract_tree(self, tree, conflict_operators):
        node_matrix = []
        edge_matrix = []

        def recurse(tree):
            nonlocal node_matrix
            nonlocal edge_matrix
            ## leaf
            if len(tree) == 3:
                node_feature, max_oid, rels = tree
                node_matrix = [node_feature] + node_matrix
                return
            
            node_feature, left, right, join_type, max_oid, rels = tree
            root_oid = node_feature[0]

            node_matrix = [node_feature] + node_matrix
            edge_matrix = [[left[0][0], root_oid, 1], [right[0][0], root_oid, 1]] + edge_matrix

            recurse(left)
            recurse(right)

        recurse(tree)

        ## 这里返回的oid需要是下一个可用的oid
        return node_matrix, edge_matrix, conflict_operators, tree[-2] + 1


    def run_train_upd(self, X, y):
        self.__tree_transform.fit(X)
        trees = [self.__tree_transform.transform(x, 0) for x in X]
        _features, _adj = [], []

        if not self.__node_level:
            if isinstance(y, list):
                y = np.array(y)
            self.__n = len(X)
            # transform the set of trees into feature vectors using a log
            # (assuming the tail behavior exists, TODO investigate
            #  the quantile transformer from scikit)
            # y = self.__pipeline.fit_transform(y.reshape(-1, 1)).astype(np.float32)
            
        else:
            ## 拆出所有的join子树
            _X = []
            y = []

            def is_leaf(x):
                return len(x) == 3

            def recurse(x):
                if is_leaf(x):
                    return
                my_type = x[-3]
                if my_type < 3:
                    _X.append(x)
                    y.append(my_type)
                    return
                
                recurse(x[1])
                recurse(x[2])
            
            for x in trees:
                recurse(x)

            trees = _X
            y = np.array(y)
            self.__n = len(trees)

        for sample in trees:
            vmatrix = []
            ematrix = []
            conflict_operators = {}

            ## node_matrix的元素是node_feature，node_feature第一维是oid
            node_matrix, edge_matrix, conflict_operators, oid = self.extract_tree(sample, conflict_operators)
            vmatrix = vmatrix + node_matrix
            ematrix = ematrix + edge_matrix

            # ematrix = add_across_plan_relations(conflict_operators, ematrix)

            if len(ematrix) == 0:
                ematrix = [[0, 0, 0]]
            adj, features = load_data_from_matrix(np.array(vmatrix, dtype=np.float32), np.array(ematrix, dtype=np.float32))
            _features.append(features)
            _adj.append(adj)

        # if CUDA:
        #     self.__net = self.__net.cuda()

        optimizer = optim.Adam(self.__net.parameters(), lr=args.lr, weight_decay=args.weight_decay)

        losses = []
        for epoch in range(args.epochs):
            loss_accum = 0
            t = time.time()
            for labels, features, adj in zip(y, _features, _adj):
                # if CUDA:
                #     features = features.cuda()
                #     adj = adj.cuda()
                loss_train = self.train(labels, features, adj, model=self.__net, optimizer=optimizer)
                loss_accum += loss_train
            loss_accum /= len(X)
            print('Epoch: {:04d}'.format(epoch + 1),
            'loss_train: {:.4f}'.format(loss_accum),
            'time: {:.4f}s'.format(time.time() - t))
            losses.append(loss_accum)
            if len(losses) > 10 and losses[-1] < 0.1:
                last_two = np.min(losses[-2:])
                if last_two > losses[-10] or (losses[-10] - last_two < 0.0001):
                    print("Stopped training from convergence condition at epoch", epoch)
                    break

        return self.__net


    def run_test_upd(self, X):
        oid = 0
        tree = self.__tree_transform.transform(X, oid)

        if not self.__node_level:
            vmatrix = []
            ematrix = []
            conflict_operators = {}
            node_matrix, edge_matrix, conflict_operators, oid = self.extract_tree(tree, conflict_operators)
            vmatrix = vmatrix + node_matrix
            ematrix = ematrix + edge_matrix
            # ematrix = add_across_plan_relations(conflict_operators, ematrix)

            if len(ematrix) == 0:
                ematrix = [[0, 0, 0]]
            adj, features = load_data_from_matrix(np.array(vmatrix, dtype=np.float32), np.array(ematrix, dtype=np.float32))

            self.__net.eval()
            output = self.__net(features, adj)

            return output.cpu().detach().numpy()

        hint = ""

        def is_leaf(x):
            return len(x) == 3

        def recurse(x):
            nonlocal hint
            if is_leaf(x):
                ## __featurize_scan返回值
                ## return (np.concatenate((arr, self.__stats(node))),
                ##         self.__relation_name(node))
                ## self.__relation_name(node): str
                # return (x[0][5] == 1, [x[1][0]])
                return [x[-1][0]]
            
            # all_cscan, rel = True, []
            # lscan, lrel = recurse(x[1])
            # rscan, rrel = recurse(x[2])
            # all_cscan &= lscan
            # all_cscan &= rscan
            # rel.extend(lrel)
            # rel.extend(rrel)
            rel = []
            lrel = recurse(x[1])
            rrel = recurse(x[2])
            rel.extend(lrel)
            rel.extend(rrel)
            my_type = x[-3]

            ## 逻辑：如果儿子全是列扫就是vjoin（这种情况好像不需要特别处理，应该会自动选择到vjoin），否则要预测。hint包含join算子和表名
            ## todo：怎么收集儿子扫描类别和表名
            ## todo：根据分类结果修改type

            if is_leaf(x[1]) and is_leaf(x[2]):
                assert my_type < 3
            # if my_type < 3:

                subtree = x
                # if not all_cscan:
                vmatrix = []
                ematrix = []
                conflict_operators = {}
                node_matrix, edge_matrix, conflict_operators, oid = self.extract_tree(subtree, conflict_operators)
                vmatrix = vmatrix + node_matrix
                ematrix = ematrix + edge_matrix
                # ematrix = add_across_plan_relations(conflict_operators, ematrix)

                if len(ematrix) == 0:
                    ematrix = [[0, 0, 0]]
                adj, features = load_data_from_matrix(np.array(vmatrix, dtype=np.float32), np.array(ematrix, dtype=np.float32))

                self.__net.eval()
                pred = self.__net(features, adj).cpu().detach().numpy()
                JOIN_TYPES = ["NestLoop", "HashJoin", "MergeJoin"]
                pred_idx = np.argmax(pred)
                hint += f"{JOIN_TYPES[pred_idx]}({' '.join(rel)})\n"
                # else:
                #     hint += "VJION("
                #     for i in range(len(rel)):
                #         hint += rel[i] + ","
                #     hint += ")"

            return rel
            
        recurse(tree)
        return hint
    
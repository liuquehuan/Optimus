import numpy as np
import torch
import torch.optim
import joblib
import os
from sklearn import preprocessing
from sklearn.pipeline import Pipeline
import json
from torch.utils.data import DataLoader
import net
from featurize import TreeFeaturizer

CUDA = torch.cuda.is_available()

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

def collate(x):
    trees = []
    targets = []

    for tree, target in x:
        trees.append(tree)
        targets.append(target)

    targets = torch.tensor(targets)
    return trees, targets

class Model:
    # def __init__(self, verbose=False, have_cache_data=False):
    def __init__(self, verbose=False, node_level=False):
        self.__verbose = verbose
        self.__node_level = node_level

        log_transformer = preprocessing.FunctionTransformer(
            np.log1p, _inv_log1p,
            validate=True)
        scale_transformer = preprocessing.MinMaxScaler()

        self.__pipeline = Pipeline([("log", log_transformer),
                                    ("scale", scale_transformer)])
        
        self.__tree_transform = TreeFeaturizer()
        self.__in_channels = 3 + 13
        self.__net = net.BaoNet(self.__in_channels, self.__node_level)
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
            
        self.__net = net.BaoNet(self.__in_channels, self.__node_level)
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

    def fit(self, X, y):

        self.__tree_transform.fit(X)
        X = self.__tree_transform.transform(X)
        if not self.__node_level:
            if isinstance(y, list):
                y = np.array(y)

            # X = [json.loads(x) if isinstance(x, str) else x for x in X]
            self.__n = len(X)
            
        else:
            _X = []
            y = []

            def is_leaf(x):
                return len(x) < 3

            def recurse(x):
                if is_leaf(x):
                    return
                my_type = x[-2]
                if my_type < 3:
                # if is_leaf(x[1]) and is_leaf(x[2]):
                #     assert 1 in my_type

                    ## modification starts here
                    # x[0][0:3] = np.zeros(3)
                    ## modification ends here

                    _X.append(x)
                    y.append(my_type)
                    return
                
                recurse(x[1])
                recurse(x[2])
            
            for x in X:
                recurse(x)

            X = _X
            y = np.array(y)
            self.__n = len(X)

        pairs = list(zip(X, y))
        dataset = DataLoader(pairs,
                             batch_size=256,
                             shuffle=True,
                             collate_fn=collate)

        # check the initial number of channels
        for inp, _ in dataset:
            in_channels = inp[0][0].shape[0]
            break

        self.__log("Initial input channels:", in_channels)

        # if self.__have_cache_data:
        #     assert in_channels == self.__tree_transform.num_operators() + 3
        # else:
        assert in_channels == 16

        # self.__net = net.BaoNet(in_channels)
        # self.__in_channels = in_channels
        if CUDA:
            self.__net = self.__net.cuda()

        # if self.__node_level:
        #     optimizer = torch.optim.Adam(self.__net.parameters(), lr=0.0001)
        # else:
        #     optimizer = torch.optim.Adam(self.__net.parameters())
        optimizer = torch.optim.Adam(self.__net.parameters())
        loss_fn = torch.nn.CrossEntropyLoss()
        
        losses = []
        for epoch in range(200):
            loss_accum = 0
            for x, y in dataset:
                if CUDA:
                    y = y.cuda()
                y_pred = self.__net(x)
                loss = loss_fn(y_pred, y)
                loss_accum += loss.item()
        
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

            loss_accum /= len(dataset)
            losses.append(loss_accum)
            if epoch % 15 == 0:
                self.__log("Epoch", epoch, "training loss:", loss_accum)

            # stopping condition
            if len(losses) > 10 and losses[-1] < 0.1:
                last_two = np.min(losses[-2:])
                if last_two > losses[-10] or (losses[-10] - last_two < 0.0001):
                    self.__log("Stopped training from convergence condition at epoch", epoch)
                    break
        else:
            self.__log("Stopped training after max epochs")

    def predict(self, X):
        if not isinstance(X, list):
            X = [X]
        X = [json.loads(x) if isinstance(x, str) else x for x in X]

        X = self.__tree_transform.transform(X)
        
        self.__net.eval()
        
        if not self.__node_level:
            pred = self.__net(X).cpu().detach().numpy()
            return pred
        
        else:
            hint = ""

            def is_leaf(x):
                if len(x) < 3:
                    return True
                my_type = x[-2]
                return my_type > 3

            def recurse(x):
                nonlocal hint
                if is_leaf(x):
                    ## return (np.concatenate((arr, self.__stats(node))),
                    ##         self.__relation_name(node))
                    ## self.__relation_name(node): str
                    # return (x[0][5] == 1, [x[1][0]])
                    if len(x) < 3:
                        return [x[1][0]]
                    rel = []
                    lrel = recurse(x[1])
                    rrel = recurse(x[2])
                    rel.extend(lrel)
                    rel.extend(rrel)
                    return rel
                
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
                my_type = x[-2]

                if is_leaf(x[1]) and is_leaf(x[2]):
                    assert my_type < 3
                # if my_type < 3:

                    subtree = [x]
                    # if not all_cscan:
                    pred = self.__net(subtree).cpu().detach().numpy()
                    JOIN_TYPES = ["NestLoop", "HashJoin", "MergeJoin"]
                    pred_idx = np.argmax(pred, axis=1)[0]
                    hint += f"{JOIN_TYPES[pred_idx]}({' '.join(set(rel))})\n"
                    # else:
                    #     hint += "VJION("
                    #     for i in range(len(rel)):
                    #         hint += rel[i] + ","
                    #     hint += ")"

                return rel
                
            assert len(X) == 1
            recurse(X[0])
            
            return hint


def train_and_save_model(fn, X, y, verbose=True, node_level=False, reg=None):
    import time
    
    if reg is None:
        reg = Model(verbose=verbose, node_level=node_level)

    start = time.time()
    reg.fit(X, y)
    end = time.time()
    print("training:", end - start)
    reg.save(fn)
    return reg

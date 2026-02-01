import scipy.sparse as sp ## type: ignore
import torch ## type: ignore
import numpy as np ## type: ignore
from constants import NODE_DIM
import torch.nn.functional as F ## type: ignore


# ## Load Data


def encode_onehot(labels):
    classes = set(labels)
    classes_dict = {c: np.identity(len(classes))[i, :] for i, c in
                    enumerate(classes)}
    labels_onehot = np.array(list(map(classes_dict.get, labels)),
                             dtype=np.int32)
    return labels_onehot


def normalize(mx):
    """Row-normalize sparse matrix"""
    rowsum = np.array(mx.sum(1))
    r_inv = np.power(rowsum, -1).flatten()
    r_inv[np.isinf(r_inv)] = 0.
    r_mat_inv = sp.diags(r_inv)
    mx = r_mat_inv.dot(mx)
    return mx


def accuracy(output, labels):
    preds = output.max(1)[1].type_as(labels)
    correct = preds.eq(labels).double()
    correct = correct.sum()
    return correct / len(labels)


def sparse_mx_to_torch_sparse_tensor(sparse_mx):
    """Convert a scipy sparse matrix to a torch sparse tensor."""
    sparse_mx = sparse_mx.tocoo().astype(np.float32)
    indices = torch.from_numpy(
        np.vstack((sparse_mx.row, sparse_mx.col)).astype(np.int64))
    values = torch.from_numpy(sparse_mx.data)
    shape = torch.Size(sparse_mx.shape)
    return torch.sparse.FloatTensor(indices, values, shape)


def load_data_from_matrix(vmatrix, ematrix):
    idx_features_labels = vmatrix

    # encode vertices
    features = sp.csr_matrix(idx_features_labels[:, 1:-1], dtype=np.float32) ## 看上去是把第0维（节点oid）和最后一维（runtime）去掉

    # encode labels
    # labels = encode_onehot(idx_features_labels[:, -2])
    labels = idx_features_labels[:, -1] ## 把节点的runtime作为标签

    # encode edges
    idx = np.array(idx_features_labels[:, 0], dtype=np.int32) ## oid

    idx_map = {j: i for i, j in enumerate(idx)}
    edges_unordered = ematrix[:, :-1] ## 把最后一维（权重）去掉

    # print(list(map(idx_map.get, edges_unordered.flatten())))
    # print(edges_unordered.flatten())

    edges = np.array(list(map(idx_map.get, edges_unordered.flatten())),
                     dtype=np.int32).reshape(edges_unordered.shape) ## 把节点编号映射到新的索引

    # edges (weights are computed in gcn)

    # modified begin.
    edges_value = ematrix[:, -1:] ## 把权重放到第0维？
    # modified end.

    # adj = sp.coo_matrix((np.ones(edges.shape[0]), (edges[:, 0], edges[:, 1])),shape=(node_dim, node_dim),dtype=np.float32)
    # print("old_adj = ", adj)
    ## 看上去adj是邻接矩阵（以稀疏形式存储）
    adj = sp.coo_matrix((edges_value[:, 0], (edges[:, 0], edges[:, 1])), shape=(NODE_DIM, NODE_DIM), dtype=np.float32) ## 构造稀疏矩阵
    # print("new_adj = ", adj)
    # print(adj.shape)

    # build symmetric adjacency matrix
    # adj = adj + adj.T.multiply(adj.T > adj) - adj.multiply(adj.T > adj)

    features = normalize(features)
    adj = normalize(adj + sp.eye(adj.shape[0])) ## 加上一个单位阵，使每个节点都产生自环

    operator_num = adj.shape[0]
    idx_train = range(int(0.8 * operator_num))
    # print("idx_train", idx_train)
    idx_val = range(int(0.8 * operator_num), int(0.9 * operator_num))
    idx_test = range(int(0.9 * operator_num), int(operator_num))

    features = torch.FloatTensor(np.array(features.todense()))
    # labels = torch.LongTensor(np.where(labels)[1])
    adj = sparse_mx_to_torch_sparse_tensor(adj)

    idx_train = torch.LongTensor(idx_train)
    idx_val = torch.LongTensor(idx_val)
    idx_test = torch.LongTensor(idx_test)

    # padding to the same size
    # print(features.shape)
    # print(node_dim - features.shape[0])
    dim = (0, 0, 0, NODE_DIM - features.shape[0])
    features = F.pad(features, dim, "constant", value=0) ## 填充，否则无法左乘邻接矩阵进行message passing

    # labels = labels.astype(np.float32)
    labels = torch.from_numpy(labels)
    # print(labels[idx_train].dtype)
    labels.unsqueeze(1)
    # labels = labels * 10 ## 为什么要乘10？
    labels = F.pad(labels, [0, NODE_DIM - labels.shape[0]], "constant", value=4)

    # print("features", features.shape)
    return adj, features, labels, idx_train, idx_val, idx_test, idx_map

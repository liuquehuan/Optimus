import scipy.sparse as sp ## type: ignore
import torch ## type: ignore
import numpy as np ## type: ignore


def normalize(mx):
    rowsum = np.array(mx.sum(1))
    r_inv = np.power(rowsum, -1).flatten()
    r_inv[np.isinf(r_inv)] = 0.
    r_mat_inv = sp.diags(r_inv)
    mx = r_mat_inv.dot(mx)
    return mx


def load_data_from_matrix(vmatrix, ematrix):
    idx_features_labels = vmatrix

    # encode vertices
    features, type = np.array(idx_features_labels[:, 1:3]), np.array(idx_features_labels[:, 3:])

    # encode edges
    idx = np.array(idx_features_labels[:, 0], dtype=np.int32) ## oid

    idx_map = {j: i for i, j in enumerate(idx)}
    edges_unordered = ematrix[:, :-1] if ematrix.size else np.empty((0, 2))
    edges_value = ematrix[:, -1:] if ematrix.size else np.empty((0, 1))

    valid_edges = []
    valid_values = []
    for edge, value in zip(edges_unordered, edges_value):
        src = idx_map.get(edge[0])
        dst = idx_map.get(edge[1])
        if src is None or dst is None:
            continue
        valid_edges.append([src, dst])
        valid_values.append(value[0])

    edges = np.array(valid_edges, dtype=np.int32).reshape((-1, 2))
    edges_value = np.array(valid_values, dtype=np.float32)

    # adj = sp.coo_matrix((np.ones(edges.shape[0]), (edges[:, 0], edges[:, 1])),shape=(node_dim, node_dim),dtype=np.float32)
    adj = sp.coo_matrix((edges_value, (edges[:, 0], edges[:, 1])), shape=(idx.shape[0], idx.shape[0]), dtype=np.float32)

    # build symmetric adjacency matrix
    # adj = adj + adj.T.multiply(adj.T > adj) - adj.multiply(adj.T > adj)

    features = normalize(features)
    features = np.concatenate((features, type), axis=1)
    adj = normalize(adj + sp.eye(adj.shape[0]))

    features = torch.FloatTensor(features)
    # adj = sparse_mx_to_torch_sparse_tensor(adj)
    adj = torch.FloatTensor(np.array(adj.todense()))

    # padding to the same size
    # dim = (0, 0, 0, NODE_DIM - features.shape[0])

    return adj, features

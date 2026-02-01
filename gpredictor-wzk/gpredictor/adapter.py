import json
import os
from typing import Dict, List, Tuple, Optional

import numpy as np
import torch
import torch.nn.functional as F

from constants import NODE_DIM, args
from GCN import get_model, get_optimizer
from nodeutils import extract_plan, add_across_plan_relations
from graphembedding import normalize, sparse_mx_to_torch_sparse_tensor
import scipy.sparse as sp
import torch


def _safe_eval(line: str):
    line = line.strip()
    if line.lower() == "none":
        return None
    try:
        return json.loads(line)
    except Exception:
        try:
            return eval(line)
        except Exception:
            return None


def _plan_duration(plan_obj: dict) -> float:
    if plan_obj is None:
        return 0.0
    if "Execution Time" in plan_obj:
        return float(plan_obj["Execution Time"])
    if "Actual Total Time" in plan_obj.get("Plan", {}):
        return float(plan_obj["Plan"]["Actual Total Time"])
    if "Plan" in plan_obj and "Total Cost" in plan_obj["Plan"]:
        return float(plan_obj["Plan"]["Total Cost"])
    return 0.0


def _compute_start_times(plans: List[dict], strategy: str = "zero") -> List[float]:

    if strategy == "zero":
        return [0.0 for _ in plans]

    if strategy == "sequential_exec":
        starts = []
        cur = 0.0
        for p in plans:
            starts.append(cur)
            cur += _plan_duration(p)
        return starts

 
    return [0.0 for _ in plans]


def build_graph_from_plans(
    plans: List[dict],
    mp_optype: Optional[Dict[str, int]] = None,
    base_start: float = 0.0,
    start_strategy: str = "zero",
    start_times: Optional[List[float]] = None,
) -> Tuple[np.ndarray, np.ndarray, List[int], Dict[str, int]]:
    if mp_optype is None:
        mp_optype = {'Aggregate': 0, 'Nested Loop': 1, 'Index Scan': 2, 'Hash Join': 3, 'Seq Scan': 4, 'Hash': 5,
                     'Update': 6}

    if start_times is None:
        start_times = _compute_start_times(plans, strategy=start_strategy)

    vmatrix: List[List[float]] = []
    ematrix: List[List[float]] = []
    target_oids: List[int] = []
    mergematrix: List[List[float]] = []
    conflict_operators: Dict[str, list] = {}
    oid = 0
    min_timestamp = -1

    for idx, plan_obj in enumerate(plans):
        if plan_obj is None:
            continue
        sample = {
            "start_time": base_start + (start_times[idx] if idx < len(start_times) else 0.0),
            "plan": plan_obj
        }
        start_time, node_matrix, edge_matrix, conflict_operators, node_merge_matrix, mp_optype, oid, min_timestamp = \
            extract_plan(sample, conflict_operators, mp_optype, oid, min_timestamp)

        if idx == 0:
            target_oids.extend([n[0] for n in node_matrix])

        vmatrix.extend(node_matrix)
        ematrix.extend(edge_matrix)
        mergematrix.extend(node_merge_matrix)

    ematrix = add_across_plan_relations(conflict_operators, ematrix)

    return np.array(vmatrix, dtype=np.float32), np.array(ematrix, dtype=np.float32), target_oids, mp_optype


def matrices_to_tensors(
    vmatrix: np.ndarray,
    ematrix: np.ndarray,
):
    idx_features_labels = vmatrix
    features = sp.csr_matrix(idx_features_labels[:, 0:-1], dtype=np.float32)

    labels = idx_features_labels[:, -1].astype(np.float32)
    idx = np.array(idx_features_labels[:, 0], dtype=np.int32)
    idx_map = {j: i for i, j in enumerate(idx)}

    node_dim = len(idx_map) if len(idx_map) > 0 else NODE_DIM
    max_edge_idx = -1
    if len(ematrix) > 0:
        edge_rows = []
        edge_cols = []
        edge_vals = []
        for raw_src, raw_dst, val in ematrix:
            src = idx_map.get(int(raw_src))
            dst = idx_map.get(int(raw_dst))
            if src is None or dst is None:
                continue
            edge_rows.append(src)
            edge_cols.append(dst)
            edge_vals.append(val)
        if len(edge_rows) > 0:
            max_edge_idx = max(max_edge_idx, max(max(edge_rows), max(edge_cols)))
        if len(edge_rows) == 0:
            adj = sp.coo_matrix((node_dim, node_dim), dtype=np.float32)
        else:
            node_dim = max(node_dim, max_edge_idx + 1)
            adj = sp.coo_matrix((edge_vals, (edge_rows, edge_cols)), shape=(node_dim, node_dim), dtype=np.float32)
    else:
        adj = sp.coo_matrix((node_dim, node_dim), dtype=np.float32)

    features = normalize(features)
    adj = normalize(adj + sp.eye(adj.shape[0]))

    operator_num = adj.shape[0]
    idx_train = range(int(0.8 * operator_num))
    idx_val = range(int(0.8 * operator_num), int(0.9 * operator_num))
    idx_test = range(int(0.9 * operator_num), int(operator_num))

    features = torch.FloatTensor(np.array(features.todense()))
    adj = sparse_mx_to_torch_sparse_tensor(adj)

    idx_train = torch.LongTensor(idx_train)
    idx_val = torch.LongTensor(idx_val)
    idx_test = torch.LongTensor(idx_test)

    dim = (0, 0, 0, operator_num - features.shape[0])
    features = F.pad(features, dim, "constant", value=0)

    labels = torch.from_numpy(labels)
    labels = labels * 10
    labels = F.pad(labels, [0, operator_num - labels.shape[0]], "constant", value=0)

    return adj, features, labels, idx_train, idx_val, idx_test, idx_map


def train_model_from_graph_data(
    graph_root: str,
    limit_per_dir: Optional[int] = None,
    device: Optional[torch.device] = None,
    start_strategy: str = "zero",
):
    vmatrix_all: List[List[float]] = []
    ematrix_all: List[List[float]] = []
    mp_optype: Dict[str, int] = None

    subdirs = sorted([d for d in os.listdir(graph_root) if os.path.isdir(os.path.join(graph_root, d))])
    for sub in subdirs:
        paths = sorted(os.listdir(os.path.join(graph_root, sub)))
        if limit_per_dir is not None:
            paths = paths[:limit_per_dir]
        for fname in paths:
            fpath = os.path.join(graph_root, sub, fname)
            with open(fpath, "r") as f:
                lines = f.readlines()
            plans = []
            for line in lines:
                plan_obj = _safe_eval(line)
                if plan_obj is not None:
                    plans.append(plan_obj)
            if len(plans) == 0:
                continue
            vmatrix, ematrix, _, mp_optype = build_graph_from_plans(
                plans, mp_optype=mp_optype, start_strategy=start_strategy
            )
            vmatrix_all.extend(vmatrix.tolist())
            ematrix_all.extend(ematrix.tolist())

    vmatrix_arr = np.array(vmatrix_all, dtype=np.float32)
    ematrix_arr = np.array(ematrix_all, dtype=np.float32) if len(ematrix_all) > 0 else np.zeros((0, 3),
                                                                                               dtype=np.float32)
    adj, features, labels, idx_train, idx_val, idx_test, _ = matrices_to_tensors(vmatrix_arr, ematrix_arr)

    model = get_model(feature_num=features.shape[1], hidden=args.hidden, nclass=NODE_DIM, dropout=args.dropout)
    if device is not None:
        model = model.to(device)
        features = features.to(device)
        adj = adj.to(device)
        labels = labels.to(device)
        idx_train = idx_train.to(device)
        idx_val = idx_val.to(device)
        idx_test = idx_test.to(device)
    optimizer = get_optimizer(model=model, lr=args.lr, weight_decay=args.weight_decay)

    model.train()
    ok_times = 0
    for epoch in range(args.epochs):
        optimizer.zero_grad()
        output = model(features, adj)
        loss_train = F.mse_loss(output[idx_train], labels[idx_train].unsqueeze(1))
        loss_train.backward()
        optimizer.step()
        if loss_train.item() < 0.002:
            ok_times += 1
        if ok_times >= 20:
            break
    return model, mp_optype


def train_and_save_model(
    graph_root: str,
    model_path: str,
    limit_per_dir: Optional[int] = None,
    device: Optional[torch.device] = None,
    start_strategy: str = "zero",
):
    model, mp_optype = train_model_from_graph_data(
        graph_root, limit_per_dir=limit_per_dir, device=device, start_strategy=start_strategy
    )
    torch.save({"state_dict": model.state_dict(), "mp_optype": mp_optype}, model_path)
    return model, mp_optype


def load_model(model_path: str, device: Optional[torch.device] = None):
    ckpt = torch.load(model_path, map_location=device or "cpu")
    feature_num = 3  # vmatrix 每行: [oid, op_type_id, run_cost, label]
    model = get_model(feature_num=feature_num, hidden=args.hidden, nclass=NODE_DIM, dropout=args.dropout)
    model.load_state_dict(ckpt["state_dict"])
    if device is not None:
        model = model.to(device)
    mp_optype = ckpt.get("mp_optype", None)
    return model, mp_optype


def predict_latency_for_plan(
    model,
    plan_json: dict,
    mp_optype: Dict[str, int],
    device: Optional[torch.device] = None,
    concurrent_plans: Optional[List[dict]] = None,
    start_strategy: str = "zero",
) -> float:
    plans = [plan_json] + (concurrent_plans or [])
    vmatrix, ematrix, target_oids, mp_optype = build_graph_from_plans(
        plans, mp_optype=mp_optype, start_strategy=start_strategy
    )
    adj, features, labels, idx_train, idx_val, idx_test, idx_map = matrices_to_tensors(vmatrix, ematrix)

    if device is not None:
        adj = adj.to(device)
        features = features.to(device)

    model.eval()
    with torch.no_grad():
        output = model(features, adj).squeeze()
    pred_nodes = []
    for oid in target_oids:
        if oid in idx_map:
            pred_nodes.append(output[idx_map[oid]].item())
    if len(pred_nodes) == 0:
        return float("inf")
    return max(pred_nodes) / 10.0


__all__ = [
    "train_model_from_graph_data",
    "predict_latency_for_plan",
    "build_graph_from_plans",
    "_safe_eval",
]


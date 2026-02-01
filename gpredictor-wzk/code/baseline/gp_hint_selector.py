"""
离线使用 graph_data（无需数据库），基于 gpredictor 为同一查询的候选计划选出预测耗时最小者。

假设目录结构：
code/graph_data/{query_id}/{workload_id}.txt
每个 txt：
  - 第一行：目标 OLAP 计划（某个 hint 产出的计划），可能为 "none"
  - 后续行：同一时间并发的事务计划（可为空）

逻辑：
  1) 用 graph_data 训练 gpredictor。
  2) 对每个 query_id 子目录内的所有 txt，分别预测 (OLAP + 并发) 的耗时。
  3) 为每个 query_id 选出预测耗时最小的 workload_id（可视为最佳 hint）。
输出：
  - 控制台：每个 query_id 的最优 workload_id 与预测值
  - 保存 results.csv（query_id, workload_id, pred_time）
"""

import argparse
import csv
import os
import sys
from typing import List, Dict, Any, Optional

# 路径设置
CUR_DIR = os.path.dirname(__file__)
PROJ_ROOT = os.path.abspath(os.path.join(CUR_DIR, "..", ".."))
for p in [PROJ_ROOT, os.path.join(PROJ_ROOT, "gpredictor"), os.path.join(PROJ_ROOT, "code")]:
    if p not in sys.path:
        sys.path.append(p)

from gpredictor.adapter import (
    train_model_from_graph_data,
    predict_latency_for_plan,
    _safe_eval,
)


def load_plans_from_file(fpath: str) -> List[Optional[Dict[str, Any]]]:
    """读取单个 workload 文件，返回计划列表（保持 None 以便后续过滤）。"""
    plans: List[Optional[Dict[str, Any]]] = []
    with open(fpath, "r") as f:
        for line in f:
            obj = _safe_eval(line)
            plans.append(obj)
    return plans


def predict_dir(model, mp_optype, dir_path: str) -> List[Dict[str, Any]]:
    """对 query_id 目录内的全部 workload 预测，返回列表字典。"""
    records = []
    for fname in sorted(os.listdir(dir_path)):
        if not fname.endswith(".txt"):
            continue
        fpath = os.path.join(dir_path, fname)
        plans = load_plans_from_file(fpath)
        if len(plans) == 0 or plans[0] is None:
            continue
        olap_plan = plans[0]
        concurrent = [p for p in plans[1:] if p is not None]
        pred = predict_latency_for_plan(model, olap_plan, mp_optype, concurrent_plans=concurrent)
        records.append({"workload": fname, "pred": pred})
    return records


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--graph_root", default=os.path.join(CUR_DIR, "..", "graph_data"),
                        help="graph_data 根目录，默认为 code/graph_data")
    parser.add_argument("--limit_per_dir", type=int, default=20,
                        help="训练时每个目录最多使用多少样本，防止过慢")
    parser.add_argument("--output", default="results.csv", help="输出 CSV 路径")
    args = parser.parse_args()

    graph_root = os.path.abspath(args.graph_root)
    print(f"training gpredictor from {graph_root}, limit_per_dir={args.limit_per_dir}")
    model, mp_optype = train_model_from_graph_data(graph_root, limit_per_dir=args.limit_per_dir)

    all_dirs = [d for d in sorted(os.listdir(graph_root)) if os.path.isdir(os.path.join(graph_root, d))]
    results = []
    for d in all_dirs:
        dir_path = os.path.join(graph_root, d)
        recs = predict_dir(model, mp_optype, dir_path)
        if len(recs) == 0:
            print(f"[query {d}] 无可用计划，跳过")
            continue
        best = min(recs, key=lambda x: x["pred"])
        results.append({"query_id": d, "workload": best["workload"], "pred": best["pred"]})
        print(f"[query {d}] best workload={best['workload']} pred={best['pred']:.4f}")

    # 写 CSV
    out_path = os.path.abspath(args.output)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["query_id", "workload", "pred"])
        writer.writeheader()
        writer.writerows(results)
    print(f"saved results to {out_path}")


if __name__ == "__main__":
    main()
"""
使用 gpredictor 模型对候选 hint 进行代价估计并选出最优 hint。
假设：
- 已有训练好的 gpredictor GCN 模型对象（同 gpredictor.train/run_train_upd 产出的 model）。
- 已有算子到 id 的映射 mp_optype（训练阶段生成）。

主要步骤：
1) 将 Postgres 计划(dict) 包装成 gpredictor 需要的样本格式（带 start_time）。
2) 通过 nodeutils.extract_plan 生成节点/边矩阵，再用 graphembedding.load_data_from_matrix 转成张量。
3) 前向推理得到节点级预测值，对同一计划的节点求和作为查询级代价。
4) 对所有候选 hint 取预测值最小者。
"""

import ast
from typing import Any, Dict, List, Tuple

import torch
import psycopg2  # type: ignore

from gpredictor.nodeutils import extract_plan
from gpredictor.graphembedding import load_data_from_matrix
from gpredictor.constants import args, NODE_DIM
from gpredictor.GCN import get_model


# -------------------------
# 数据读取与构图
# -------------------------

def parse_graph_data_line(line: str) -> Dict[str, Any]:
    """
    将 graph_data 中的一行（Python 字面量字典或字符串 "none"）转为 gpredictor 样本。
    start_time 使用行号或外部指定的基准时间，这里简单设为 0。
    """
    line = line.strip()
    if line.lower() == "none":
        return {}
    sample = ast.literal_eval(line)
    # graph_data 行内是 {'Plan': {...}, 'Execution Time': ...}
    # gpredictor 需要 {"start_time": float, "plan": {...}}
    return {"start_time": 0.0, "plan": sample}


def plan_to_graph_tensors(plan_json: Dict[str, Any], mp_optype: Dict[str, int]) -> Tuple[torch.Tensor, torch.Tensor]:
    """
    将单个计划 dict 转为 (features, adj) 张量以供模型推理。
    返回 features, adj；labels/idx 不需要，用 dummy 替代。
    """
    dummy_conflict = {}
    oid = 0
    min_ts = -1
    _, vmatrix, ematrix, _, _, mp_optype, oid, min_ts = extract_plan(
        {"start_time": 0.0, "plan": plan_json}, dummy_conflict, mp_optype, oid, min_ts
    )

    # load_data_from_matrix 会返回 (adj, features, labels, idx_train, idx_val, idx_test)
    adj, features, _, _, _, _ = load_data_from_matrix(
        torch.tensor(vmatrix, dtype=torch.float32).numpy(),
        torch.tensor(ematrix, dtype=torch.float32).numpy(),
    )
    return features, adj


# -------------------------
# 推理与 hint 选择
# -------------------------

def predict_query_cost(plan_json: Dict[str, Any], model: torch.nn.Module, mp_optype: Dict[str, int]) -> float:
    """
    用 gpredictor 模型预测单个计划的查询级代价。
    当前策略：对整棵计划的节点预测结果求和作为查询代价。
    """
    model.eval()
    with torch.no_grad():
        features, adj = plan_to_graph_tensors(plan_json, mp_optype)
        output = model(features, adj)  # shape: [NODE_DIM, 1]
        # 只累加真实节点（vmatrix 长度），其余是 padding；通过 features 非零行判断
        valid_mask = features.abs().sum(dim=1) > 0
        total_cost = output[valid_mask].sum().item()
    return total_cost


def select_best_hint(
    conn: psycopg2.extensions.connection,
    sql: str,
    params: Any,
    hint_list: List[str],
    model: torch.nn.Module,
    mp_optype: Dict[str, int],
) -> Tuple[int, float]:
    """
    对候选 hint 逐个生成 EXPLAIN (FORMAT JSON) 计划，使用 gpredictor 预测代价并返回最优 hint 的索引与预测值。
    """
    cur = conn.cursor()
    best_idx, best_cost = -1, float("inf")
    for idx, hint in enumerate(hint_list):
        explain_sql = hint + "EXPLAIN (FORMAT JSON)\n" + sql
        if params is None:
            cur.execute(explain_sql)
        else:
            cur.execute(explain_sql, params)
        plan_json = cur.fetchone()[0][0]
        cost = predict_query_cost(plan_json, model, mp_optype)
        if cost < best_cost:
            best_cost, best_idx = cost, idx
    cur.close()
    return best_idx, best_cost





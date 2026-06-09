import torch # type: ignore
import numpy as np # type: ignore
import sys
import argparse
sys.path.append("../../..")
from model import train_and_save_model


def load_plans(filepath: str):
    with open(filepath, "r") as f:
        plan_list = f.readlines()
        plan_list = [eval(x) for x in plan_list]
        return plan_list


def cost_split(plans, n, train_pos, num_stream):
    X_cost, y_cost, X_latency, y_latency = [], [], [], []
    failed_count = 0

    for i in range(num_stream):
        start = i * 13 * n
        for id in range(13):
            cur_id = start + id
            cost = []
            
            for _ in range(n):
                plan_cost = None
                if plans[cur_id] is not None:
                    plan_cost = plans[cur_id]['Execution Time'] if 'Execution Time' in plans[cur_id] else plans[cur_id]['Plan']['Total Cost']

                if plan_cost is not None:
                    cost.append(plan_cost)
                else:
                    cost.append(6000000000000)
                cur_id += 13
            
            if min(cost) == 6000000000000:
                failed_count += 1
            else:
                if 'Execution Time' in plans[start]:
                    y_latency.append(cost.index(min(cost)))
                    if train_pos == -1:
                        X_latency.append(plans[start + id + 13 * cost.index(min(cost))])
                    else:
                        X_latency.append(plans[start + id + 13 * train_pos])
                else:
                    y_cost.append(cost.index(min(cost)))
                    if train_pos == -1:
                        X_cost.append(plans[start + id + 13 * cost.index(min(cost))])
                    else:
                        X_cost.append(plans[start + id + 13 * train_pos])

    print("failed_count:", failed_count)
    # assert failed_count == 0
    if (train_pos == -1):
        print(X_latency[10], X_latency[13 + 10], X_latency[13 * 2 + 10])
    return  X_cost, y_cost, X_latency, y_latency


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('scan_model', type=str)
    parser.add_argument('join_model', type=str)
    parser.add_argument('--db', type=str, default='1x', choices=['1x', '10x'])
    args = parser.parse_args()

    CUDA = torch.cuda.is_available()
    print("CUDA:", CUDA)
    if args.db == '10x':
        scan_plan_path = "../../data/cost_col_hint_plan_hybench_10x.txt"
        join_plan_path = "../../data/scan_aware_cost_join_type_hint_plan_hybench_10x.txt"
    else:
        scan_plan_path = "../../data/cost_col_hint_plan_hybench.txt"
        join_plan_path = "../../data/cost_join_type_hint_plan_hybench.txt"
    scan_plan, join_plan = load_plans(scan_plan_path), load_plans(join_plan_path)

    X_cost, y_cost, X_latency, y_latency = cost_split(scan_plan, 35, 32, 160)
    scan_reg = train_and_save_model(args.scan_model, X_cost, y_cost)
    scan_reg = train_and_save_model(args.scan_model, X_latency, y_latency, reg=scan_reg)
    del scan_plan

    X_cost, y_cost, X_latency, y_latency = cost_split(join_plan, 7, -1, 160)
    join_reg = train_and_save_model(args.join_model, X_cost, y_cost, node_level=True)
    join_reg = train_and_save_model(args.join_model, X_latency, y_latency, reg=join_reg, node_level=True)
    del join_plan
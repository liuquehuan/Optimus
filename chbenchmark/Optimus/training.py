import torch # type: ignore
import sys
import argparse
sys.path.append("../../")
from utils.utils import cost_split, load_plans
from HGT.train import Optimus
import time


def train_and_save_model(fn, X, y, verbose=True, node_level=False, reg=None):
    if reg is None:
        reg = Optimus(verbose=verbose, node_level=node_level)

    start = time.time()
    reg.run_train_upd(X, y)
    end = time.time()
    print("training:", end - start)
    reg.save(fn)
    return reg


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('scan_model', type=str)
    parser.add_argument('join_model', type=str)
    parser.add_argument('--db', type=str, default='1x', choices=['1x', '10x'])
    args = parser.parse_args()

    CUDA = torch.cuda.is_available()
    print("CUDA:", CUDA)
    if args.db == '10x':
        scan_plan_path, join_plan_path = "../../data/cost_col_hint_plan_10x.txt", "../../data/cost_join_type_hint_plan_10x.txt"
    else:
        scan_plan_path, join_plan_path = "../../data/cost_col_hint_plan.txt", "../../data/cost_join_type_hint_plan.txt"
    scan_plan, join_plan = load_plans(scan_plan_path), load_plans(join_plan_path)

    X_cost, y_cost, X_latency, y_latency = cost_split(scan_plan, 35, 32, 80)
    scan_reg = train_and_save_model(args.scan_model, X_cost, y_cost)
    scan_reg = train_and_save_model(args.scan_model, X_latency, y_latency, reg=scan_reg)
    del scan_plan

    X_cost, y_cost, X_latency, y_latency = cost_split(join_plan, 7, -1, 80)
    join_reg = train_and_save_model(args.join_model, X_cost, y_cost, node_level=True)
    join_reg = train_and_save_model(args.join_model, X_latency, y_latency, reg=join_reg, node_level=True)
    del join_plan
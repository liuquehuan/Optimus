import os
import sys
import time
import datetime
import numpy as np
import psycopg2
from multiprocessing import Pool

CUR_DIR = os.path.dirname(__file__)
PROJ_ROOT = os.path.abspath(os.path.join(CUR_DIR, "..", ".."))
if PROJ_ROOT not in sys.path:
    sys.path.append(PROJ_ROOT)
GPREDICTOR_DIR = os.path.join(PROJ_ROOT, "gpredictor")
CODE_DIR = os.path.join(PROJ_ROOT, "code")
for p in [GPREDICTOR_DIR, CODE_DIR]:
    if p not in sys.path:
        sys.path.append(p)

from utils.htap_query import HTAPController
from utils.restore import restore
from utils.utils import run_ap_with_tp
from gpredictor.adapter import (
    train_and_save_model,
    load_model,
    predict_latency_for_plan,
)


params = []
PARAM_ROOT = os.path.join(CODE_DIR, "params")
for i in range(1, 23):
    path = os.path.join(PARAM_ROOT, f"{i}.txt")
    param = []
    if os.path.exists(path):
        with open(path, "r") as file:
            lines = file.readlines()
            for tup in lines:
                tup = eval(tup)
                param.append(tup[0])
    params.append(param)


def build_hint_set():
    operator_type = ["enable_indexscan", "enable_seqscan", "google_columnar_engine.enable_columnar_scan",
                     "enable_hashjoin", "enable_mergejoin", "enable_nestloop"]
    row_hint_set = []
    for i in range(0, 64):
        hint = "/*+\n"
        if i >> 3 == 0 or i & 7 == 0:
            continue

        for j in range(0, 6):
            k = i >> j & 1
            if k == 1:
                hint += "Set(" + operator_type[j] + " on)\n"
            else:
                hint += "Set(" + operator_type[j] + " off)\n"
        hint += "*/\n"
        row_hint_set.append(hint)
    return row_hint_set


def load_or_train_model(graph_data_root, model_path, limit_per_dir=500, start_strategy="zero"):
    if os.path.exists(model_path):
        print(f"loading gpredictor model from {model_path}")
        model, mp_optype = load_model(model_path)
    else:
        print(f"training gpredictor on {graph_data_root}, limit_per_dir={limit_per_dir}")
        model, mp_optype = train_and_save_model(
            graph_data_root, model_path, limit_per_dir=limit_per_dir, start_strategy=start_strategy
        )
        print(f"model saved to {model_path}")
    return model, mp_optype


if __name__ == "__main__":
    restore()
    conn = psycopg2.connect(
        host="localhost",
        database="htap_sf1",
        user="postgres",
        port=5555,
        password="postgres"
    )
    cur = conn.cursor()
    cur.execute("load 'pg_hint_plan'")

    htapcontroller = HTAPController()

    row_hint_set = build_hint_set()
    model_path = os.path.join(PROJ_ROOT, "gpredictor", "gp_model.pt")
    model, mp_optype = load_or_train_model(
        os.path.join(os.path.dirname(__file__), "..", "graph_data"),
        model_path,
        limit_per_dir=500,
        start_strategy="sequential_exec",
    )

    pg_latency = []
    sql_latency = np.zeros(22)
    k1, k2 = 50000, 20

    with Pool(100) as p1:
        for i in range(k1 // 100):
            print(i)
            tp1 = p1.map_async(htapcontroller.oltp_worker, range(100))

    start_time = time.time()
    for i in range(20):
        for j in range(22):
            qid = j + 1
            
            query_tpl_path = os.path.join(CODE_DIR, "queries_for_plan", f"{qid}.sql.template")

            with open(query_tpl_path, "r") as file:
                sql = file.read()
                param = params[qid - 1]
                param = None if len(param) == 0 else param[i * 5]

               
                pred_costs = []
                plans = []
                for hint in row_hint_set:
                    explain_sql = hint + "EXPLAIN (FORMAT JSON)\n" + sql
                    cur.execute(explain_sql, param)
                    plan_json = cur.fetchone()[0][0]
                    plans.append(plan_json)

                for plan_json in plans:
                    pred = predict_latency_for_plan(model, plan_json, mp_optype)
                    pred_costs.append(pred)

                opt_idx = int(np.argmin(pred_costs))
                opt_hint = row_hint_set[opt_idx]

                exp_sql = "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                hint_sql = opt_hint + exp_sql
                print("execute stream, sql_id:", i, qid, "chosen hint:", opt_idx)

                plan, tp_results = run_ap_with_tp(hint_sql, param, k2, htapcontroller)
                # 某些情况下 OLAP worker 会因为连接/超时问题返回 None，这里做健壮性处理
                if plan is None:
                    print(f"[warn] OLAP plan is None for stream {i}, qid {qid}, chosen hint {opt_idx}, skip this run")
                    continue

                if 'Execution Time' not in plan:
                    print(f"[warn] plan has no 'Execution Time' field for stream {i}, qid {qid}, chosen hint {opt_idx}, skip this run")
                    continue

                pg_latency.append(plan['Execution Time'])
                sql_latency[j] += plan['Execution Time']

    print(time.time() - start_time)
    cur.execute("show google_columnar_engine.enable_vectorized_join")
    print(cur.fetchone())
    cur.execute("show google_columnar_engine.refresh_threshold_percentage")
    print(cur.fetchone())
    print("k1:", k1, "k2:", k2)
    print(sql_latency)
    pg_latency = np.sort(np.array(pg_latency))
    print("sum latency:", np.sum(pg_latency))
    print("median latency:", np.median(pg_latency))
    idx95, idx99, idx995 = int(max(0, len(pg_latency) * 0.95 - 1)), int(max(0, len(pg_latency) * 0.99 - 1)), int(max(0, len(pg_latency) * 0.995 - 1))
    print("95 latency:", pg_latency[idx95])
    print("99 latency:", pg_latency[idx99])
    print("995 latency:", pg_latency[idx995])

    cur.close()
    conn.close()


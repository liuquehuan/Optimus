import numpy as np
import psycopg2
import os
import sys
import argparse
sys.path.append("../")
from utils.htap_query import HTAPController
from multiprocessing import Pool
from utils.restore import restore, restore_10x
import datetime
import time
from utils.utils import run_ap_with_tp, set_refresh_threshold

params = []
for i in range(1, 23):
    path = "../../data/params/" + str(i) + ".txt"
    param = []
    if os.path.exists(path):
        with open(path, "r") as file:
            lines = file.readlines()
            for tup in lines:
                tup = eval(tup)
                param.append(tup[0])
    params.append(param)


def get_scan_hint(scan_mode):
    if scan_mode == 'row':
        hint = """/*+
Set(google_columnar_engine.enable_columnar_scan off)
Set(google_columnar_engine.enable_vectorized_join off)
Set(enable_seqscan on)
Set(enable_indexscan on)
Set(enable_indexonlyscan on)
*/
"""
    elif scan_mode == 'column':
        hint = """/*+
Set(google_columnar_engine.enable_columnar_scan on)
Set(google_columnar_engine.enable_vectorized_join on)
Set(enable_seqscan off)
Set(enable_indexscan off)
Set(enable_indexonlyscan off)
*/
"""
    else:  # mixed
        hint = ""
    return hint


def get_k2_by_time(elapsed_minutes):
    period = int(elapsed_minutes // 5)
    
    if period == 0:
        return 30
    elif period == 1:
        return 50
    elif period == 2:
        return 30
    elif period == 3:
        return 10
    else:
        return 30


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run the AlloyDB benchmark')
    parser.add_argument('--db', type=str, default='1x', choices=['1x', '10x'],
                        help='Database scale: 1x (default) or 10x')
    parser.add_argument('--scan-mode', type=str, default='mixed', 
                        choices=['row', 'column', 'mixed'],
                        help='Scan mode: row, column, or mixed (default)')
    args = parser.parse_args()
    
    if args.db == '10x':
        restore_10x()
        database_name = "htap_sf10"
        timeout = 100000
    else:
        restore()
        database_name = "htap_sf1"
        timeout = 20000
    
    set_refresh_threshold(50)

    conn = psycopg2.connect(
        host="localhost",
        database=database_name,
        user="postgres",
        port=5555,
        password="postgres"
    )
    cur = conn.cursor()
    cur.execute("load \'pg_hint_plan\'")

    pg_latency = []
    sql_latency = np.zeros(22)
    k1 = 50000 * (1 if args.db == '1x' else 10)
    k2 = 30

    htapcontroller = HTAPController()

    with Pool(50) as p1:
        for i in range(k1 // 50):
            # print(i)
            tp1 = p1.map_async(htapcontroller.oltp_worker, range(50))
    
    failed = []
    time.sleep(10)
    tp_count = 0
    start_time = time.time()
    last_report_time = time.time()
    minute_tp_count = 0
    minute_query_count = 0
    minute_statistics = []
    current_k2 = 30
    last_k2_change_time = start_time
    
    for i in range(20):
        for j in range(22):
            current_time = time.time()
            elapsed_minutes = (current_time - start_time) / 60
            new_k2 = get_k2_by_time(elapsed_minutes)
            if new_k2 != current_k2:
                print(f"[k2 update] elapsed time: {elapsed_minutes:.2f} min, k2 changed from {current_k2} to {new_k2}")
                current_k2 = new_k2
                last_k2_change_time = current_time
            
            id = j + 1
            path = "../../data/queries_for_plan/" + str(id) + ".sql.template"

            with open(path, "r") as file:
                sql = file.read()
                scan_hint = get_scan_hint(args.scan_mode)
                exp_sql = scan_hint + "EXPLAIN (ANALYZE, FORMAT JSON)\n" + sql
                param = params[id - 1]
                param = None if len(param) == 0 else param[i * 5]
                print("execute stream, sql_id:", i, id)

                plan, tp_results = run_ap_with_tp(exp_sql, param, current_k2, htapcontroller, timeout)
                tp_count += len(tp_results)
                
                minute_tp_count += len(tp_results)

                if plan is None or 'Execution Time' not in plan:
                    print(f"plan is None or missing 'Execution Time', skip. stream={i}, sql_id={id}")
                    failed.append(id)
                    continue

                pg_latency.append(plan['Execution Time'])
                sql_latency[j] += plan['Execution Time']
                
                minute_query_count += 1
                
                current_time = time.time()
                if current_time - last_report_time >= 60:
                    elapsed_minutes = (current_time - last_report_time) / 60
                    minute_statistics.append({
                        'time': current_time,
                        'elapsed_minutes': elapsed_minutes,
                        'tp_count': minute_tp_count,
                        'query_count': minute_query_count
                    })
                    minute_tp_count = 0
                    minute_query_count = 0
                    last_report_time = current_time
    
    end_time = time.time()
    if minute_tp_count > 0 or minute_query_count > 0:
        elapsed_minutes = (end_time - last_report_time) / 60
        minute_statistics.append({
            'time': end_time,
            'elapsed_minutes': elapsed_minutes,
            'tp_count': minute_tp_count,
            'query_count': minute_query_count
        })
    
    if len(minute_statistics) > 0:
        tp_list = [s['tp_count'] for s in minute_statistics]
        query_list = [s['query_count'] for s in minute_statistics]
        print("\n[Per-minute statistics]")
        print(f"TP result counts: {tp_list}")
        print(f"Processed query counts: {query_list}")
        
        output_filename = f"minute_statistics_{args.db}_{args.scan_mode}_{time.strftime('%Y%m%d_%H%M%S', time.localtime(start_time))}.txt"
        try:
            with open(output_filename, 'w', encoding='utf-8') as f:
                f.write(f"TP result counts: {tp_list}\n")
                f.write(f"Processed query counts: {query_list}\n")
            print(f"Statistics saved to file: {output_filename}")
        except Exception as e:
            print(f"Warning: failed to save statistics to file: {e}")

    print("failed:", failed)
    print(time.time() - start_time)
    cur.execute("show google_columnar_engine.enable_vectorized_join")
    print(cur.fetchone())
    cur.execute("show google_columnar_engine.refresh_threshold_percentage")
    print(cur.fetchone())
    print("k1:", k1, "k2:", k2)
    cur.execute("show max_parallel_workers_per_gather")
    print(cur.fetchone())
    
    print("tp: ", tp_count)
    print(sql_latency)
    pg_latency = np.sort(np.array(pg_latency))
    print("sum latency:", np.sum(pg_latency))
    print("median latency:", np.median(pg_latency))
    idx95, idx99, idx995 = int(max(0, len(pg_latency) * 0.95 - 1)), int(max(0, len(pg_latency) * 0.99 - 1)), int(max(0, len(pg_latency) * 0.995 - 1))
    print("95 latency:", pg_latency[idx95])
    print("99 latency:", pg_latency[idx99])
    print("995 latency:", pg_latency[idx995])

    # set_refresh_threshold(50)
    cur.close()
    conn.close()

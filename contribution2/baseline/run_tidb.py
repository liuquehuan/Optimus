import numpy as np
import pymysql
import os
import sys
import argparse
sys.path.append("../")
from utils.htap_query_tidb import HTAPController
from multiprocessing import Pool
import time
import datetime

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


def run_ap_with_tp_tidb(sql, param, TP_WORKERS, controller, timeout=100000, mode='hybrid'):
    """TiDB版本的run_ap_with_tp，支持mode参数"""
    from multiprocessing import Pool, Manager
    from utils.utils import tp_worker_loop
    
    pool = Pool(TP_WORKERS + 1)
    manager = Manager()
    
    stop_event = manager.Event()
    async_results = []

    for i in range(TP_WORKERS):
        async_results.append(
            pool.apply_async(tp_worker_loop, args=(i, stop_event, controller, False, False))
        )

    # 传递mode参数给olap_worker
    ap_async = pool.starmap_async(controller.olap_worker, [(sql, param, timeout, mode)])

    plan = ap_async.get()[0]

    stop_event.set()
    
    # 等待所有 TP worker 完成并收集返回值，扁平化为一维列表
    tp_results = []
    for tp in async_results:
        worker_results = tp.get()
        tp_results.extend(worker_results)  # 用 extend 而不是 append，扁平化

    # 打印调试信息
    print(f"len(tp_results) = {len(tp_results)}")

    pool.close()
    pool.join()

    return plan, tp_results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='运行 TiDB 基准测试')
    parser.add_argument('--db', type=str, default='1x', choices=['1x', '10x'],
                        help='选择数据库规模: 1x (默认) 或 10x')
    parser.add_argument('--scan-mode', type=str, default='mixed', 
                        choices=['row', 'column', 'mixed'],
                        help='扫描模式: row (只使用行扫描), column (只使用列扫描), mixed (混合扫描，默认)')
    args = parser.parse_args()
    
    # 根据参数选择数据库
    if args.db == '10x':
        database_name = "htap_sf10"
        timeout = 100000
    else:
        database_name = "htap_sf1"
        timeout = 20000
    
    # 将scan_mode转换为TiDB的mode格式
    mode_map = {
        'row': 'row',
        'column': 'col',
        'mixed': 'hybrid'
    }
    mode = mode_map[args.scan_mode]

    # TiDB连接配置
    tidb_config = {
        "host": "localhost",
        "database": database_name,
        "user": "root",
        "port": 4000,
        "password": "",
        "charset": "utf8mb4",
        "autocommit": True
    }
    
    conn = pymysql.connect(**tidb_config)
    cur = conn.cursor()

    pg_latency = []
    sql_latency = np.zeros(22)
    k1, k2 = 50000 * (1 if args.db == '1x' else 10), 50

    htapcontroller = HTAPController(database_name=database_name)

    with Pool(50) as p1:
        for i in range(k1 // 50):
            # print(i)
            tp1 = p1.map_async(htapcontroller.oltp_worker, range(50))
    
    failed = []
    time.sleep(10)
    tp_count = 0
    start_time = time.time()
    for i in range(20):
        for j in range(22):
            id = j + 1
            path = "../../data/queries_for_plan_tidb/" + str(id) + ".sql.template"

            with open(path, "r") as file:
                sql = file.read()
                
            param = params[id - 1]
            param = None if len(param) == 0 else param[i * 5]
            print("execute stream, sql_id:", i, id)

            # 使用TiDB版本的run_ap_with_tp
            plan, tp_results = run_ap_with_tp_tidb(sql, param, k2, htapcontroller, timeout, mode)
            tp_count += len(tp_results) if tp_results else 0

            # 可能因为连接错误等原因，olap_worker 返回 None，此时跳过该次结果
            if plan is None or 'Execution Time' not in plan:
                print(f"plan is None or missing 'Execution Time', skip. stream={i}, sql_id={id}")
                failed.append(id)
                continue

            pg_latency.append(plan['Execution Time'])
            sql_latency[j] += plan['Execution Time']

    print("failed:", failed)
    print(time.time() - start_time)
    
    # 显示当前TiDB配置
    cur.execute("SELECT @@tidb_isolation_read_engines, @@tidb_allow_mpp, @@tidb_enforce_mpp")
    print("TiDB configuration:", cur.fetchone())
    
    print("k1:", k1, "k2:", k2)
    cur.execute("SELECT @@tidb_max_pipeline_workers")
    print("max_parallel_workers_per_gather:", cur.fetchone())
    
    print("tp: ", tp_count)
    print(sql_latency)
    pg_latency = np.sort(np.array(pg_latency))
    print("sum latency:", np.sum(pg_latency))
    print("median latency:", np.median(pg_latency))
    idx95, idx99, idx995 = int(max(0, len(pg_latency) * 0.95 - 1)), int(max(0, len(pg_latency) * 0.99 - 1)), int(max(0, len(pg_latency) * 0.995 - 1))
    print("95 latency:", pg_latency[idx95])
    print("99 latency:", pg_latency[idx99])
    print("99.5 latency:", pg_latency[idx995])

    cur.close()
    conn.close()


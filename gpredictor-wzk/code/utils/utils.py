def load_plans(filepath: str):
    with open(filepath, "r") as f:
        plan_list = f.readlines()
        plan_list = [eval(x) for x in plan_list]
        return plan_list


def cost_split(plans, n, train_pos, num_stream):
    X_cost, y_cost, X_latency, y_latency = [], [], [], []
    failed_count = 0

    for i in range(num_stream):
        start = i * 22 * n
        for id in range(22):
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
                cur_id += 22
            
            if min(cost) == 6000000000000:
                failed_count += 1
            else:
                if 'Execution Time' in plans[start]:
                    y_latency.append(cost.index(min(cost)))
                    if train_pos == -1:
                        X_latency.append(plans[start + id + 22 * cost.index(min(cost))])
                    else:
                        X_latency.append(plans[start + id + 22 * train_pos])
                else:
                    y_cost.append(cost.index(min(cost)))
                    if train_pos == -1:
                        X_cost.append(plans[start + id + 22 * cost.index(min(cost))])
                    else:
                        X_cost.append(plans[start + id + 22 * train_pos])

    print("failed_count:", failed_count)
    return  X_cost, y_cost, X_latency, y_latency


def set_refresh_threshold(threshold):
    import psycopg2 # type: ignore
    conn = psycopg2.connect(
                    host="localhost",
                    database="htap_sf1",
                    user="postgres",
                    port=5555,
                    password="postgres"
                )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"ALTER SYSTEM SET google_columnar_engine.refresh_threshold_percentage={threshold};")
    cur.execute("SELECT pg_reload_conf();")
    cur.close()
    conn.close()

from multiprocessing import Pool, Manager

def tp_worker_loop(worker_id, stop_event, controller, tp_explain=False, tp_analyze=False):
    import traceback
    results = []
    while True:
        if stop_event.is_set():
            break
        try:
            result = controller.oltp_worker(worker_id, tp_explain, tp_analyze)
            results.append(result)
        except Exception as e:
            print(f"TP worker {worker_id} error: {e}")
            # 连接错误时跳过，继续尝试
            import time
            time.sleep(0.1)
    return results


def run_ap_with_tp(sql, param, TP_WORKERS, controller, timeout=100000, tp_explain=False, tp_analyze=False, pool=None, manager=None):
    
    # 如果没有传入 pool 和 manager，则创建新的
    print(timeout)
    own_pool = pool is None
    if own_pool:
        pool = Pool(TP_WORKERS + 1)
    if manager is None:
        manager = Manager()
    
    stop_event = manager.Event()
    async_results = []

    for i in range(TP_WORKERS):
        async_results.append(
            pool.apply_async(tp_worker_loop, args=(i, stop_event, controller, tp_explain, tp_analyze))
        )

    ap_async = pool.starmap_async(controller.olap_worker, [(sql, param, timeout)])

    plan = ap_async.get()[0]

    stop_event.set()
    
    # 等待所有 TP worker 完成并收集返回值，扁平化为一维列表
    tp_results = []
    for tp in async_results:
        worker_results = tp.get()
        tp_results.extend(worker_results)  # 用 extend 而不是 append，扁平化

    # 打印调试信息
    print(f"len(tp_results) = {len(tp_results)}")

    # 只有自己创建的 pool 才需要关闭
    if own_pool:
        pool.close()
        pool.join()

    return plan, tp_results

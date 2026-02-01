import os
import psycopg2 # type: ignore
import time
import pexpect
import time
from .utils import set_refresh_threshold

def restore():
    ## restore
    time.sleep(10)
    password = "postgres"
    
    # 设置环境变量避免密码提示
    os.environ['PGPASSWORD'] = password
    os.environ['LC_ALL'] = 'C'
    os.environ['LANG'] = 'C'

    conn = psycopg2.connect(host="localhost", port=5555, user="postgres", password="postgres", dbname="htap_sf1")
    conn.autocommit = True
    cur = conn.cursor()
    
    cur.execute("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database();")
    thread_num = cur.fetchone()[0]
    print("thread_num:", thread_num)
    with open("thread_num.txt", "a+") as file:
        file.write(str(thread_num) + '\n')
    if thread_num > 1:
        cur.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid();")
    cur.execute("show google_columnar_engine.refresh_threshold_percentage;")
    threshold = cur.fetchone()[0]
    print("threshold:", threshold)
    cur.close()
    conn.close()
    time.sleep(10)

    set_refresh_threshold(50)
    
    restore_res = pexpect.spawn("pg_restore -c -h localhost -p 5555 -U postgres -d htap_sf1 /home/ruc/Optimus/migration/htap_sf1.dump")
    
    # 等待命令完成，不需要密码输入
    with open("restore.log", "wb") as f:
        restore_res.logfile = f
        restore_res.expect(pexpect.EOF, timeout=120)
    time.sleep(10)  # 给一点额外时间确保完成
    print("restore:", restore_res)

    conn = psycopg2.connect(host="localhost", port=5555, user="postgres", password="postgres", dbname="htap_sf1")
    conn.autocommit = True
    cur = conn.cursor()

    # cur.execute("alter system set google_columnar_engine.relations = 'htap_sf10.public.district, htap_sf10.public.customer, htap_sf10.public.nation, htap_sf10.public.orders, htap_sf10.public.history, htap_sf10.public.region, htap_sf10.public.supplier, htap_sf10.public.item, htap_sf10.public.new_orders, htap_sf10.public.order_line, htap_sf10.public.stock, htap_sf10.public.warehouse, htap_sf1.public.district, htap_sf1.public.customer, htap_sf1.public.nation, htap_sf1.public.orders, htap_sf1.public.history, htap_sf1.public.region, htap_sf1.public.supplier, htap_sf1.public.item, htap_sf1.public.new_orders, htap_sf1.public.order_line, htap_sf1.public.stock, htap_sf1.public.warehouse'")
    # cur.execute("SELECT pg_reload_conf();")
    # cur.execute("alter system set google_columnar_engine.relations = 'htap_sf10.public.district, htap_sf10.public.customer, htap_sf10.public.nation, htap_sf10.public.orders, htap_sf10.public.history, htap_sf10.public.region, htap_sf10.public.supplier, htap_sf10.public.item, htap_sf10.public.new_orders, htap_sf10.public.order_line, htap_sf10.public.stock, htap_sf10.public.warehouse, htap_sf1.public.district, htap_sf1.public.customer, htap_sf1.public.nation, htap_sf1.public.orders, htap_sf1.public.history, htap_sf1.public.region, htap_sf1.public.supplier, htap_sf1.public.item, htap_sf1.public.new_orders, htap_sf1.public.order_line, htap_sf1.public.stock, htap_sf1.public.warehouse, htap_test.public.district, htap_test.public.customer, htap_test.public.nation, htap_test.public.orders, htap_test.public.history, htap_test.public.region, htap_test.public.supplier, htap_test.public.item, htap_test.public.new_orders, htap_test.public.order_line, htap_test.public.stock, htap_test.public.warehouse';")
    # cur.execute("SELECT pg_reload_conf();")

    cur.execute("SELECT google_columnar_engine_add('district');")
    cur.execute("SELECT google_columnar_engine_add('customer');")
    cur.execute("SELECT google_columnar_engine_add('nation');")
    cur.execute("SELECT google_columnar_engine_add('orders');")
    cur.execute("SELECT google_columnar_engine_add('history');")
    cur.execute("SELECT google_columnar_engine_add('region');")
    cur.execute("SELECT google_columnar_engine_add('supplier');")
    cur.execute("SELECT google_columnar_engine_add('item');")
    cur.execute("SELECT google_columnar_engine_add('new_orders');")
    cur.execute("SELECT google_columnar_engine_add('order_line');")
    cur.execute("SELECT google_columnar_engine_add('stock');")
    cur.execute("SELECT google_columnar_engine_add('warehouse');")

    start = time.time()
    tries = 0
    while True:
        time.sleep(10)
        cur.execute("SELECT count(*) FROM g_columnar_columns;")
        result = cur.fetchall()[0][0]
        if result == 108:
            break
        tries += 1
        print("tries:", tries, "result:", result)
        # if tries > 10:
        #     print("columnar engine failed to load")
        #     exit(1)
    end = time.time()
    print("time:", end - start)

    cur.close()
    conn.close()
    set_refresh_threshold(threshold)


def restore_10x():
    ## restore
    time.sleep(10)
    password = "postgres"
    
    # 设置环境变量避免密码提示
    os.environ['PGPASSWORD'] = password
    os.environ['LC_ALL'] = 'C'
    os.environ['LANG'] = 'C'

    conn = psycopg2.connect(host="localhost", port=5555, user="postgres", password="postgres", dbname="htap_sf10")
    conn.autocommit = True
    cur = conn.cursor()
    
    cur.execute("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database();")
    thread_num = cur.fetchone()[0]
    print("thread_num:", thread_num)
    with open("thread_num.txt", "a+") as file:
        file.write(str(thread_num) + '\n')
    if thread_num > 1:
        cur.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid();")
    cur.execute("show google_columnar_engine.refresh_threshold_percentage;")
    threshold = cur.fetchone()[0]
    print("threshold:", threshold)
    cur.close()
    conn.close()
    time.sleep(10)

    # set_refresh_threshold(50)
    
    restore_res = pexpect.spawn("pg_restore -c -h localhost -p 5555 -U postgres -d htap_sf10 /home/ruc/Optimus/migration/htap_sf10.dump")
    
    # 等待命令完成，不需要密码输入
    with open("restore.log", "wb") as f:
        restore_res.logfile = f
        restore_res.expect(pexpect.EOF, timeout=500)

    time.sleep(30)  # 给一点额外时间确保完成
    print("restore:", restore_res)

    conn = psycopg2.connect(host="localhost", port=5555, user="postgres", password="postgres", dbname="htap_sf10", options="-c statement_timeout=0")
    conn.autocommit = True
    cur = conn.cursor()

    # cur.execute("alter system set google_columnar_engine.relations = 'htap_sf10.public.district, htap_sf10.public.customer, htap_sf10.public.nation, htap_sf10.public.orders, htap_sf10.public.history, htap_sf10.public.region, htap_sf10.public.supplier, htap_sf10.public.item, htap_sf10.public.new_orders, htap_sf10.public.order_line, htap_sf10.public.stock, htap_sf10.public.warehouse, htap_sf1.public.district, htap_sf1.public.customer, htap_sf1.public.nation, htap_sf1.public.orders, htap_sf1.public.history, htap_sf1.public.region, htap_sf1.public.supplier, htap_sf1.public.item, htap_sf1.public.new_orders, htap_sf1.public.order_line, htap_sf1.public.stock, htap_sf1.public.warehouse'")
    # cur.execute("SELECT pg_reload_conf();")
    # cur.execute("alter system set google_columnar_engine.relations = 'htap_sf10.public.district, htap_sf10.public.customer, htap_sf10.public.nation, htap_sf10.public.orders, htap_sf10.public.history, htap_sf10.public.region, htap_sf10.public.supplier, htap_sf10.public.item, htap_sf10.public.new_orders, htap_sf10.public.order_line, htap_sf10.public.stock, htap_sf10.public.warehouse, htap_sf1.public.district, htap_sf1.public.customer, htap_sf1.public.nation, htap_sf1.public.orders, htap_sf1.public.history, htap_sf1.public.region, htap_sf1.public.supplier, htap_sf1.public.item, htap_sf1.public.new_orders, htap_sf1.public.order_line, htap_sf1.public.stock, htap_sf1.public.warehouse, htap_test.public.district, htap_test.public.customer, htap_test.public.nation, htap_test.public.orders, htap_test.public.history, htap_test.public.region, htap_test.public.supplier, htap_test.public.item, htap_test.public.new_orders, htap_test.public.order_line, htap_test.public.stock, htap_test.public.warehouse';")
    # cur.execute("SELECT pg_reload_conf();")

    cur.execute("SELECT google_columnar_engine_add('district');")
    cur.execute("SELECT google_columnar_engine_add('customer');")
    cur.execute("SELECT google_columnar_engine_add('nation');")
    cur.execute("SELECT google_columnar_engine_add('orders');")
    cur.execute("SELECT google_columnar_engine_add('history');")
    cur.execute("SELECT google_columnar_engine_add('region');")
    cur.execute("SELECT google_columnar_engine_add('supplier');")
    cur.execute("SELECT google_columnar_engine_add('item');")
    cur.execute("SELECT google_columnar_engine_add('new_orders');")
    cur.execute("SELECT google_columnar_engine_add('order_line');")
    cur.execute("SELECT google_columnar_engine_add('stock');")
    cur.execute("SELECT google_columnar_engine_add('warehouse');")

    start = time.time()
    tries = 0
    while True:
        time.sleep(10)
        cur.execute("SELECT count(*) FROM g_columnar_columns;")
        result = cur.fetchall()[0][0]
        if result == 108:
            break
        tries += 1
        print("tries:", tries, "result:", result)
        # if tries > 10:
        #     print("columnar engine failed to load")
        #     exit(1)
    end = time.time()
    print("time:", end - start)

    cur.close()
    conn.close()
    # set_refresh_threshold(threshold)


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: restore.py db")
        exit(-1)
    
    db = sys.argv[1]
    if db == "sf1":
        restore()
    elif db == "sf10":
        restore_10x()
    else:
        print("Invalid database")
        exit(-1)

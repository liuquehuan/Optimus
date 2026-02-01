import os
import psycopg2
import time
import pexpect
import time

def restore():
    ## restore
    password = "postgres"
    restore_res = pexpect.spawn("pg_restore -c -h localhost -p 5555 -U postgres -d htap_test /home/ruc/htap_test.dump")
    restore_res.expect("Password: ")
    restore_res.sendline(password)
    time.sleep(60)
    # print("restore:", restore_res)

    conn = psycopg2.connect(host="localhost", port=5555, user="postgres", password="postgres", dbname="htap_test")
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

if __name__ == "__main__":
    restore()

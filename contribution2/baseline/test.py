import numpy as np
import psycopg2
import os
import sys
sys.path.append("../")
from utils.htap_query import HTAPController
from multiprocessing import Pool
from utils.restore import restore
import datetime
import time

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


if __name__ == "__main__":
    conn = psycopg2.connect(
        host="localhost",
        database="htap_sf1",
        user="postgres",
        port=5555,
        password="postgres"
    )
    cur = conn.cursor()
    cur.execute("load \'pg_hint_plan\'")
    cur.execute("select count(*) from orders")
    print(cur.fetchone())

    for _ in range(1):
        p1 = Pool(100)
        htapcontroller = HTAPController()
        tp1 = p1.map_async(htapcontroller.oltp_worker, range(100))
        p1.close()
        p1.join()

cur.close()
conn.close()

conn = psycopg2.connect(
        host="localhost",
        database="htap_sf1",
        user="postgres",
        port=5555,
        password="postgres"
    )
cur = conn.cursor()
cur.execute("load \'pg_hint_plan\'")
cur.execute("select count(*) from orders")
print(cur.fetchone())
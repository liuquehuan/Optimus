import psycopg2 # type: ignore
conn = psycopg2.connect(
        host="localhost",
        database="htap_sf1",
        user="postgres",
        port=5555,
        password="postgres"
    )
cur = conn.cursor()
cur.execute("load \'pg_hint_plan\'")
cur.execute("/*+ ColumnarScan(orders) */ EXPLAIN (FORMAT JSON) SELECT * FROM orders")
plan = cur.fetchone()[0][0]
print(plan)

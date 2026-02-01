from htap_query import HTAPController

htapcontroller = HTAPController()
for i in range(100):
    htapcontroller.oltp_worker(i)

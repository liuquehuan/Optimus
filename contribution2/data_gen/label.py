labels = []
for id in range(0, 20 * 22):
    opt_latency, idx = None, None
    for hint_id in range(0, 35):
        with open("../data/gnn_data/" + str(hint_id) + "/" + str(id) + ".txt", "r") as file:
            plans = file.readlines()
            plans = [eval(x) for x in plans]
            if plans[-1] is not None and (opt_latency is None or plans[-1]['Execution Time'] < opt_latency):
                opt_latency = plans[-1]['Execution Time']
                idx = hint_id
    labels.append(idx)

with open("../data/htap_scan_labels.txt", "a+") as file:
    file.write(str(labels))

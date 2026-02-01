hint_plans = []
for id in range(0, 20 * 22):
    opt_plan = None
    for hint_id in range(0, 35):
        with open("../../data/gnn_data/" + str(hint_id) + "/" + str(id) + ".txt", "r") as file:
            plans = file.readlines()
            plans = [eval(x) for x in plans]
            if plans[-1] is not None and (opt_plan is None or plans[-1]['Execution Time'] < opt_plan[-1]['Execution Time']):
                opt_plan = plans
    hint_plans.append(opt_plan)

import os
os.makedirs("../../data/gnn_data/best_plans", exist_ok=True)
id = 0
for plans in hint_plans:
    with open("../../data/gnn_data/best_plans/" + str(id) + ".txt", "a+") as file:
        for plan in plans:
            file.write(str(plan) + '\n')
        id += 1

DATAPATH = "../../data/exp_data/"
num_sample = 60 * 22
type = 'cost'

hint_plans = []
for id in range(0, num_sample):
    opt_plan = []
    for hint_id in range(0, 35):
        with open(DATAPATH + str(hint_id) + "/" + str(id) + ".txt", "r") as file:
            plans = file.readlines()
            plans = [eval(x) for x in plans]
            if plans[-1] is not None:
                opt_plan.append(plans)
    
    opt_plan = sorted(opt_plan, key=lambda x: x[-1]['Plan']['Total Cost'] if type == 'cost' else x[-1]['Execution Time'])
    for i in range(0, min(5 if type == 'cost' else 10, len(opt_plan))):
        hint_plans.append(opt_plan[i])

import os
os.makedirs(DATAPATH + "best_5_exp_plans", exist_ok=True)
id = 0
for plans in hint_plans:
    with open(DATAPATH + "best_5_exp_plans/" + str(id) + ".txt", "a+") as file:
        for plan in plans:
            file.write(str(plan) + '\n')
        id += 1

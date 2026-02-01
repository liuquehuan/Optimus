time = 0

for id in range(0, 20 * 22):
    with open("../../data/gnn_data/best_plans/" + str(id) + ".txt", "r") as file:
        plans = file.readlines()
        plans = [eval(x) for x in plans]
        time += plans[-1]['Execution Time']

print(time)

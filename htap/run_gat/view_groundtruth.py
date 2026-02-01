import sys
sys.path.append(r".")

import numpy as np # type: ignore
from graphembedding import load_data_from_matrix

from nodeutils import extract_plan

label_count = [0, 0, 0, 0]
for sample_id in range(0, 440):
    with open(f"../../data/gnn_data/best_plans/{sample_id}.txt", "r") as f:
        sample = eval(f.readlines()[10])
        node_matrix, edge_matrix, conflict_operators, oid, _ = extract_plan(sample, {}, 0, True)
        if len(edge_matrix) == 0:
            edge_matrix = [[1, 1, 0]]
        adj, features, labels, _, _, _, _ = load_data_from_matrix(np.array(node_matrix, dtype=np.float32), np.array(edge_matrix, dtype=np.float32))
        for i in range(len(labels)):
            assert(labels[i] < 5)
            if labels[i] < 4:
                label_count[labels[i]] += 1

print(label_count)
label_count = np.array(label_count)
print(label_count / sum(label_count))

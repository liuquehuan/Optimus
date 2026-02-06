# **Optimus: Steering Query Optimizer in HTAP Databases**

We propose an HTAP-aware learning-based framework for hint selection in the operator level, named Optimus. First, we represent the HTAP workload as a heterogeneous graph, and leverage *Heterogeneous Graph Transformer (HGT)* to perform the graph embedding for capturing the HTAP interference. Second, we propose a two-phase hinting framework, which can select the proper scan and join hint sets accordingly, enabling practical yet efficient query optimization. Third, we propose an online plan adaptation approach to calibrate the ranking score and adjust the query plan dynamically.

# Preparation

1.Get database ready according to https://github.com/swarm64/s64da-benchmark-toolkit (htap) or https://github.com/Rucchao/HyBench-2024. We have included workloads into the dir `chbenchmark/workloads/` and `hybench/sql`.

2.Get conda environment ready:

```bash
conda env create -f environment.yml
conda activate optimus
```

# Usage

1.Generate data for model training. Following commands will enumerate candidate hints under training workload for training HGT.

```bash
# generate plans for CHBenchmark queries
python chbenchmark/generate_data/generate_plan_scan.py --db=10x
python chbenchmark/generate_data/generate_plan_join.py --db=10x

# generate plans for Hybench queries
python hybench/generate_data/generate_plan_scan.py --db=10x
python hybench/generate_data/generate_plan_join.py --db=10x
```

2.Train the model and run test workload. Scan model and join model will be saved on /path_to_scan_model /path_to_join_model, respectively.

```bash
# CHBenchmark
python chbenchmark/Optimus/run_Optimus.py /path_to_scan_model /path_to_join_model --db=10x

# Hybench
python hybench/Optimus/run_Optimus.py /path_to_scan_model /path_to_join_model --db=10x
```

[alt text](motivation.png)


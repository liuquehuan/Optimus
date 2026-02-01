# %%
import numpy as np
import os
import torch
import torch.nn as nn
import time
import pandas as pd
from scipy.stats import pearsonr
import random

# %%
from model.util import Normalizer
from model.database_util import get_hist_file, get_job_table_sample, collator
from model.model import QueryFormer
from model.database_util import Encoding
from model.dataset import PlanTreeDataset
from model.trainer import eval_workload, train

all_results = []

# ===================== 配置 =====================

data_dir = "data/10g"
all_csvs = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.csv') and f[:2].isdigit()]
all_csvs.sort()

# 1. 合并全部csv
all_df = pd.concat([pd.read_csv(f) for f in all_csvs], ignore_index=True)

# 2. 随机划分训练集和验证集
all_idx = list(range(len(all_df)))
random.shuffle(all_idx)
train_size = int(len(all_df) * 0.3)
train_idx = all_idx[:train_size]
val_idx = all_idx[train_size:]
train_df = all_df.iloc[train_idx].reset_index(drop=True)
val_df = all_df.iloc[val_idx].reset_index(drop=True)

print(f"训练集数量: {len(train_df)}，验证集数量: {len(val_df)}")

# ===================== 配置训练参数 =====================
class Args:
    bs = 128
    lr = 0.001
    epochs = 100
    clip_size = 50
    embed_size = 64
    pred_hid = 128
    ffn_dim = 128
    head_size = 12
    n_layers = 8
    dropout = 0.1
    sch_decay = 0.6
    device = 'cpu'
    newpath = './results/full/cost/'
    to_predict = 'cost'
args = Args()

if not os.path.exists(args.newpath):
    os.makedirs(args.newpath)

data_path = './data/imdb/'
hist_file = get_hist_file(data_path + 'histogram_string.csv')
cost_norm = Normalizer(-3.61192, 12.290855)
card_norm = Normalizer(1,100)

encoding_ckpt = torch.load('checkpoints/encoding.pt')
encoding_dict = encoding_ckpt['encoding']
encoding = Encoding(
    encoding_dict['column_min_max_vals'],
    encoding_dict['col2idx'],
    encoding_dict['op2idx']
)
encoding.type2idx = encoding_dict['type2idx']
encoding.idx2type = encoding_dict['idx2type']
encoding.join2idx = encoding_dict['join2idx']
encoding.idx2join = encoding_dict['idx2join']
checkpoint = torch.load('checkpoints/cost_model.pt', map_location='cpu')

from model.util import seed_everything
seed_everything()

model = QueryFormer(emb_size = args.embed_size ,ffn_dim = args.ffn_dim, head_size = args.head_size, \
                 dropout = args.dropout, n_layers = args.n_layers, \
                 use_sample = False, use_hist = False, \
                 pred_hid = args.pred_hid, \
                 joins=len(encoding.join2idx)
                )
_ = model.to(args.device)
to_predict = 'cost'
imdb_path = './data/imdb/'
table_sample = get_job_table_sample(imdb_path+'train')

# ========== 构建数据集 ==========
train_ds = PlanTreeDataset(train_df, None, encoding, hist_file, card_norm, cost_norm, to_predict, table_sample)
val_ds = PlanTreeDataset(val_df, None, encoding, hist_file, card_norm, cost_norm, to_predict, table_sample)

crit = nn.MSELoss()
model, best_path = train(model, train_ds, val_ds, crit, cost_norm, args)

# ========== 验证集评估 ==========
from torch.utils.data import DataLoader
def test_collate(batch):
    dicts = [item[0] for item in batch]
    cost_labels = [item[1][0] for item in batch]  # 只取 cost
    from model.database_util import collator
    batch_obj, _ = collator((dicts, cost_labels))
    return batch_obj, torch.stack(cost_labels)

val_loader = DataLoader(val_ds, batch_size=32, shuffle=False, collate_fn=test_collate)

model.eval()
preds = []
labels = []
with torch.no_grad():
    for batch, y in val_loader:
        batch = batch.to(args.device)
        y = y.to(args.device)
        y_pred = model(batch)
        if isinstance(y_pred, tuple):
            y_pred = y_pred[0]  # 只取 cost 预测
        preds.append(y_pred.detach().cpu().numpy())
        labels.append(y.detach().cpu().numpy())

preds = np.concatenate(preds)
labels = np.concatenate(labels)
preds_real = cost_norm.unnormalize_labels(preds)
labels_real = cost_norm.unnormalize_labels(labels)

# ========== 收集结果 ==========
all_results = []
print(f"========== 验证集评估结果 ==========")
for idx, (id_val, true_time, pred_time) in enumerate(zip(val_df['id'], labels_real, preds_real)):
    err = float(pred_time) - float(true_time)
    err_pct = err / float(true_time) if float(true_time) != 0 else 0
    err_pct_percent = round(err_pct * 100, 6)
    all_results.append({
        'id': id_val,
        'true': true_time,
        'pred': pred_time,
        'err': err,
        'err_pct': err_pct_percent
    })
    print(f"{id_val}\t{float(true_time):.2f}\t{float(pred_time):.2f}\t{err:.2f}\t{err_pct_percent:.6f}%")

# ========== 保存所有结果 ==========
# 根据数据目录生成对应的结果文件名
if "5g" in data_dir:
    output_filename = 'all_val_results_5g.csv'
elif "10g" in data_dir:
    output_filename = 'all_val_results_10g.csv'
else:
    output_filename = 'all_val_results.csv'

pd.DataFrame(all_results).to_csv(output_filename, index=False)
print(f"所有验证集评估结果已保存到 {output_filename}") 
import numpy as np
import os
import torch
import torch.nn as nn
import pandas as pd
import random

from model.util import Normalizer
from model.model import QueryFormer
from model.database_util import Encoding
from model.dataset import PlanTreeDataset
from model.trainer import train

# ===================== 配置 =====================

# 修改这里的数据路径来训练不同的数据集
# 可选路径：
# data_dir = "data/converted"  # 然后手动指定单个CSV文件
# 或者直接指定单个CSV文件路径
data_file = "data/converted/TPCH-10.csv"  # 请手动修改这个路径

# 读取单个CSV文件
all_df = pd.read_csv(data_file)

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

## 结果：preds_real

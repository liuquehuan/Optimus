import os
DATAPATH = os.path.join(os.path.abspath('../..'), 'data','gnn_data', 'best_plans')
NODE_DIM = 500
class arguments():
    def __init__(self):
        self.cuda = True
        self.fastmode = False
        self.seed = 42
        self.epochs = 20
        self.lr = 0.005
        self.weight_decay = 5e-4
        self.hidden = 16
        self.dropout = 0.5
args = arguments()
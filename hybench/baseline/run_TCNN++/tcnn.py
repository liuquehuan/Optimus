import torch # type: ignore
import torch.nn as nn # type: ignore

class BinaryTreeConv(nn.Module):
    def __init__(self, in_channels, out_channels):
        super(BinaryTreeConv, self).__init__()

        self.__in_channels = in_channels
        self.__out_channels = out_channels

        # we can think of the tree conv as a single dense layer
        # that we "drag" across the tree.
        ## https://pytorch.org/docs/stable/generated/torch.nn.Conv1d.html
        self.weights = nn.Conv1d(in_channels, out_channels, stride=3, kernel_size=3)

    ## 输入的flat_data是prepare_trees处理后得到的那个东西
    def forward(self, flat_data):
        trees, idxes = flat_data
        orig_idxes = idxes

        ## 初始的__in_channels应该是特征向量长度
        ## 保持idxes前两维，第三维扩充到__in_channels。然后交换第二维和第三维
        ## 现在，每棵树是一个二维矩阵（而非原本的列向量），每行有__in_channels个数，值和原先相同。然后这个二维矩阵被转置（节点和通道两维交换）
        ## trees的节点和通道两维在prepare_tree中已经被交换过
        idxes = idxes.expand(-1, -1, self.__in_channels).transpose(1, 2)

        ## https://pytorch.org/docs/stable/generated/torch.gather.html
        ## 相当于对于idxes，其元素从下标变成了对应的值
        expanded = torch.gather(trees, 2, idxes)

        ## result: batch_size * out_channels * node_count
        results = self.weights(expanded)

        # add a zero vector back on
        ## 是为了让第三维重新变为1-index
        zero_vec = torch.zeros((trees.shape[0], self.__out_channels)).unsqueeze(2)
        zero_vec = zero_vec.to(results.device)
        results = torch.cat((zero_vec, results), dim=2)
        return (results, orig_idxes)

class TreeActivation(nn.Module):
    def __init__(self, activation):
        super(TreeActivation, self).__init__()
        self.activation = activation

    def forward(self, x):
        ## x[1]是idxes，那么显然只要对x[0]应用激活函数
        return (self.activation(x[0]), x[1])

class TreeLayerNorm(nn.Module):
    def forward(self, x):
        data, idxes = x
        mean = torch.mean(data, dim=(1, 2)).unsqueeze(1).unsqueeze(1)
        std = torch.std(data, dim=(1, 2)).unsqueeze(1).unsqueeze(1)
        normd = (data - mean) / (std + 0.00001)
        return (normd, idxes)
    
class DynamicPooling(nn.Module):
    def forward(self, x):
        return torch.max(x[0], dim=2).values
    
class ExtractRoot(nn.Module):
    ## 输入：TreeLayerNorm()的输出，x[0]形状如同(batch_size, out_channels, node_count)(1-index)
    def forward(self, x):
        ## 取x[0]的第一个元素（根节点）
        return x[0][:, :, 1]
    
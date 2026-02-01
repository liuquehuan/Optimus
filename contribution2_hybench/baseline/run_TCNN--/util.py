import numpy as np
import torch


class TreeConvolutionError(Exception):
    pass

def _is_leaf(x, left_child, right_child):
    has_left = left_child(x) is not None
    has_right = right_child(x) is not None
    
    if has_left != has_right:
        raise TreeConvolutionError(
            "All nodes must have both a left and a right child or no children"
        )

    return not has_left

## 将一棵树按前序遍历展平为一个向量数组accum（二维数组），在accum的最前面加上np.zeros(accum[0].shape)（看上去是为了变为1-index）。
def _flatten(root, transformer, left_child, right_child):
    """ turns a tree into a flattened vector, preorder """

    if not callable(transformer):
        raise TreeConvolutionError(
            "Transformer must be a function mapping a tree node to a vector"
        )

    if not callable(left_child) or not callable(right_child):
        raise TreeConvolutionError(
            "left_child and right_child must be a function mapping a "
            + "tree node to its child, or None"
        )

        
    accum = []

    def recurse(x):
        if _is_leaf(x, left_child, right_child):
            accum.append(transformer(x))
            return

        accum.append(transformer(x))
        recurse(left_child(x))
        recurse(right_child(x))

    recurse(root)

    try:
        accum = [np.zeros(accum[0].shape)] + accum
    except:
        raise TreeConvolutionError(
            "Output of transformer must have a .shape (e.g., numpy array)"
        )
    
    return np.array(accum)

## 得到一个结构与输入的tree相同的东西，只不过原本每个节点存放特征向量的位置变为其preorder
def _preorder_indexes(root, left_child, right_child, idx=1):
    """ transforms a tree into a tree of preorder indexes """
    
    if not callable(left_child) or not callable(right_child):
        raise TreeConvolutionError(
            "left_child and right_child must be a function mapping a " +
            "tree node to its child, or None"
        )


    if _is_leaf(root, left_child, right_child):
        # leaf
        return idx

    def rightmost(tree):
        if isinstance(tree, tuple):
            return rightmost(tree[2])
        return tree
    
    left_subtree = _preorder_indexes(left_child(root), left_child, right_child,
                                     idx=idx+1)
    
    max_index_in_left = rightmost(left_subtree)
    right_subtree = _preorder_indexes(right_child(root), left_child, right_child,
                                      idx=max_index_in_left + 1)

    return (idx, left_subtree, right_subtree)
    
## 对于每棵树，先调用\_preorder\_indexes
## 然后通过它得到一张列表，每个元素是一个节点，按先序遍历排列，每个元素都是一个长度为3的列表，其中存放该节点、左儿子和右儿子的preorder（如果是叶子则后两个值为0）。
## 然后把它转成np.array，再展平为一维数组，最后通过reshape(-1, 1)转为一个列向量。
def _tree_conv_indexes(root, left_child, right_child):
    """ 
    Create indexes that, when used as indexes into the output of `flatten`,
    create an array such that a stride-3 1D convolution is the same as a
    tree convolution.
    """
    
    if not callable(left_child) or not callable(right_child):
        raise TreeConvolutionError(
            "left_child and right_child must be a function mapping a "
            + "tree node to its child, or None"
        )
    
    index_tree = _preorder_indexes(root, left_child, right_child)

    def recurse(root):
        if isinstance(root, tuple):
            my_id = root[0]
            left_id = root[1][0] if isinstance(root[1], tuple) else root[1]
            right_id = root[2][0] if isinstance(root[2], tuple) else root[2]
            yield [my_id, left_id, right_id]
                                           
            yield from recurse(root[1])
            yield from recurse(root[2])
        else:
            yield [root, 0, 0]

    return np.array(list(recurse(index_tree))).flatten().reshape(-1, 1)

## 保证输入的数组x中的每个元素（树）都是一个二维数组，且具有相同的列数（特征向量长度一致）。设max_first_dim是行数的最大值，对于每棵树arr，有
## ```python
## padded = np.zeros((max_first_dim, second_dim))
## padded[0:arr.shape[0]] = arr
## ```
## 也就是说，现在每棵树的行数都被扩充到和原本行数最多的那棵树一样，多余部分用0填充。
## 最后把每棵树的padded拼起来，得到三维数组vecs。
def _pad_and_combine(x):
    assert len(x) >= 1
    assert len(x[0].shape) == 2

    for itm in x:
        if itm.dtype == np.dtype("object"):
            raise TreeConvolutionError(
                "Transformer outputs could not be unified into an array. "
                + "Are they all the same size?"
            )
    
    second_dim = x[0].shape[1]
    for itm in x[1:]:
        assert itm.shape[1] == second_dim

    max_first_dim = max(arr.shape[0] for arr in x)

    vecs = []
    for arr in x:
        padded = np.zeros((max_first_dim, second_dim))
        padded[0:arr.shape[0]] = arr
        vecs.append(padded)

    return np.array(vecs)

def prepare_trees(trees, transformer, left_child, right_child, cuda=False):
    flat_trees = [_flatten(x, transformer, left_child, right_child) for x in trees]
    flat_trees = _pad_and_combine(flat_trees)
    flat_trees = torch.Tensor(flat_trees)

    # flat trees is now batch x max tree nodes x channels
    flat_trees = flat_trees.transpose(1, 2) ## 交换第二维和第三维，即通道和节点编号
    if cuda:
        flat_trees = flat_trees.cuda()

    indexes = [_tree_conv_indexes(x, left_child, right_child) for x in trees]
    indexes = _pad_and_combine(indexes)
    indexes = torch.Tensor(indexes).long()

    if cuda:
        indexes = indexes.cuda()

    return (flat_trees, indexes)
                    


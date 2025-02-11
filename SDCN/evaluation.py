import numpy as np
from munkres import Munkres, print_matrix
from sklearn.metrics.cluster import normalized_mutual_info_score as nmi_score
from sklearn.metrics import adjusted_rand_score as ari_score
from scipy.optimize import linear_sum_assignment as linear
from sklearn import metrics


def cluster_acc(y_true, y_pred):
    # 将真实标签值减去最小值，确保标签从0开始
    y_true = y_true - np.min(y_true)

    # 获取真实标签的唯一值列表
    l1 = list(set(y_true))
    # 真实标签的类别数量
    numclass1 = len(l1)

    # 获取预测标签的唯一值列表
    l2 = list(set(y_pred))
    # 预测标签的类别数量
    numclass2 = len(l2)

    # 如果真实标签和预测标签的类别数量不一致，尝试调整预测标签
    ind = 0
    if numclass1 != numclass2:
        for i in l1:
            if i in l2:
                pass
            else:
                # 将预测标签中未出现的类别值赋值给预测标签中的未匹配项
                y_pred[ind] = i
                ind += 1

    # 重新获取调整后的预测标签的唯一值列表和类别数量
    l2 = list(set(y_pred))
    numclass2 = len(l2)

    # 如果调整后真实标签和预测标签的类别数量仍然不一致，则返回错误
    if numclass1 != numclass2:
        print('error')
        return

    # 初始化成本矩阵，用于后续的匹配算法
    cost = np.zeros((numclass1, numclass2), dtype=int)
    for i, c1 in enumerate(l1):
        # 找出真实标签中等于c1的索引列表
        mps = [i1 for i1, e1 in enumerate(y_true) if e1 == c1]
        for j, c2 in enumerate(l2):
            # 找出预测标签中等于c2的索引列表，并与真实标签索引列表取交集
            mps_d = [i1 for i1 in mps if y_pred[i1] == c2]
            # 计算交集的长度，即为成本矩阵的对应元素
            cost[i][j] = len(mps_d)

    # 使用Munkres算法（也称为匈牙利算法）来匹配两个聚类结果
    m = Munkres()
    # 将成本矩阵取负数并转换为列表，以适应Munkres算法的输入要求
    cost = cost.__neg__().tolist()
    indexes = m.compute(cost)

    # 获取匹配结果
    new_predict = np.zeros(len(y_pred))
    for i, c in enumerate(l1):
        # 获取与真实标签c对应的预测标签c2
        c2 = l2[indexes[i][1]]

        # 找出预测标签中等于c2的索引列表
        ai = [ind for ind, elm in enumerate(y_pred) if elm == c2]
        # 将这些索引处的预测标签更新为真实标签c
        new_predict[ai] = c

    # 计算准确率
    acc = metrics.accuracy_score(y_true, new_predict)
    # 计算宏平均F1分数
    f1_macro = metrics.f1_score(y_true, new_predict, average='macro')
    # 计算宏平均精确率
    precision_macro = metrics.precision_score(y_true, new_predict, average='macro')
    # 计算宏平均召回率
    recall_macro = metrics.recall_score(y_true, new_predict, average='macro')
    # 计算微平均F1分数
    f1_micro = metrics.f1_score(y_true, new_predict, average='micro')
    # 计算微平均精确率
    precision_micro = metrics.precision_score(y_true, new_predict, average='micro')
    # 计算微平均召回率
    recall_micro = metrics.recall_score(y_true, new_predict, average='micro')
    # 返回准确率和宏平均F1分数
    return acc, f1_macro


def eva(y_true, y_pred, epoch=0):
    acc, f1 = cluster_acc(y_true, y_pred)
    nmi = nmi_score(y_true, y_pred, average_method='arithmetic')
    ari = ari_score(y_true, y_pred)
    print(epoch, ':acc {:.4f}'.format(acc), ', nmi {:.4f}'.format(nmi), ', ari {:.4f}'.format(ari),
            ', f1 {:.4f}'.format(f1))


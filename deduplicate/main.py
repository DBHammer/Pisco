import json
import numpy as np
import pandas as pd
import re
import logging
import sys
from collections import Counter
import time
from scipy.sparse import issparse

# 本地库
import llmConnect
import dataLoader
import resultStat
from deduplicate import DuplicateDetector

def get_top_32_ids(new_vector, X_one_hot_weighted, y, lim=32):
    # 检查是否是稀疏矩阵
    if issparse(X_one_hot_weighted):
        X_one_hot_weighted_dense = X_one_hot_weighted.toarray()
    else:
        X_one_hot_weighted_dense = X_one_hot_weighted
    
    # 确保 new_vector 是一个稠密向量
    if issparse(new_vector):
        new_vector_dense = new_vector.toarray()
    else:
        new_vector_dense = new_vector
    
    # 将 new_vector 转换为一维向量
    if new_vector_dense.ndim > 1:
        new_vector_dense = new_vector_dense.flatten()
    
    # 计算点积
    dot_products = np.dot(X_one_hot_weighted_dense, new_vector_dense)
    
    # 获取前 32 个索引
    top_32_indices = np.argsort(dot_products)[-lim:][::-1]
    
    # 使用 iloc 根据索引从 y 中提取值
    top_32_ids = y.iloc[top_32_indices].values    
    return list(top_32_ids)

if __name__ == '__main__':
    connector = llmConnect.LLMConnector()
    dataLoader = dataLoader.DataLoader()
    for x in [16]:
        print(f'filter number: {x}')
        start_time = time.time()
        

        new_data_one_hot = dataLoader.new_data_one_hot
        case_data = dataLoader.case_data
        X_one_hot_weighted = dataLoader.X_one_hot_weighted
        y = dataLoader.y
        reports = dataLoader.reports
        cases = dataLoader.cases

        results = []
        for i, new_vector in enumerate(new_data_one_hot):
            # 添加新数据的 id 和 duplicate_id
            new_case_id = case_data['id'].iloc[i]
            new_case_duplicate_id = case_data['Duplicate ID'].iloc[i]

            candidate_ids = get_top_32_ids(new_vector, X_one_hot_weighted, y, x)

            results.append((new_case_id, new_case_duplicate_id, candidate_ids))

        result_rank = []
        for result in results[:20]:
            # new_case_id 是自身的id
            # new_case_duplicate_id 是和它重复的case id 理论上应该找到这个case
            new_case_id, new_case_duplicate_id, top_32_matches = result

            candidate_reports = reports[reports['id'].isin(top_32_matches)].values.tolist()

            _candidate_ids = set(report[-1] for report in candidate_reports)
            _missing_ids = list(set(top_32_matches) - _candidate_ids)

            # if len(_missing_ids) > 0:
            #     print(f"Missing ids: {_missing_ids}")

            case_info = cases[new_case_id]

            # print(candidate_reports[0])

            detector = DuplicateDetector(candidate_reports, case_info, connector)

            rank = detector.deduplicate(new_case_duplicate_id)
            
            result_rank.append(rank)
            # winnerTree = WinnerTree(candidate_reports, case_info, connector)
            # winnerTree.build_tree()
            # layer = winnerTree.getResultLayer(new_case_duplicate_id)

            # result_layer.append(layer)
            

        rs = resultStat.ResultStat()
        rs.calculate_all_recall(results)
        rs.print_recall()
        rs.print_result_rank(result_rank)
        print(f'time cost: {time.time() - start_time}')
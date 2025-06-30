import pandas as pd
import numpy as np
import json
from sklearn.preprocessing import LabelEncoder, OneHotEncoder
from sklearn.metrics import mutual_info_score

import casePreparer

class DataLoader:
    def _get_report_data(self):
        df = pd.read_csv('TXBug-feature.csv')
        X = df.drop(columns=['id', 'Duplicate ID'])
        y = df['id']

        # 2. 编码所有非数字列
        label_encoders = {}
        for column in X.columns:
            if X[column].dtype == 'object':
                le = LabelEncoder()
                X[column] = le.fit_transform(X[column])
                label_encoders[column] = le

        # 3. 计算每列的互信息分数
        mi_scores = {}
        for column in X.columns:
            mi = mutual_info_score(X[column], y)
            mi_scores[column] = mi

        # 4. 将每一列转换为 one-hot 编码，并乘上对应的互信息
        one_hot_encoder = OneHotEncoder(handle_unknown='ignore')
        X_one_hot = one_hot_encoder.fit_transform(X)
        X_one_hot = X_one_hot.toarray()

        # 获取每个原始特征的互信息分数，并扩展到 one-hot 编码后的列
        # feature_names = one_hot_encoder.get_feature_names_out(X.columns)

        # 确保 mi_scores_expanded 的长度与 X_one_hot 的列数匹配
        mi_scores_expanded = []

        for column in X.columns:
            num_categories = len(one_hot_encoder.categories_[X.columns.get_loc(column)])
            mi_scores_expanded.extend([mi_scores[column]] * num_categories)


        # 将 one-hot 编码后的列与对应的互信息分数相乘
        X_one_hot_weighted = X_one_hot * mi_scores_expanded

        return X_one_hot_weighted, y, label_encoders, one_hot_encoder

    def _get_case_data(self, label_encoders, one_hot_encoder):
        case_data = pd.read_csv('case_data.csv')
        new_data = case_data.drop(columns=['id', 'Duplicate ID'])

        for column in new_data.columns:
            if column in label_encoders:
                le = label_encoders[column]
                new_data[column] = le.transform(new_data[column])

        # 对新数据进行 One-Hot 编码
        new_data_one_hot = one_hot_encoder.transform(new_data)

        return case_data, new_data_one_hot
    
    def _get_cases(self):
        data = json.load(open('cat.json'))
        case_data = {}

        for i in data:
            case_data[i['id']] = self.casePreparer.prepare_case(i)
        return case_data

    def __init__(self):
        with open('./knowledge/knowledge.json', 'r', encoding='utf-8') as file:
            knowledge = json.load(file) 
        self.casePreparer = casePreparer.casePreparer(knowledge)

        X_one_hot_weighted, y, label_encoders, one_hot_encoder = self._get_report_data()
        self.X_one_hot_weighted = X_one_hot_weighted
        self.y = y

        case_data, new_data_one_hot = self._get_case_data(label_encoders, one_hot_encoder)
        self.case_data = case_data
        self.new_data_one_hot = new_data_one_hot

        self.cases = self._get_cases()
        self.reports = pd.read_csv('TXBug Set.csv')

        



import numpy as np

class ResultStat:

    def __init__(self):
        self.all_recalls = []
        self.layer = []

    def _calculate_recall_at_k(self, new_case_duplicate_id, top_k_duplicates):
        recalls = []
        for k in [1, 2, 4, 8, 16, 32]:
            top_k = top_k_duplicates[:k]
            recall = sum(1 for dup_id in top_k if dup_id == new_case_duplicate_id)
            recalls.append(recall)
        return recalls

    def calculate_all_recall(self, results):
        # 计算 recall
        all_recalls = []
        for result in results:
            new_case_id, new_case_duplicate_id, top_32_matches = result    
            recalls = self._calculate_recall_at_k(new_case_duplicate_id, top_32_matches)
            all_recalls.append(recalls)
        
        self.all_recalls = np.array(all_recalls).T

    def print_recall(self):
        mean_recalls = np.mean(self.all_recalls, axis=1)
        # 打印 recall
        for i, k in enumerate([1, 2, 4, 8, 16, 32]):
            print(f"Recall@{k}: {mean_recalls[i]:.2f}")

    def print_result_layer(self, result_layer):

        layers = np.array(result_layer)
        layer_number = np.max(layers)
        print(f"Layer number: {len(layers)}")
        for i in range(-1, layer_number + 1):
            print(f"Layer {i}:{2**(i-1)}: {np.sum(layers == i)} = {100.0*np.sum(layers == i)/len(layers)}%")
    
    def print_result_rank(self, result_rank):

        ranks = np.array(result_rank)
        rank_number = np.max(ranks)
        print(f"Rank number: {len(ranks)}")
        for i in range(-1, rank_number + 1):
            print(f"Rank {i}: {np.sum(ranks == i)} = {100.0*np.sum(ranks == i)/len(ranks)}%")
    
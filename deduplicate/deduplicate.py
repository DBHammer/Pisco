import random
from collections import deque
import networkx as nx
import json
import time

import casePreparer

class UnionFind:
    def __init__(self):
        # 使用字典存储每个id的父节点
        self.parent = {}
        # 使用字典存储每个id的秩（高度）
        self.rank = {}

    def find(self, x):
        # 查找x的根节点，并进行路径压缩
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x, y):
        # 合并x和y所在的集合
        root_x = self.find(x)
        root_y = self.find(y)
        if root_x != root_y:
            # 按秩合并
            if self.rank[root_x] > self.rank[root_y]:
                self.parent[root_y] = root_x
            elif self.rank[root_x] < self.rank[root_y]:
                self.parent[root_x] = root_y
            else:
                self.parent[root_y] = root_x
                self.rank[root_x] += 1

    def is_connected(self, x, y):
        # 判断x和y是否在同一个集合中
        return self.find(x) == self.find(y)

    def all_connected(self, elements):
        # 判断所有元素是否都联通
        if not elements:
            return True
        root = self.find(elements[0])
        for elem in elements[1:]:
            if self.find(elem) != root:
                return False
        return True

    def add_element(self, x):
        # 添加新元素到并查集
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0




class BugReport:
    def __init__(self, report_id, features):
        self.id = report_id
        self.features = features  # 假设features是字典格式

class DuplicateDetector:
    def __init__(self, candidate_reports, reduced_case, LLMConnector):

        self.uf = UnionFind()
        self.candidate_reports = []  # BugReport对象列表
        with open('./knowledge/knowledge.json', 'r', encoding='utf-8') as file:
            knowledge = json.load(file) 

        prep = casePreparer.casePreparer(knowledge)

        reports = [prep.parse_report(i) for i in candidate_reports]
        for i in reports:
            self.candidate_reports.append(BugReport(i['id'].lower(),i)) 
            self.uf.add_element(i['id'].lower())


        self.reduced_case = reduced_case            # BugReport对象
        self.graph = {}                             # 邻接表表示图结构
        self._initialize_graph()
        self.connector = LLMConnector
        self.fix_edge = []
        

    
    def _initialize_graph(self):
        """初始化图的邻接表"""
        for report in self.candidate_reports:
            self.graph[report.id] = []
    
    def compare_reports(self, br1, br2):
        """
        模拟LLM比较两个报告与当前案例的相似性
        返回True表示br1更相似，False表示br2更相似
        """
        print('start comparing...')
        # 此处应实现实际的特征相似度比较逻辑
        # 示例：随机返回结果用于测试
        
        # if br1.id == self.reduced_case['case']:
        #     return 1
        # elif br2.id == self.reduced_case['case']:
        #     return 0
        # else:
        #     return br1.id > br2.id #random.choice([True, False])
        start_time = time.time()
        response = self.connector.compare_by_experts(br1,br2,self.reduced_case)
        response -= self.connector.compare_by_experts(br2,br1,self.reduced_case)
        
        print(response)
        end_time = time.time()
        # 计算执行时间
        execution_time = end_time - start_time

        # 输出执行时间
        print(f"代码执行时间: {execution_time:.6f} 秒; 结果为 {response}")

        return response >= 0

    def compare_with_confidence(self, br1, br2):
        start_time = time.time()
        response = (self.connector.compare_by_experts(br1,br2,self.reduced_case))
        response -= (self.connector.compare_by_experts(br2,br1,self.reduced_case))
        end_time = time.time()

        # 计算执行时间
        execution_time = end_time - start_time

        # 输出执行时间
        print(f"代码执行时间: {execution_time:.6f} 秒")

        return response
    
    def break_cycle_compare(self, br1, br2):
        # if br1.id == self.reduced_case['case']:
        #     return True
        # elif br2.id == self.reduced_case['case']:
        #     return False
        # else:
        return br1.id > br2.id #random.choice([True, False])
    
    def build_graph_edges(self, k=8):
        edges = set()
        all_reports = self.candidate_reports
        all_report_ids = [r.id for r in all_reports]

        for report in all_reports:
            u = report.id
            candidates = [r for r in all_reports if r.id != u]
            sampled = random.sample(candidates, min(k, int(len(candidates))))

            for other in sampled:
                v = other.id
                if (u, v) in edges or (v, u) in edges:
                    continue

                br_i = report
                br_j = other
                edges.add((u, v))
                self.uf.union(u, v)

                if self.compare_reports(br_i, br_j):
                    self.graph[u].append(v)
                else:
                    self.graph[v].append(u)

        print("end building...")

    

    def find_cycles(self):
        """使用networkx查找环"""
        G = nx.DiGraph()
        for u in self.graph:
            for v in self.graph[u]:
                G.add_edge(u, v)
        return sorted(nx.simple_cycles(G, length_bound=8))
    
    def break_cycles_final(self, cycls):
        print("break cycle final...")
        """打破图中的循环结构"""
        # cycles = self.find_cycles()
        # 按大小降序排序
        cycles = sorted(cycles, key=lambda x: len(x), reverse=True)
        for nodes_in_cycle in cycles:
        
            cycle_exist = (nodes_in_cycle[-1] in self.graph[nodes_in_cycle[0]])
            edges_in_cycle = [(nodes_in_cycle[-1], nodes_in_cycle[0])]
            for i in range(len(nodes_in_cycle)-1):
                e = (nodes_in_cycle[i], nodes_in_cycle[i+1])
                edges_in_cycle.append(e)

            # 尝试打破循环
            for u, v in edges_in_cycle:
                if (u,v) in self.fix_edge:
                    continue
            # 重新比较两个节点
                br_u = next(r for r in self.candidate_reports if r.id == u)
                br_v = next(r for r in self.candidate_reports if r.id == v)
                confidence = self.compare_with_confidence(br_u, br_v)
                if confidence < 0:
                    # 反转这条边
                    print(f"Reversing edge: {u} -> {v}")
                    if v in self.graph.get(u, []):
                        self.graph[u].remove(v)  # 删除原边
                    self.graph[v].append(u)
        print("break cycle ends ...")

    def break_cycles(self, cycles):
        print("break cycle ...")
        """打破图中的循环结构"""
        # cycles = self.find_cycles()
        # 按大小降序排序
        cycles = sorted(cycles, key=lambda x: len(x), reverse=True)
        nodes_in_cycle = cycles[0]
        
        cycle_exist = (nodes_in_cycle[-1] in self.graph[nodes_in_cycle[0]])
        edges_in_cycle = [(nodes_in_cycle[-1], nodes_in_cycle[0])]
        for i in range(len(nodes_in_cycle)-1):
            e = (nodes_in_cycle[i], nodes_in_cycle[i+1])
            edges_in_cycle.append(e)

        print(f"break cycle whose edge is: {len(edges_in_cycle)}")
        min_confidence = 1e9
        reverse_edge = None
        # 尝试打破循环
        for u, v in edges_in_cycle:
            if (u,v) in self.fix_edge:
                continue
            # 重新比较两个节点
            br_u = next(r for r in self.candidate_reports if r.id == u)
            br_v = next(r for r in self.candidate_reports if r.id == v)
            confidence = self.compare_with_confidence(br_u, br_v)
            if confidence < 0:
            # 反转这条边
                print(f"Reversing edge: {u} -> {v}")
                if v in self.graph.get(u, []):
                    self.graph[u].remove(v)  # 删除原边
                self.graph[v].append(u)
                self.fix_edge.append((v,u))
        print("break cycle ends ...")
    
    
    def find_most_similar(self):
        """查找入度为0的节点（最相似报告）"""
        in_degree = {report.id:0 for report in self.candidate_reports}
        for u in self.graph:
            for v in self.graph[u]:
                in_degree[v] += 1
        nodes = []
        min_degree = 100
        min_node = None
        for node, degree in in_degree.items():
            if degree == 0:
                nodes.append(node)

        return nodes
    
    def get_similar_rank(self):
        in_degree = {report.id:0 for report in self.candidate_reports}
        for u in self.graph:
            for v in self.graph[u]:
                in_degree[v] += 1
        sorted_ids = [report_id for report_id, _ in sorted(in_degree.items(), key=lambda item: item[1])]
        return sorted_ids
    
    def check_root_cause(self, report_id):
        """模拟根因检查"""
        return random.choice([True, False])
    
    def deduplicate(self, duplicate_id):
        """执行去重主流程"""
        # 构建初始图
        self.build_graph_edges()
        
        # 循环处理直到无环
        cnt = 0
        cycles = self.find_cycles()
        while len(cycles) > 0 and cnt < 5:
            print("start break cycles")
            self.break_cycles(cycles)
            cnt += 1
            cycles = self.find_cycles()


        
        ranks = self.get_similar_rank()

        # print("ranks is : " + str(ranks))
        # print("answer is : " + duplicate_id)
        # positions = [index for index, value in enumerate(ranks) if value == duplicate_id.lower()]
        # ranks = self.find_most_similar()
        position = -1
        for index, value in enumerate(ranks):
            if value.lower() == duplicate_id.lower():
                position = index

        print("ranks is : " + str(ranks))
        print("answer is : " + duplicate_id)
        print("position is : " + str(position))

        return position

        # print(self.get_similar_rank())
        # # 查找最相似报告
        # most_similar_id = self.find_most_similar()
        
        # if not most_similar_id:
        #     return "New bug: No similar report found"
        
        # # 检查根因是否相同
        # if self.check_root_cause(most_similar_id):
        #     return f"Duplicate with report {most_similar_id}"
        # else:
        #     return "New bug: Different root cause"

# 测试用例
if __name__ == "__main__":
    # 生成测试数据
    reports = [
        BugReport("BR1", {"txn_num": 2, "ops": ["SELECT", "UPDATE"]}),
        BugReport("BR2", {"txn_num": 3, "ops": ["INSERT", "SELECT"]}),
        BugReport("BR3", {"txn_num": 2, "ops": ["UPDATE", "SELECT"]}),
        BugReport("BR4", {"txn_num": 4, "ops": ["DELETE", "SELECT"]})
    ]
    current_case = BugReport("Cr", {"txn_num": 2, "ops": ["SELECT", "UPDATE"]})
    
    # 执行去重检测
    detector = DuplicateDetector(reports, current_case)
    result = detector.deduplicate()
    print("Detection Result:", result)

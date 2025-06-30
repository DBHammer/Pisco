import logging
import json
import re

# 用于转换case为询问LLM时的格式
class casePreparer:
    def __init__(self, knowledge: dict = {}):
        self.logger = logging.getLogger(__name__)
        self.knowledge = knowledge
    
    def prepare_case(self, ori_case: json):
        # return ori_case
        return self._prepare_case_comment(ori_case)
        # return self._prepare_case_comment_with_knowledge(ori_case)

    # 只是删掉不能暴露的信息
    def _prepare_case_ori(self, ori_case: json):
        new_case = ori_case.copy()

        return new_case

    # 获取操作的结果集
    def _parse_result(self, op: json, thread: str, il: dict):
        if 'start transaction' in op['sql']:
            return il.get(thread, 'REPEATABLE_READ')

        pattern = r'\"(\w+)\":\s*\"(\d+)\"'

        tuples = None
        if 'writeTupleList' in op:
            header = 'write'
            tuples = op['writeTupleList']
        elif 'readTupleList' in op:
            header = 'read'
            tuples = op['readTupleList']
        else:
            return ''

        body = ''
        for i in tuples:
            tuple_info = ",".join([str(v) for k, v in re.findall(pattern, i)])
            output_str = f'(row {i.split("->")[0]}->{tuple_info})'
            body += output_str

        return f'{header} {body}'

    # 计算操作序列
    def _parse_seq(self, ori_case: json):
        seq = []
        operations = []
        for i in ori_case['operation']:
            operations += i

        for op in operations:
            thread = '-'.join(op['operationID'].split(',')[:2])
            r = self._parse_result(op, thread, ori_case['isolation_level'])
            seq.append(f'{thread}> {op["sql"]} -- {r};')

        return seq
    
    # 获取对应的文档知识
    def _get_doc(self, op: json, thread: str, ori_case: dict):
        il = ori_case['isolation_level'].get(thread, 'REPEATABLE_READ')
        db = ori_case['database']

        docs = self.knowledge[db][il]

        if 'start' in op['sql'] or 'commit' in op['sql']:
            return ''
        elif op['sql'].startswith('select'):
            return docs['Read_Behavior']
        else:
            return docs['Write_Behavior']
    
    # 计算操作序列，并追加文档知识
    def _parse_seq_with_knowledge(self, ori_case: json):
        seq = []
        operations = []
        for i in ori_case['operation']:
            operations += i

        for op in operations:
            thread = '-'.join(op['operationID'].split(',')[:2])
            r = self._parse_result(op, thread, ori_case['isolation_level'])
            expect_behavior = self._get_doc(op, thread, ori_case)
            if len(expect_behavior) > 0:
                seq.append(f'{thread}> {op["sql"]} -- {r}, expect {expect_behavior};')

        return seq

    # 优化表述格式
    def _prepare_case_comment(self, ori_case: json):
        new_case = {}

        new_case['case'] = ori_case['case']
        new_case['database'] = ori_case['database']
        new_case['schema'] = ';'.join(ori_case['schema'])
        new_case['execution sequence'] = self._parse_seq(ori_case)

        return new_case
    
    # 优化表述格式并追加知识库
    def _prepare_case_comment_with_knowledge(self, ori_case: json):
        new_case = {}

        new_case['case'] = ori_case['case']
        new_case['database'] = ori_case['database']
        new_case['schema'] = ''.join(ori_case['schema'])
        new_case['execution sequence'] = self._parse_seq_with_knowledge(ori_case)

        return new_case

    def parse_report(self, report):
        report_info = {
                # 'summary': report[3],
                'isolation level': report[10],
                'description': report[4],
                'database': report[0],
                'id': report[-1]
            }
        return report_info

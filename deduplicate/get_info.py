# output json: <database, isolation_level, schema, init_data, operation>
import pandas as pd
import os
import json
import json

# 递归解析最终结果的字符串
def parse_result(result:str)->(str,int):
    parsed_result = []
    i = 0
    cnt = 0
    while i < len(result):
        # 如果是'['就向下递归，把结果作为嵌套的结果
        if result[i] == '[':
            sub_result,offset = parse_result(result[i+1:])
            parsed_result.append(sub_result)
            i += offset + 1
            # print(i, result[i])
            continue
        # 如果是']'就返回当前结果，用空格划分
        if result[i] == ']':
            if len(parsed_result) == 0:
                return result[:i].split(' '),i+1
            else:
                return parsed_result,i+1
        i += 1
    
    if len(parsed_result) == 0:
        return result.split(' '),len(result)
    else:
        return parsed_result,len(result)


# 解开多个嵌套列表，改为一个列表多个元素: [[op1], [op2]] --> [op1, op2]
def extract_elements(nested_list):
    elements = []
    for item in nested_list:
        if isinstance(item, list):
            # 如果是列表，则递归调用该函数
            elements.extend(extract_elements(item))
        else:
            # 否则，直接添加到结果列表中
            elements.append(item)
    return elements

# 把嵌套列表的元素映射为trace中的完整信息
def mapping_elements(nested_list, final_op_info):
    elements = []
    for item in nested_list:
        if isinstance(item, list):
            # 如果是列表，则递归调用该函数
            elements.append(mapping_elements(item, final_op_info))
        else:
            # 否则，直接添加到结果列表中
            elements.append(final_op_info[item])
    return elements

def read_last_line(file_path):
    with open(file_path, 'rb') as file:
        file.seek(-2, os.SEEK_END)
        while file.read(1) != b'\n':
            file.seek(-2, os.SEEK_CUR)
        last_line = file.readline().decode()
    return last_line

# get the case name and the result sequence
case_description = []
class case_description_st:
    def __init__(self, database, number, dir, last_line):
        self.database = database
        self.number = number
        self.dir = dir
        self.result_seq = last_line
        self.result_op = ''

for root, dirs, files in os.walk('./cases'):
    for name in files:
        str = name.split('-')
        database_name = str[0]
        if database_name == 'mariadb':
            database_name = 'maria'
        case_dir = database_name + '_' + str[1] + '/' + str[2] + '-' + str[3] + '-' 
        file_path = os.path.join(root, name)
        last_line = read_last_line(file_path)
        case_1 = case_description_st(str[0], str[1], case_dir, last_line)
        case_description.append(case_1)

# 解析并处理每个 result_seq
for c1 in case_description:
    c1.result_seq = c1.result_seq.replace('Operation sequence: ','')
    # 把结果解析为两层的嵌套列表
    c1.result_seq = parse_result(c1.result_seq)[0][0]
    # print(c1.result_seq)
    # 提取结果中的操作id
    c1.result_op = extract_elements(c1.result_seq)
    # print(c1.result_op)
    # fix bug: ['0-0-10,272,5,', '0-0-2,254,4'] --> ['0-0-10,272,5', '0-0-2,254,4']
    for i in range(len(c1.result_seq)):
        for j in range(len(c1.result_seq[i])):
            if c1.result_seq[i][j][-1] == ',':
                c1.result_seq[i][j] = c1.result_seq[i][j][:-1]
    for i in range(len(c1.result_op)):
        if c1.result_op[i][-1] == ',':
            c1.result_op[i] = c1.result_op[i][:-1]

# 调整tuple list的格式
def parse_tuple(tuple_list:json)->str:
    tuples = []
    for t in tuple_list:
        # print(type(t['primaryKey']))
        # print(type(t['valueMap']))
        # _ = str(t['primaryKey']) + '->' + str(t['valueMap'])
        _ = t['primaryKey'] + '->' + json.dumps(t['valueMap'])
        # 如果涉及到最后一个读写操作的主键，则用红色标记
        tuples.append(_)
    return tuples


# 读取trace中的数据
all_result_list = []
for c1 in case_description:
    src = '/home/wsy/pisco/bug_case/' + c1.dir + '/out'
    if os.path.exists(src) == False:
        print("dir not exist: " + src)
        continue
    result = {}
    result['id'] = c1.dir
    results = [] # traces

    final_op_info = {}
    final_txn = {}
    txn_il = {}
    
    # 遍历所有 trace
    for root, dirs, files in os.walk(src + '/trace'):
        for file in files:
            file_path = os.path.join(root, file)
            results += json.loads(open(file_path).read())
            # print(results[0])
    for i in results:
        if i['operationID'] in c1.result_op:
            final_op_info[i['operationID']] = i
            if 'isolationLevel' in i:
                txn_il[i['transactionID']] = i['isolationLevel']
            final_txn[i['transactionID']] = final_txn.get(i['transactionID'],0) + 1

    result['database'] = c1.database
    result['case'] = c1.database + '_' + c1.number
    result['isolation_level'] = txn_il
    result['schema'] = open(src + '/schema/schema.sql').readlines()
    result['init_data'] = open(src + '/init_data/dataInsert.sql').readlines()
    
    try:
        final_result_info = mapping_elements(c1.result_seq, final_op_info)
    except:
        print("error: ", c1.dir)
        print(c1.result_seq)
        continue

    # final_result_info = mapping_elements(c1.result_seq, final_op_info)
    
    
    rows = []
    for i in final_result_info:
        row = []
        # 看每一行有没有对应线程id的操作
        for op in i:
            op_new = {}
            op_new['operationID'] = op['operationID']
            if 'sql' in op:
                op_new['sql'] = op['sql']
            else:
                op_new['sql'] = op['operationTraceType']
    
            if 'readTupleList' in op:
                op_new['readTupleList'] = parse_tuple(op['readTupleList']) 
            if 'writeTupleList' in op:
                op_new['writeTupleList'] = parse_tuple(op['writeTupleList']) 
    
            # 如果没找到对应线程id的操作，则用空字符串填充
            row.append(op_new)
        rows.append(row)
    
    result['operation'] = rows
    all_result_list.append(result)


# Write JSON data to a file
with open('./output.json', 'w') as file:
    # 直接写入，一行 json
    # json_data = json.dumps(all_result_list)
    # file.write(json_data)
    
    # json格式化
    json.dump(all_result_list, file, indent=4)
import json
import os
import plotly as py
import plotly.graph_objs as go
import sys
import re

pyplt = py.offline.plot


def paint_fail_cause(dirname):
    file_list = os.listdir(dirname)
    percent_map_list = []
    for filename in file_list:
        filepath = f'{dirname}/{filename}'

        try:
            infile = open(filepath, encoding='utf-8')
        except UnicodeDecodeError:
            infile = open(filepath, encoding='gbk')

        json_array = json.load(infile)
        percent_map = json_array[-1]['percentMap']
        percent_map_list.append(percent_map)

    percent_sum_map = {}
    for percent_map in percent_map_list:
        for key, val in percent_map.items():
            if key not in percent_sum_map:
                percent_sum_map[key] = val
            else:
                percent_sum_map[key] += val

    labels = list(percent_sum_map.keys())
    values = [percent_sum_map[key] for key in labels]

    pieTrace = go.Pie(
        values=values, 
        labels=labels,
        title="Transaction Execution Result",
        textinfo='label+percent')
    
    pyplt([pieTrace], filename='pie_of_fail.html')


def paint_commit_rate(dirname):
    file_list = os.listdir(dirname)
    data_map = {}
    digits_pattern = re.compile(r'\d+')
    for filename in file_list:
        filepath = f'{dirname}/{filename}'
        infile = open(filepath, encoding='utf-8')
        json_array = json.load(infile)
        percent_map = json_array[-1]['percentMap']
        commit_rate_list = []
        for item in json_array:
            percent_map = item['percentMap']
            if 'Commit' in percent_map:
                commit_rate = percent_map['Commit']
            else:
                commit_rate = 0
            commit_rate_list.append(commit_rate)

            loader_id = digits_pattern.findall(filename)[0]
            loader_name = f'loader-{loader_id}'
        data_map[loader_name] = commit_rate_list

    sorted_loader_names = sorted(list(data_map.keys()),
                                 key=lambda name:int(re.compile(r'\d+').findall(name)[0]))
    data = []
    for loader_name in sorted_loader_names:
        trace_tmp = go.Scatter(y=data_map[loader_name], name=loader_name)
        data.append(trace_tmp)

    layout = go.Layout(
        title="Commit rate - Count of transaction",
        xaxis=dict(title='Count of transaction'),
        yaxis=dict(title='Commit rate')
    )
    fig = go.Figure(data, layout)
    pyplt(fig, filename='commit_rate_with_count_of_transaction.html')


if __name__ == '__main__':
    stat_dir = sys.argv[1]
    paint_fail_cause(stat_dir)
    paint_commit_rate(stat_dir)

import jsonlines
import matplotlib.pyplot as plt
import networkx as nx
import os
import sys

# Load json
jsonFile = os.getcwd() + '/' + sys.argv[1]

jsonList = []
with open(jsonFile, 'r') as f:
    for item in jsonlines.Reader(f):
        jsonList.append(item)
edge = []
# all dependecy
dependency_list = []
# dependency_String
dependency = []
for e in jsonList[0]['dependencySet']:
    tmp = (e['fromProfile']['transactionID'], e['toProfile']['transactionID'])
    edge.append(tmp)
    dependency_list.append(e['dependencies'])

for i in dependency_list:
    if len(i) > 1:
        strCom = ''
        for j in i:
            strCom += "".join(j + ',')
        strCom = strCom[:-1]
    else:
        strCom = i[0]
    dependency.append(strCom)
# paint
G = nx.DiGraph()
for index in range(len(edge)):
    G.add_edge(edge[index][0], edge[index][1], capacity=dependency[index])
pos = nx.spring_layout(G)
capacity = nx.get_edge_attributes(G, 'capacity')
nx.draw_networkx_nodes(G, pos, node_color='blue', alpha=0.5)
nx.draw_networkx_edges(G, pos,  arrowstyle='->')
nx.draw_networkx_labels(G, pos)
nx.draw_networkx_edge_labels(G, pos, capacity)
plt.show()
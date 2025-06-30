import datetime
import jsonlines
import pandas as pd
import plotly
import plotly.express as px
import sys

# Load json

# Load json file from cwd (or use a absolute path)
jsonFile = sys.argv[1]
print(jsonFile)

jsonList = []
with open(jsonFile, 'r', encoding='utf-8') as f:
    for item in jsonlines.Reader(f):
        jsonList.append(item)

# Fix x's tickmode problem
timeList = []
for i in jsonList:
    timeList.append(i['startTimestamp'])
    timeList.append(i['finishTimestamp'])
sortedTimeListSet = sorted(set(timeList))
offsetDict = dict(zip(sortedTimeListSet, range(len(sortedTimeListSet))))
start = datetime.datetime(2020, 1, 1)
dataDictList = []

for i in jsonList:
    sidx, finx = i['startTimestamp'], i['finishTimestamp']
    sts = (start + datetime.timedelta(days=offsetDict[sidx])).strftime("%Y-%m-%d")
    fts = (start + datetime.timedelta(days=offsetDict[finx])).strftime("%Y-%m-%d")
    dataDictList.append(dict(threadID=i['threadID'],
                             startValue=sts, finishValue=fts,
                             Resource=i['threadID'],
                             transactionID=i['transactionID'],
                             operationID=i['operationID'],
                             operationTraceType=i['operationTraceType'],
                             startTimestamp=i['startTimestamp'],
                             finishTimestamp=i['finishTimestamp']))  # ,
#                          predicateLock=i['predicateLock'],
#                          traceLockMode=i['traceLockMode'],
#                          readMode=i['readMode']))

# Paint timeline gragh
fig = px.timeline(pd.DataFrame(dataDictList),
                  x_start="startValue",
                  x_end="finishValue",
                  y="threadID",
                  facet_row_spacing=0.02,
                  hover_data={'startTimestamp': True,
                              'finishTimestamp': True,
                              'transactionID': True,
                              'operationID': True,
                              'operationTraceType': True,
                              'startValue': False,
                              'finishValue': False})
# Adjust layout
fig.update_yaxes(autorange="reversed")
fig.update_layout(
    xaxis=dict(visible =False),
    title={'text': 'Visualization',
           'xanchor': 'center',
           'yanchor': 'top',
           'x': 0.5,
           'y': 0.95})

#fig.show()

# Save output in cwd (or use a absolute path)
htmlFile = sys.argv[2]
print(htmlFile)
plotly.io.write_html(fig, file=htmlFile)

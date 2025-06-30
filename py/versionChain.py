import datetime
import jsonlines
import os
import pandas as pd
import plotly
import plotly.express as px
import sys

#Load .json file
jsonFile = os.getcwd() + '/' + sys.argv[1]
jsonList, versionList, timeList = [], [], []
with open(jsonFile, 'r') as f:
    for item in jsonlines.Reader(f):
        jsonList.append(item)

#Append Operations
for i in range(len(jsonList)-1):
    jsonList[i]['status'] = 'Operation'
    versionList.append(jsonList[i])
#Append VersionChain
for i in jsonList[-1]['versionChain']:
    versionList.append(i)
#Init timeList
for i in versionList:
    timeList.append(i['startTimestamp'])
    timeList.append(i['finishTimestamp'])

# Fix x's tickmode problem
sortedTimeListSet = sorted(set(timeList))
offsetDict = dict(zip(sortedTimeListSet, range(len(sortedTimeListSet))))

# paint
start = datetime.datetime(2020, 1, 1)
dictList = []
versionName = [str(i) for i in range(len(versionList))]
versionIndex = 0
for i in versionList:
    sidx, finx = i['startTimestamp'], i['finishTimestamp']
    sts = (start + datetime.timedelta(days=offsetDict[sidx])).strftime("%Y-%m-%d")
    fts = (start + datetime.timedelta(days=offsetDict[finx])).strftime("%Y-%m-%d")
    dictList.append(dict(version=versionName[versionIndex],
                         startValue=sts,
                         finishValue=fts,
                         Tag=i['status'],
                         transactionID=i['transactionID'],
                         startTimestamp=i['startTimestamp'],
                         finishTimestamp=i['finishTimestamp']))
    versionIndex += 1

# put into pd
fig = px.timeline(pd.DataFrame(dictList),
                  x_start="startValue",
                  x_end="finishValue",
                  y="version",
                  color="Tag",
                  hover_data={'startTimestamp': True,
                              'finishTimestamp': True,
                              'transactionID': True,
                              'Tag': False,
                              'startValue': False,
                              'finishValue': False,
                              'version': False})
# Adjust layout
fig.update_yaxes(autorange="reversed")
fig.update_layout(
    xaxis=dict(visible=False),
    yaxis=dict(visible=False),
    title={'text': 'Version Chain',
           'xanchor': 'center',
           'yanchor': 'top',
           'x': 0.5,
           'y': 0.95})

# Save output
htmlFile = os.getcwd() + '/' + sys.argv[2]
print(htmlFile)
plotly.io.write_html(fig, file=htmlFile)

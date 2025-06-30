import datetime
import json
import os
import pandas as pd
import plotly
import plotly.express as px
import sys

# trans data to pd.Dataframe
def getTimelineDict(jsonList):
    versionList, timeList = [], []
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
    versionIndex = 0
    for i in versionList:
        sidx, finx = i['startTimestamp'], i['finishTimestamp']
        sts = (start + datetime.timedelta(days=offsetDict[sidx])).strftime("%Y-%m-%d")
        fts = (start + datetime.timedelta(days=offsetDict[finx])).strftime("%Y-%m-%d")
        thread = i['transactionID'].split(',')[0]
        dictList.append(dict(version=str(versionIndex),
                            startValue=sts,
                            thread=thread,
                            finishValue=fts,
                            Tag=i['status'],
                            transactionID=i['transactionID'],
                            startTimestamp=i['startTimestamp'],
                            finishTimestamp=i['finishTimestamp']))
        versionIndex += 1
    
    return pd.DataFrame(dictList)

# generate figure for one error
def genFig(jsonList,title):
    fig = px.timeline(pd.DataFrame(getTimelineDict(jsonList)),
                    x_start="startValue",
                    x_end="finishValue",
                    y="thread",
                    color="Tag",
                    hover_data={'startTimestamp': True,
                                'finishTimestamp': True,
                                'transactionID': True,
                                'Tag': False,
                                'startValue': False,
                                'finishValue': False,
                                'thread': False,
                                'version': False})
    # Adjust layout
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(
        xaxis=dict(visible=False),
        yaxis=dict(visible=True),
        title={'text': title,
            'xanchor': 'center',
            'yanchor': 'top',
            'x': 0.5,
            'y': 0.95})
    return fig
def split2Dict(l):
    idx = l[1:].index('\"')+2
    dict = {l[:idx]:l[idx:]}
    return dict


#Load .json file
jsonFile = os.getcwd() + '/' + sys.argv[1]
f = open(jsonFile,'r',encoding='utf8')
lines = f.readlines()
f.close()
# Save output
htmlFile = os.getcwd() + '/' + sys.argv[2]
print(htmlFile)





infoList = []
lst = []
htmlStr = '<html><body>'

# decode json file
for l in lines:
    tmp = l.strip()
    if len(tmp) == 0:
        if len(lst) > 0:
            infoList.append(lst)
        lst = []
        continue
    lst.append(split2Dict(tmp))

if len(lst) > 0:
    infoList.append(lst)

# generate fig for each error
for info in infoList:
    title = list(info[0].keys())[0]
    jsonList = []
    for l in info:
        if 'version chain' in list(l.keys())[0]:
            jsonList.append(json.loads(l['"the version chain(VC) on R: "']))
        elif '"the error record(R) of T1,Op"' == list(l.keys())[0]:
            _ = json.loads(l['"the error record(R) of T1,Op"'])
            title += 'table='+_['table']+';pk='+_['primaryKey']
    fig = genFig(jsonList,title)
    htmlStr += plotly.io.to_html(fig, full_html=False)


htmlStr += '</body></html>'

# load to html
f = open(htmlFile,'w',encoding='utf8')
f.writelines(htmlStr)
f.close()

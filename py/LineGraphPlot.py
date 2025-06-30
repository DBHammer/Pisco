import matplotlib.pyplot as plt
import numpy as np
import xlrd

absolute_path = r"D:\Desktop\coverage(2)(1).xlsx"
workbook = xlrd.open_workbook(absolute_path)
fontFamily = 'Times New Roman'
fontSize = 20
pixel = 500
resolutionRatio = 500


# Plot total line coverage
def LineTotal():
    worksheet = workbook.sheet_by_name(u'Line')
    TPCC = []
    YCSB = []
    SmallBank = []
    SQLSmith = []
    SQLancer = []
    Squirrel = []
    Orca = []
    for row in range(76, 88):
        TPCC.append(int(float(str(worksheet.cell(row, 1).value).strip('\t'))))
        YCSB.append(int(float(str(worksheet.cell(row, 2).value).strip('\t'))))
        SmallBank.append(int(float(str(worksheet.cell(row, 3).value).strip('\t'))))
        SQLSmith.append(int(float(str(worksheet.cell(row, 4).value).strip('\t'))))
        SQLancer.append(int(float(str(worksheet.cell(row, 5).value).strip('\t'))))
        Squirrel.append(int(float(str(worksheet.cell(row, 6).value).strip('\t'))))
        Orca.append(int(float(str(worksheet.cell(row, 7).value).strip('\t'))))
    plt.figure(figsize=(8, 5))
    axisX = np.array(range(10, 130, 10))
    plt.plot(axisX, TPCC, color='#50CB93', linestyle=':', marker='x', markersize=10, markerfacecolor='white')
    plt.plot(axisX, YCSB, color='#7DEDFF', linestyle='-.', marker='2', markersize=10, markerfacecolor='white')
    plt.plot(axisX, SmallBank, color='#7C83FD', linestyle='--', marker='>', markersize=10,
             markerfacecolor='white')
    plt.plot(axisX, SQLSmith, color='#ECD662', linestyle='-.', marker='^', markersize=10,
             markerfacecolor='white')
    plt.plot(axisX, SQLancer, color='#FB7AFC', linestyle='--', marker='s', markersize=10,
             markerfacecolor='white')
    plt.plot(axisX, Squirrel, color='#FFAA4C', linestyle=':', marker='+', markersize=10,
             markerfacecolor='white')
    plt.plot(axisX, Orca, color='r', linestyle=':', marker='o', markersize=10, markerfacecolor='white')
    plt.ylabel("Line Coverage", fontdict={'size': fontSize})
    plt.rcParams['font.sans-serif'] = fontFamily
    my_y_ticks = np.arange(4650, 10500, 600)
    my_x_ticks = np.arange(0, 125, 10)
    plt.ylim((4650, 10500))
    plt.tick_params(labelsize=15)
    plt.yticks(my_y_ticks)
    plt.xticks(my_x_ticks)
    plt.xlim(5, 125)
    plt.rcParams['savefig.dpi'] = pixel  # 图片像素
    plt.rcParams['figure.dpi'] = resolutionRatio  # 分辨率
    ax = plt.gca()
    ax.spines['right'].set_color('none')
    ax.spines['top'].set_color('none')
    plt.savefig("line-total.pdf", bbox_inches='tight')
    plt.show()


# Plot total branch coverage
def BranchTotal():
    workbook = xlrd.open_workbook(absolute_path)
    worksheet = workbook.sheet_by_name(u'Branch')
    TPCC = []
    YCSB = []
    SmallBank = []
    SQLSmith = []
    SQLancer = []
    Squirrel = []
    Orca = []
    for row in range(76, 88):
        TPCC.append(int(float(str(worksheet.cell(row, 1).value).strip('\t'))))
        YCSB.append(int(float(str(worksheet.cell(row, 2).value).strip('\t'))))
        SmallBank.append(int(float(str(worksheet.cell(row, 3).value).strip('\t'))))
        SQLSmith.append(int(float(str(worksheet.cell(row, 4).value).strip('\t'))))
        SQLancer.append(int(float(str(worksheet.cell(row, 5).value).strip('\t'))))
        Squirrel.append(int(float(str(worksheet.cell(row, 6).value).strip('\t'))))
        Orca.append(int(float(str(worksheet.cell(row, 7).value).strip('\t'))))
    plt.figure(figsize=(8, 5))
    axisX = np.array(range(10, 130, 10))
    plt.plot(axisX, TPCC, color='#50CB93', linestyle=':', marker='x', markersize=10, markerfacecolor='white')
    plt.plot(axisX, YCSB, color='#7DEDFF', linestyle='-.', marker='2', markersize=10, markerfacecolor='white')
    plt.plot(axisX, SmallBank, color='#7C83FD', linestyle='--', marker='>', markersize=10,
             markerfacecolor='white')
    plt.plot(axisX, SQLSmith, color='#ECD662', linestyle='-.', marker='^', markersize=10,
             markerfacecolor='white')
    plt.plot(axisX, SQLancer, color='#FB7AFC', linestyle='--', marker='s', markersize=10,
             markerfacecolor='white')
    plt.plot(axisX, Squirrel, color='#FFAA4C', linestyle=':', marker='+', markersize=10,
             markerfacecolor='white')
    plt.plot(axisX, Orca, color='r', linestyle=':', marker='o', markersize=10, markerfacecolor='white')
    plt.ylabel("Branch Coverage", fontdict={'size': fontSize})
    plt.rcParams['font.sans-serif'] = fontFamily
    my_y_ticks = np.arange(1850, 4560, 250)
    my_x_ticks = np.arange(0, 125, 10)
    plt.tick_params(labelsize=15)
    plt.ylim((1850, 4560))
    plt.yticks(my_y_ticks)
    plt.xticks(my_x_ticks)
    plt.xlim(5, 125)
    ax = plt.gca()
    plt.rcParams['savefig.dpi'] = pixel  # 图片像素
    plt.rcParams['figure.dpi'] = resolutionRatio  # 分辨率
    ax.spines['right'].set_color('none')
    ax.spines['top'].set_color('none')
    plt.savefig("branch-total.pdf", bbox_inches='tight')
    plt.show()


# Plot total function coverage
def FuncTotal():
    workbook = xlrd.open_workbook(absolute_path)
    worksheet = workbook.sheet_by_name(u'Function')
    TPCC = []
    YCSB = []
    SmallBank = []
    SQLSmith = []
    SQLancer = []
    Squirrel = []
    Orca = []
    for row in range(76, 88):
        TPCC.append(int(float(str(worksheet.cell(row, 1).value).strip('\t'))))
        YCSB.append(int(float(str(worksheet.cell(row, 2).value).strip('\t'))))
        SmallBank.append(int(float(str(worksheet.cell(row, 3).value).strip('\t'))))
        SQLSmith.append(int(float(str(worksheet.cell(row, 4).value).strip('\t'))))
        SQLancer.append(int(float(str(worksheet.cell(row, 5).value).strip('\t'))))
        Squirrel.append(int(float(str(worksheet.cell(row, 6).value).strip('\t'))))
        Orca.append(int(float(str(worksheet.cell(row, 7).value).strip('\t'))))
    plt.figure(figsize=(8, 5))
    axisX = np.array(range(10, 130, 10))
    plt.plot(axisX, TPCC, color='#50CB93', linestyle=':', marker='x', markersize=10, markerfacecolor='white')
    plt.plot(axisX, YCSB, color='#7DEDFF', linestyle='-.', marker='2', markersize=10, markerfacecolor='white')
    plt.plot(axisX, SmallBank, color='#7C83FD', linestyle='--', marker='>', markersize=10,
             markerfacecolor='white')
    plt.plot(axisX, SQLSmith, color='#ECD662', linestyle='-.', marker='^', markersize=10,
             markerfacecolor='white')
    plt.plot(axisX, SQLancer, color='#FB7AFC', linestyle='--', marker='s', markersize=10,
             markerfacecolor='white')
    plt.plot(axisX, Squirrel, color='#FFAA4C', linestyle=':', marker='+', markersize=10,
             markerfacecolor='white')
    plt.plot(axisX, Orca, color='r', linestyle=':', marker='o', markersize=10, markerfacecolor='white')
    plt.ylabel("Function Coverage", fontdict={'size': fontSize})
    plt.rcParams['font.sans-serif'] = fontFamily
    my_y_ticks = np.arange(360, 740, 40)
    my_x_ticks = np.arange(0, 125, 10)
    plt.tick_params(labelsize=15)
    plt.ylim((360, 740))
    plt.yticks(my_y_ticks)
    plt.xticks(my_x_ticks)
    plt.xlim(5, 125)
    ax = plt.gca()
    plt.rcParams['savefig.dpi'] = pixel  # 图片像素
    plt.rcParams['figure.dpi'] = resolutionRatio  # 分辨率
    ax.spines['right'].set_color('none')
    ax.spines['top'].set_color('none')
    plt.savefig("function-total.pdf", bbox_inches='tight')
    plt.show()


# export legend
def export_legend(expand=None):
    if expand is None:
        expand = [-2, -2, 2, 2]
    colors = ['#50CB93', '#7DEDFF', '#7C83FD', '#ECD662', '#FB7AFC', '#FFAA4C', 'red']
    markers = ['x', '2', '>', '^', 's', '+', 'o']
    config = lambda m, c: plt.plot([], [], marker=m, color=c, ls="none", markersize=10, markerfacecolor='white')[0]
    handles = [config(markers[i], colors[i]) for i in range(7)]
    labels = ['TPCC', 'YCSB', 'SmallBank', 'SQLSmith', 'SQLancer', 'Squirrel', 'Orca']
    legend = plt.legend(handles, labels, ncol=7)
    fig = legend.figure
    ax = plt.gca()
    ax.spines['left'].set_color('none')
    ax.spines['top'].set_color('none')
    fig.canvas.draw()
    plt.rcParams['savefig.dpi'] = 800  # 图片像素
    plt.rcParams['figure.dpi'] = 800  # 分辨率
    bbox = legend.get_window_extent()
    bbox = bbox.from_extents(*(bbox.extents + np.array(expand)))
    bbox = bbox.transformed(fig.dpi_scale_trans.inverted())
    fig.savefig('legend-result.pdf', dpi="figure", bbox_inches=bbox)
    plt.show()


if __name__ == '__main__':
    LineTotal()
    BranchTotal()
    FuncTotal()
    export_legend()

#!/usr/bin/python3
# -*- coding: UTF-8 -*-

# 目前计划问题  ： reroute 计划可行  但是 reroute的过程中 失败  需要调整checkifconfilict 函数
# reason: [YES(shard has no previous failures)][YES(primary shard for this replica is already active)][YES(explicitly ignoring any disabling of allocation due to manual allocation commands via the reroute API)][YES(can allocate replica shard to a node with version [7.6.2] since this is equal-or-newer than the primary version [7.6.2])][YES(the shard is not being snapshotted)][YES(ignored as shard is not being recovered from a snapshot)][YES(node passes include/exclude/require filters)][NO(the shard cannot be allocated to the same node on which a copy of the shard already exists [[indextest14][8], node[8MeQmCl_R-CQJD6RlbFmtg], relocating [xyWQRA0ORtC1tjjJe1WRpg], [P], s[RELOCATING], a[id=CjhtOKY6TTyoY5idXDUOAw, rId=An206dPtTsS60Oo-CyQ_pQ], expected_shard_size[24726]])][YES(enough disk for shard on node, free: [22.6gb], shard size: [24.1kb], free after allocating shard: [22.6gb])][THROTTLE(reached the limit of outgoing shard recoveries [2] on the node [8MeQmCl_R-CQJD6RlbFmtg] which holds the primary, cluster setting [cluster.routing.allocation.node_concurrent_outgoing_recoveries=2] (can also be set via [cluster.routing.allocation.node_concurrent_recoveries]))][YES(total shard limits are disabled: [index: -1, cluster: -1] <= 0)][YES(allocation awareness is not enabled, set cluster setting [cluster.routing.allocation.awareness.attributes] to enable it)]
#  [dorr-2] failed to perform [cluster_reroute (api)]java.lang.IllegalArgumentException: [move_allocation] can't move 1, failed to find it on node {dorr-1}{xyWQRA0ORtC1tjjJe1WRpg}{z9SGpS4XSGiHy70zb11C2g}{hadoop102}{192.168.58.12:9301}{dilm}{ml.machine_memory=8189157376, ml.max_open_jobs=20, xpack.installed=true}

from elasticsearch import Elasticsearch
import os
import sys
import logging


logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
# 计算存储空间的函数
url = "hadoop102"
percent = 0.0001
seeIfBalance = False
seeExchangePlan = False
seeIfExecute = False
# 默认第一个参数是当前的路径
es = Elasticsearch([url],
                   sniff_on_start=True,  # 连接前测试
                   sniff_on_connection_fail=True,  # 节点无响应时刷新节点
                   sniff_timeout=60)  # 设置超时时间)
if (es.ping()):
    logger.info("successfully connected es cluster")
logger.info(str(len(sys.argv)) + " : " + sys.argv[0] + " : " + sys.argv[1])
if(sys.argv.__contains__("-c")):
    seeIfBalance = True
    logger.info("正在检查 ---- 请稍后")

if(sys.argv.__contains__("-s")):
    seeExchangePlan = True
    logger.info("正在计算执行计划 ， 请稍后")
if(sys.argv.__contains__("-g")):
    seeIfExecute = True
    logger.info("正在 准备执行平衡任务 ，请稍后")

if(sys.argv.__contains__("-u")):
    for i in range(len(sys.argv)):
        try:
            if(sys.argv[i] == "-u"):
                url = sys.argv[i+1]
                logger.info("设置连接 url 为 " + sys.argv[i + 1])

        except IndexError:
            logger.fatal("-u 参数输入有误  将使用默认值"  + url)

if(sys.argv.__contains__("-p")):
    for i in range(len(sys.argv)):
        try:
            if(sys.argv[i] == "-p"):
                percent = float(sys.argv[i+1])
                logger.info("设置容忍度为 " + str(percent * 100) + "%")

        except IndexError:
            logger.fatal("-p 参数输入有误  将使用默认值" + str(percent * 100) + "%")
else:
    logger.info("使用默认容忍度 " + str(percent * 100) + "%")

def convert(store_size: str):
    if (store_size == None):
        return 0.0
    dorr_store = 0.0
    if "k" in store_size:
        dorr_store += float(store_size[:-2])
    elif "m" in store_size:
        dorr_store += float(store_size[:-2]) * 1024
    elif "g" in store_size:
        dorr_store += float(store_size[:-2]) * 1024 * 1024
    elif "t" in store_size:
        dorr_store += float(store_size[:-2]) * 1024 * 1024 * 1024
    elif "p" in store_size:
        dorr_store += float(store_size[:-2]) * 1024 * 1024 * 1024 * 1024
    elif "p" in store_size:
        dorr_store += float(store_size[:-2]) * 1024 * 1024 * 1024 * 1024 * 1024
    else:
        dorr_store += float(store_size[:-2]) / 1024
    return dorr_store



# 对节点的封装
class Node_Shade:
    # {'index': '.kibana_1', 'node': 'dorr-1', 'state': 'STARTED', 'docs': '21', 'shard': '0', 'prirep': 'p','ip': '192.168.58.12', 'store': '44.2kb'}
    # 容量计算器
    def __init__(self, node: str):
        # index|shade|p/r|value
        self.storeValue = "0.0kb"
        self.node = node
        self.index_shade_p_list = []
        self.index_shade_r_list = []
        self.index_shade_list = []

    # 设置storeValue

    def setStoreValue(self, storeValue):
        self.storeValue = storeValue

    def getShardNum(self):
        return len(self.index_shade_p_list) + len(self.index_shade_r_list)

    @staticmethod
    def cmputeoStoreSize(storeValue: str, index_shade, f):
        # storeValue  22.4kb
        # index
        # (index,shade,p/r,value)
        storeVal = float(storeValue[:-2])

        shade_value = index_shade[3]
        storeVal = f(storeVal, convert(shade_value))
        return str(storeVal) + "kb"

    def addShade(self, index_shade):
        if (not self.index_shade_list.__contains__(index_shade)):
            if (index_shade[2] == "p"):
                self.index_shade_p_list.append(index_shade)
            if (index_shade[2] == "r"):
                self.index_shade_r_list.append(index_shade)
            self.storeValue = self.cmputeoStoreSize(self.storeValue, index_shade, f=lambda a, b: a + b)
            self.index_shade_list.append(index_shade)

    def removeShade(self, index_shade):
        if (self.index_shade_list.__contains__(index_shade)):
            if (index_shade[2] == "p"):
                self.index_shade_p_list.remove(index_shade)
            if (index_shade[2] == "r"):
                self.index_shade_r_list.remove(index_shade)
            self.storeValue = self.cmputeoStoreSize(self.storeValue, index_shade, f=lambda a, b: a - b)
            self.index_shade_list.remove(index_shade)

    def getIndexPShardNumAndVal(self, index: str):
        count = 0
        store_val = "0.0kb"
        for i in self.index_shade_p_list:
            if (i[0] == index):
                count += 1
                store_val = str(convert(store_val) + convert(i[3])) + "kb"
        return count, store_val

    def getIndexRShadeNumAndVal(self, index: str):
        count = 0
        store_val = "0.0kb"
        for i in self.index_shade_r_list:
            if (i[0] == index):
                count += 1
                store_val = str(convert(store_val) + convert(i[3])) + "kb"
        return count, store_val

    def checkIfConflict(self, indexShade):
        # 由于先移动的是主分区的分片 ， 不可将主分片和副本放在一台机器上
        for i in self.index_shade_list:
            if (i[0] == indexShade[0] and i[1] == indexShade[1]):
                return True
        return False


def computeStoreSize(shards, node, nod: Node_Shade):
    dorr_store = float(nod.storeValue[:-2])
    for i in shards:
        # {'index': '.kibana_1', 'node': 'dorr-1', 'state': 'STARTED', 'docs': '21', 'shard': '0', 'prirep': 'p', 'ip': '192.168.58.12', 'store': '44.2kb'}
        if (i['node'] == node):
            store_size = str(i['store']).lower()
            if (i['prirep'] == "p"):
                nod.addShade((i['index'], i['shard'], "p", i['store']))
            if (i['prirep'] == "r"):
                nod.addShade((i['index'], i['shard'], "r", i['store']))
            dorr_store += convert(store_size)
            nod.setStoreValue(str(dorr_store) + "kb")
    return nod


# {'prirep': 'p', 'ip': '192.168.58.14', 'node': 'dorr-3', 'shard': '0', 'docs': '1', 'index': 'p5', 'state': 'STARTED', 'store': '4kb'}
def move(url, index, shard, fromnode, tonode):
    # 命令是异步的 过于简单 容易出问题
    logger.info("start to execute the moving process")
    command = "curl -H \"Content-Type: application/json\" -XPOST \"" + url + ":9200/_cluster/reroute\" -d  '{\"commands\" : [{\"move\" : {\"index\" : \"" + index + "\",\"shard\" : " + str(
        shard) + ",\"from_node\" : \"" + fromnode + "\",\"to_node\" : \"" + tonode + "\"}}]}' "
    os.popen(cmd=command)
    logger.info("follow command:" + command)
    logger.info("finshed  the moving process")

def showCap(nodeList, percent):
    logger.info("sumMem" + str(sumMem) + "kb --balancedStore:"+ str(avgMem * (1 - percent))+ "kb~"+str(avgMem)+ "kb~"+ str(avgMem * (1 + percent))+"kb")
    for i in nodeList:
        info = i.node + "节点存储大小为" + i.storeValue
        logger.info(info)


def checkIfShardNumBalance(percent):
    for i in avgIndex:
        size = convert(i[1])
        for m in NodeList:
            # 采用轮询的方法来计算  所以相差不能超过一
            PNum, PVal = m.getIndexPShardNumAndVal(i[0])
            RNum, RVal = m.getIndexRShadeNumAndVal(i[0])
            if (abs(PNum - i[2]) > 1):
                logger.debug(i[0] + m.node +"pri不平衡")
            if (abs(RNum - i[2]) > 1):
                logger.debug(i[0] + m.node + "rpli不平衡")
            # 一般是均衡算法的问题  此处不做讨论
            if (abs(convert(PVal) - size) / size > percent):
                logger.debug(i[0] +m.node +"pri value 不平衡")
            if (abs(convert(RVal) - size) / size > percent):
                logger.debug(i[0] +  m.node + "r value 不平衡")

# 首先获取es es的shard 信息  此处格式只能写json  yaml
# 拿到 node 和index的 集合是两个固定的集合
node_set = set()
index_set = set()
index_list = []
nodes = es.cat.nodes(format='json')
for i in nodes:
    node_set.add(i['name'])
indexes = es.cat.indices(format='json')
for i in indexes:
    index_set.add(i['index'])
    index_list.append((i['index'], i['pri.store.size'], i['pri']))
# class Shade(nodes ):
# shard = hash(routing) % number_of_primary_shards 实际分配过程中是如此 使用hash算法进行的分片策略

# 主分片的数量一定不能改变 ， 如果改变了 ， 则之前所有的修改的都会作废
# 所以主分片的个数会比较大

# 所有的api都会接受一个 routing 的路由参数 ， 通过这个安属我们可以自定义参数到分片的映射

# 计算 每个节点对应的storeSize
# 通过遍历shard查到的结果封装进NodeList 中
# 完成每个节点数据的封装 ， 并放到一个list中
res = es.cat.shards(format='json')
NodeList = []
# 将所有的节点计算成功后移动到nodeList 中
for m in node_set:
    nod = Node_Shade(m)
    computeStoreSize(shards=res, node=m, nod=nod)
    NodeList.append(nod)

nodeSize = len(NodeList)
# 计算index 平均的shard数 和 平均的和每个节点的平均的容量
avgIndex = []
for i in index_list:
    shardNum = int(int(i[2]) / nodeSize) + 1
    avgIndex.append((i[0], (str(convert(i[1]) / nodeSize)) + "kb", shardNum))
# 判断shard 分布不均衡

# 判断磁盘均衡
sumMem = 0.0
for node in NodeList:
    sumMem += convert(node.storeValue)
avgMem = sumMem / nodeSize


def checkIfDiskBalance(percent, nodeList = NodeList):
    # 检查是否平衡 并计算出 不平衡的节点个数
    flag = True
    count = 0
    newSumMem = 0
    for node in nodeList:
        per = (convert(node.storeValue) - avgMem) / avgMem
        newSumMem += convert(node.storeValue)
        if (abs(per) > percent):
            flag = False
            count += 1
            logger.debug(node.node, "disk 不平衡与均值相差  " + str(per * 100) +  "%" + "标准差值为"+ str(percent * 100)+ "%")
    if (flag):
        logger.info("=========================")
        logger.info("disk 已经平衡")
        logger.info("=========================")
    else:
        logger.fatal("disk不平衡 ， 节点数为:" + str(count))
    return flag, count


def getNode(nodeList, num, reverse=True):
    # 从node list中拿到指定的 按照storeValue进行排序之后的list的指定位置的node  默认升序排列
    if (len(nodeList) > 0):
        return sorted(nodeList, key=lambda node: convert(node.storeValue), reverse=reverse)[num]


def getIndexShade(node, num, reverse=False):
    # 拿到该节点上的某个位置的IndexShade  默认是升序排列
    return sorted(node.index_shade_list, reverse=reverse, key=lambda s: convert(s[3]))[num]


def change(lowerList, higerList,  ifExecute=False, f=move):
    # 空值判断在之前一轮中已经进行了  最大的一个节点
    toNode = getNode(lowerList, 0)
    for m in range(len(higerList) - 1, -1, -1):
        fromNode = getNode(higerList, m)
        for i in range(len(fromNode.index_shade_list) - 1):
            # 在High list和Low list之间拿较大的 shade 先进行平衡
            indexShade = getIndexShade(fromNode, i, reverse=True)
            if (not toNode.checkIfConflict(indexShade) and (
                    convert(toNode.storeValue) + convert(indexShade[3]) < convert(fromNode.storeValue))):
                # 判断完成说明可以移动
                logger.info("将从" + fromNode.node + "向"+ toNode.node+ "移动index:"+ indexShade[0]+ "    shade :"+
                          indexShade[1]+ "移动的大小 :"+ indexShade[3]+
                          "移动前容量大小为"+ toNode.storeValue +"移动后为"+
                          str(convert(toNode.storeValue) + convert(indexShade[3]))+
                          "kb")
                toNode.addShade(indexShade)
                fromNode.removeShade(indexShade)
                if (ifExecute):
                    f(url, indexShade[0], indexShade[1], fromNode.node, toNode.node)
                return True
    return False


def changeLowerWithAvg(lowList, avgList, percent, ifExecute=False, f=move):
    toNode = getNode(lowList, 0)
    for m in range(len(avgList) - 1, -1, -1):
        fromNode = getNode(avgList, m)
        # 优先拿小的进行替换
        for i in range(len(fromNode.index_shade_list) - 1):
            indexShade = getIndexShade(fromNode, i)
            # 判断是否可以传输的条件可以再更改一下
            indexShadeValue = convert(indexShade[3])
            fromNodeValue = convert(fromNode.storeValue)
            lowLimit = avgMem * (1 - percent)
            # showCap(NodeList,percent)
            if (not toNode.checkIfConflict(indexShade) and (fromNodeValue - indexShadeValue) > lowLimit):
                # 判断完成说明可以移动
                logger.info("将从" + fromNode.node+ "向"+ toNode.node+ "移动index:"+ indexShade[0]+ "    shade :"+
                          indexShade[1]+ "移动的大小 :"+ indexShade[3]+
                          "移动前容量大小为"+ toNode.storeValue+ "移动后为"+
                          str(convert(toNode.storeValue) + convert(indexShade[3]))+
                          "kb")
                toNode.addShade(indexShade)
                fromNode.removeShade(indexShade)
                # 此处放上移动的命令
                # 只要移动了一次
                if (ifExecute):
                    f(url, indexShade[0], indexShade[1], fromNode.node, toNode.node)
                # 移动了就应该为True
                return True
    return False


def changeHighListWithAvg(avgList, highNode, percent, ifExecute=False, f=move):
    toNode = getNode(avgList, 0)
    for m in range(len(highNode) - 1, -1, -1):
        fromNode = getNode(highNode, m)
        # 优先拿小的进行替换
        for i in range(len(fromNode.index_shade_list) - 1):
            indexShade = getIndexShade(fromNode, i)
            # 判断是否可以传输的条件可以再更改一下
            toNodeValue = convert(toNode.storeValue)
            indexShadeValue = convert(indexShade[3])
            upperLimit = avgMem * (1 + percent)
            if (not toNode.checkIfConflict(indexShade) and (toNodeValue + indexShadeValue) < upperLimit):
                # 判断完成说明可以移动
                logger.info("将从" + fromNode.node+ "向"+ toNode.node+ "移动index:"+ indexShade[0]+ "    shade :"+
                          indexShade[1]+ "移动的大小 :"+ indexShade[3]+
                          "移动前容量大小为"+ toNode.storeValue+ "移动后为"+
                          str(convert(toNode.storeValue) + convert(indexShade[3]))+
                          "kb")
                toNode.addShade(indexShade)
                fromNode.removeShade(indexShade)
                # 此处放上移动的命令
                # 只要移动了一次
                if (ifExecute):
                    f(url, indexShade[0], indexShade[1], fromNode.node, toNode.node)
                return True
    return False


def balanceDisk(percent, ifExecute=False):
    count = 0
    res = True
    lRes = True
    hRes = True
    # 模仿HDFS的均衡策略 将数据的容量分为3等级 avg +- 30%    更小  更大
    # 最大node的把其最大的shade 给 最小的node并进行判断 ，计算完小的进入 avg区则表示均衡完成  ，注意检查依赖性 ，否则 可能会出现问题 ！！！ 否则把第二大的index放入 ， 让后重新划分 重新均衡
    # 此处会有一个bug ，如果有两个片 的数目大于节点的存储的平均值
    while not checkIfDiskBalance(percent)[0]:
        avgList = []
        lUnBalanceList = []
        hUnBalanceList = []
        for node in NodeList:
            store_value = convert(node.storeValue)
            if (abs(store_value - avgMem) / avgMem < percent):
                avgList.append(node)
            if ((avgMem - store_value) / avgMem > percent):
                lUnBalanceList.append(node)

            if ((store_value - avgMem) / avgMem > percent):
                hUnBalanceList.append(node)
        # 通过length 来进行判断
        if (res and not len(lUnBalanceList) == 0 and not len(hUnBalanceList) == 0):
            logger.info("开启higer list 和 lower list 间的balance操作")
            # showCap(NodeList,percent)
            res = change(lowerList=lUnBalanceList, higerList=hUnBalanceList, ifExecute=ifExecute)
            # showCap(NodeList,percent)
        elif (not res and not len(lUnBalanceList) == 0 and not len(avgList) == 0 and not len(hUnBalanceList) == 0):
            # 开始是lowlist 和 avglist 之间的一次性平衡
            # 按照两个list的length 进行判断和哪一边做平衡
            if (len(lUnBalanceList) >= len(hUnBalanceList)):
                logger.info("开始lowList 和 avgList 间的平衡操作----")
                res = changeLowerWithAvg(lowList=lUnBalanceList, avgList=avgList, percent=percent, ifExecute=ifExecute)
                if (not res):
                    logger.info("开始在highlist 和 avglist之间做均衡>>>")
                    res = changeHighListWithAvg(avgList=avgList, highNode=hUnBalanceList, percent=percent,
                                                ifExecute=ifExecute)
                    if (not res):
                        logger.fatal("移动完成 , 无法完全平衡")
                        return
            else:
                # 和 avglist 做均衡策略  结束点需要保证
                logger.info("开始在highlist 和 avglist之间做均衡----")
                res = changeHighListWithAvg(avgList=avgList, highNode=hUnBalanceList, percent=percent,
                                            ifExecute=ifExecute)
                if (not res):
                    logger.info("开始lowList 和 avgList 间的平衡操作>>>")
                    res = changeLowerWithAvg(lowList=lUnBalanceList, avgList=avgList, percent=percent,
                                             ifExecute=ifExecute)
                    if (not res):
                        logger.fatal("移动完成 , 无法完全平衡")
                        return

        elif (hRes and not len(avgList) == 0 and len(lUnBalanceList) == 0 and not len(hUnBalanceList) == 0):
            logger.info("开始avg list 和 h list之间的平衡")
            hRes = changeHighListWithAvg(avgList=avgList, highNode=hUnBalanceList, percent=percent, ifExecute=ifExecute)

        elif (lRes and not len(avgList) == 0 and not len(lUnBalanceList) == 0 and len(hUnBalanceList) == 0):
            logger.info("开始在avg list 和 l list 之间的平衡")
            lRes = changeLowerWithAvg(lowList=lUnBalanceList, avgList=avgList, percent=percent, ifExecute=ifExecute)
        else:
            count += 1
            if (count > 2):
                return


# checkIfShardNumBalance(percent,True)
# dorr-2 节点存储大小为 558.5999999999999kb   566.8kb
# dorr-3 节点存储大小为 556.4763671875kb 541.5763671875001kb
# dorr-1 节点存储大小为 545.0763671875001kb 551.7763671875kb
def showShardDistribute(NodeList):
    for node in NodeList:
        logger.info(node.node+ "shard数"+ str(len(node.index_shade_list))+ "主分片数"+ str(len(node.index_shade_p_list))+ "副本数"+ str(len(node.index_shade_r_list)))
        pNodelistInfo = ""
        rNodeListInfo = ""
        for i in node.index_shade_p_list:
            pNodelistInfo += str(i) + "   "
            logger.debug(pNodelistInfo)
        for i in node.index_shade_r_list:
            rNodeListInfo += str(i) + "   "
            logger.debug(rNodeListInfo)

def main():
    # 查看移动计划
    if (seeIfBalance):
        showCap(NodeList, percent)
        checkIfDiskBalance(percent)
    if (seeExchangePlan):
        showCap(NodeList, percent)
        showShardDistribute(NodeList)
        logger.info("-------------------------移动任务计划开始------------------------------")
        balanceDisk(percent, ifExecute=False)
        logger.info("-------------------------移动任务计划结束------------------------------")
        showCap(NodeList, percent)
        checkIfDiskBalance(percent)
        logger.info("------------------------移动结束后shard分布-----------------------------")
        showShardDistribute(NodeList)

    if (seeIfExecute):
        showCap(NodeList, percent)
        logger.info("-------------------------开始按照计划执行--------------------------------")
        balanceDisk(percent, ifExecute=seeIfExecute)
        logger.info("-----------------------------执行完毕----------------------------------")
        res = es.cat.shards(format='json')
        NodeList2 = []
        logger.info("-------------------------开始执行完的检测----------------------------------")
        for m in node_set:
            nod = Node_Shade(m)
            computeStoreSize(shards=res, node=m, nod=nod)
            NodeList2.append(nod)
        checkIfDiskBalance(percent=percent,nodeList=NodeList)
        showCap(percent=percent, nodeList=NodeList)
        #保险起见  重新查询以便看结果是否一致
        checkIfDiskBalance(percent = percent,nodeList = NodeList2)
        for node2 in  NodeList2:
            if node2 not in NodeList:
                logger.fatal("移动失败 ， 中间过程恐有变动")
                logger.fatal("------------------------预计移动结果--------------------------")
                showCap(NodeList, percent)
                logger.fatal("------------------------实际移动结果--------------------------")
                showCap(NodeList2,percent)
                return
        showShardDistribute(NodeList2)



if __name__ == '__main__':
    main()

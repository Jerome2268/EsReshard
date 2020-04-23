#!/usr/bin/python3
# -*- coding: UTF-8 -*-
# you should have the elasticsearch package installed  the default is dorr-1
# usage: python3 EsReshard.py |-c|-s|-g|-r|   |-u + url|   |-p +percent| [-w wait-factor] [-n num]
# this script will touch a execute.log to store what`s happening
# you can change the requirement in method checkifconfilt once needed
# method move if the sleepTime is to short ot`s easy to make mistake
# after cluster.routing.allocation.enable none you cannot directly use index-create
# after the execution we will set  cluster.routing.allocation.enable  all to start the index-create
# cluster.routing.allocation.total_shards_per_node setting to  the avgshard + number
from elasticsearch import Elasticsearch
import os
import sys
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

url = "dorr-1"
percent = 0.0001
seeIfBalance = False
seeExchangePlan = False
seeIfExecute = False
ifReverse = True
ifAvgReverse = False
waiting = 20
number = 0
network_transmission = "0kb"

# default sys.arg[0] is the  current path

if (len(sys.argv) == 1):
    logger.info(
        "usage: python3 EsReshard.py |-c|-s|-g|  [-r]  [-ar] [-n + num] [-u + url]   [-p +percent] [-w wating-factor]  ")
    logger.info(
        "usage:-c:showDiskStatus  -s : showBalancePlan  -g:follow the balance plan and execute   "
        " -r : Balance Strategy(big shard first or not ) "
        " -ar "
        " -n  set the avg shades num,default value is no limit"
        " -p tolerance"
        " -w wating-factor usually bigger than 5")
    logger.info("please type enought params to excute this script")
else:
    es = Elasticsearch([url],
                       sniff_on_start=True,  # test before the connect
                       sniff_on_connection_fail=True,  # flush node when failed
                       sniff_timeout=60)  # set the timeout)
    if (es.ping()):
        logger.info("Successfully connected es cluster")
    logger.info(str(len(sys.argv)) + " : " + sys.argv[0] + " : " + sys.argv[1])
    # parse all the input params
    if (sys.argv.__contains__("-c")):
        seeIfBalance = True
        logger.info("Checking if balance ---- Please wait a moment.")

    if (sys.argv.__contains__("-s")):
        seeExchangePlan = True
        logger.info("Calculating the balance planning ----  Please wait a moment.")
    if (sys.argv.__contains__("-g")):
        seeIfExecute = True
        logger.info("Preparing executing the balance task ------------------------")
    if (sys.argv.__contains__("-ar")):
        ifAvgReverse = True
        logger.info("Preparing executing the balance task ------------------------")

    if (sys.argv.__contains__("-r")):
        ifReverse = False
    if (sys.argv.__contains__("-n")):
        for i in range(len(sys.argv)):
            try:
                if (sys.argv[i] == "-n"):
                    number = int(sys.argv[i + 1])
                    logger.info("set the different shard-node num : " + sys.argv[i + 1])

            except IndexError:
                logger.fatal("Wrong parameter input with -n , will use the default value:" + str(number))

            except TypeError:
                logger.fatal("Wrong parameter input with -n , will use the default value:" + str(number))
    if (sys.argv.__contains__("-u")):
        for i in range(len(sys.argv)):
            try:
                if (sys.argv[i] == "-u"):
                    url = sys.argv[i + 1]
                    logger.info("Set the connection url : " + sys.argv[i + 1])

            except IndexError:
                logger.fatal("Wrong parameter input with -u , will use the default value:" + url)

            except TypeError:
                logger.fatal("Wrong parameter input with -u , will use the default value:" + url)
    if (sys.argv.__contains__("-w")):
        for i in range(len(sys.argv)):
            try:
                if (sys.argv[i] == "-w"):
                    waiting = float(sys.argv[i + 1])
                    logger.info("Set the waiting time : " + sys.argv[i + 1])

            except IndexError:
                logger.fatal("Wrong parameter input with -w , will use the default value:" + waiting)

            except TypeError:
                logger.fatal("Wrong parameter input with -w , will use the default value:" + waiting)

    if (sys.argv.__contains__("-p")):
        for i in range(len(sys.argv)):
            try:
                if (sys.argv[i] == "-p"):
                    percent = float(sys.argv[i + 1])
                    logger.info("Set the  balance tolerance : " + str(percent * 100) + "%")

            except IndexError:
                logger.fatal("Wrong parameter input with -p , will use the default value:" + str(percent * 100) + "%")
            except TypeError:
                logger.fatal("Wrong parameter input with -p , will use the default value:" + str(percent * 100) + "%")
    else:
        logger.info("Will use the default value:" + str(percent * 100) + "%")


    def convert(store_size: str):
        # compute the size
        if (store_size == "none"):
            return 0.0
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
        elif "e" in store_size:
            dorr_store += float(store_size[:-2]) * 1024 * 1024 * 1024 * 1024 * 1024
        elif "z" in store_size:
            dorr_store += float(store_size[:-2]) * 1024 * 1024 * 1024 * 1024 * 1024 * 1024
        elif "y" in store_size:
            dorr_store += float(store_size[:-2]) * 1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024
        else:
            # byte
            dorr_store += float(store_size[:-1]) / 1024
        return dorr_store


    class Cluster:
        def __init__(self, name):
            self.clustername = name
            self.count = 0
            self.network_transmission = "0kb"

        def move(self, storeValue):
            self.count = self.count + 1
            self.network_transmission = str(convert(self.network_transmission) + convert(storeValue)) + "kb"


    res = es.cat.shards(format='json')
    # get all the shards format :json  yaml
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
            self.avgCount = int(len(res) / len(node_set))

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
            # shard pri and shard repli can not locate on the same node
            if (not number == 0):
                if ((len(self.index_shade_list) > self.avgCount + number - 1) or len(
                        self.index_shade_list) < self.avgCount - number + 1):
                    return True
            for i in self.index_shade_list:
                if (i[0] == indexShade[0] and i[1] == indexShade[1]):
                    return True
            return False


    # check after the moving
    def esCheck(es, node):
        nod = Node_Shade(node)
        shards = es.cat.shards(format='json')
        for i in shards:
            if (i['node'] == node):
                store_size = str(i['store']).lower()
                if (i['prirep'] == "p"):
                    nod.addShade((i['index'], i['shard'], "p", i['store']))
                if (i['prirep'] == "r"):
                    nod.addShade((i['index'], i['shard'], "r", i['store']))

        return nod.storeValue


    def computeStoreSize(shards, node, nod: Node_Shade):
        for i in shards:
            # {'index': '.kibana_1', 'node': 'dorr-1', 'state': 'STARTED', 'docs': '21', 'shard': '0', 'prirep': 'p', 'ip': '192.168.58.12', 'store': '44.2kb'}
            if (i['node'] == node):
                store_size = str(i['store']).lower()
                if (i['prirep'] == "p"):
                    nod.addShade((i['index'], i['shard'], "p", i['store']))
                if (i['prirep'] == "r"):
                    nod.addShade((i['index'], i['shard'], "r", i['store']))
        return nod


    # {'prirep': 'p', 'ip': '192.168.58.14', 'node': 'dorr-3', 'shard': '0', 'docs': '1', 'index': 'p5', 'state': 'STARTED', 'store': '4kb'}
    def move(url, index, shard, fromnode, tonode, storeValue):
        # sleep for a time
        logger.info("start to execute the moving process")
        command = "curl -H \"Content-Type: application/json\" -XPOST \"" + url + ":9200/_cluster/reroute\" -d  '{\"commands\" : [{\"move\" : {\"index\" : \"" + index + "\",\"shard\" : " + str(
            shard) + ",\"from_node\" : \"" + fromnode.node + "\",\"to_node\" : \"" + tonode.node + "\"}}]}' "
        flag = 1
        while not flag == 0:
            # post is idmpotency so we can use while
            flag = os.system(command=command)
            file = open("execute.log", "a+")
            t = float(convert(storeValue) / waiting)
            time.sleep(t)
            if (flag == 0):
                toNodeCheckValue = esCheck(es=es, node=tonode.node)
                fromNodeCheckValue = esCheck(es=es, node=fromnode.node)
                file.write(
                    "move from" + fromnode.node + "  size:" + fromnode.storeValue + "to" + tonode.node + " :size" + tonode.storeValue + "  index:" + index + "  shard :" + shard + "successfully!" + " shard size: " + storeValue + "   node :" + tonode.node + "compute size :   " + str(
                        convert(tonode.storeValue) + convert(
                            storeValue)) + "kb  actual toNode  szie:     " + toNodeCheckValue + "Variance:" + str(
                        convert(toNodeCheckValue) - convert(
                            tonode.storeValue)) + "kb  actual fromNode size：  " + fromNodeCheckValue + "Variance" + str(
                        convert(fromNodeCheckValue) - convert(fromnode.storeValue)) + "kb\n")
            else:
                file.write(
                    "----------------------------------------------------------------------------------------------------------\n")
                file.write(command)
                file.write(
                    "from" + fromnode.node + "to" + tonode.node + ":index:" + index + "  shard :" + shard + "failed!  " + "shard size : " + storeValue + "\n")
                file.write(
                    "----------------------------------------------------------------------------------------------------------\n")
        logger.info("follow command:" + command)

        logger.info("finshed  the moving process")


    def showCap(nodeList, percent):
        logger.info("sumMem" + str(sumMem) + "kb   --balancedStore: " + str(avgMem * (1.0 - percent)) + "kb~" + str(
            avgMem) + "kb~" + str(avgMem * (1.0 + percent)) + "kb")
        for i in sorted(nodeList, key=lambda s: s.node):
            info = i.node + "  size:  " + i.storeValue
            logger.info(info)


    def checkIfShardNumBalance(percent):
        for i in avgIndex:
            size = convert(i[1])
            for m in NodeList:
                PNum, PVal = m.getIndexPShardNumAndVal(i[0])
                RNum, RVal = m.getIndexRShadeNumAndVal(i[0])
                if (abs(PNum - i[2]) > 1):
                    logger.debug(i[0] + m.node + "pri  unbalanced")
                if (abs(RNum - i[2]) > 1):
                    logger.debug(i[0] + m.node + "rpli unbalanced")
                # 一般是均衡算法的问题  此处不做讨论
                if (abs(convert(PVal) - size) / size > percent):
                    logger.debug(i[0] + m.node + "pri value unbalanced")
                if (abs(convert(RVal) - size) / size > percent):
                    logger.debug(i[0] + m.node + "r value unbalanced")


    # put all the shards info to a collection

    NodeList = []
    cluster = Cluster(url)
    for m in node_set:
        nod = Node_Shade(m)
        computeStoreSize(shards=res, node=m, nod=nod)
        NodeList.append(nod)

    nodeSize = len(NodeList)
    # compute the avg shard per index
    avgIndex = []
    for i in index_list:
        shardNum = int(int(i[2]) / nodeSize) + 1
        avgIndex.append((i[0], (str(convert(i[1]) / nodeSize)) + "kb", shardNum))

    sumMem = 0.0
    for node in NodeList:
        sumMem += convert(node.storeValue)
    avgMem = sumMem / nodeSize


    def checkIfDiskBalance(percent, nodeList=NodeList, ifseelog=False):
        # check if the disk is balanced
        flag = True
        count = 0
        newSumMem = 0
        for node in nodeList:
            per = (convert(node.storeValue) - avgMem) / avgMem
            newSumMem += convert(node.storeValue)
            if (abs(per) > percent):
                flag = False
                count += 1
                logger.debug(node.node,
                             "disk unbalance D-value with avgValue  " + str(per * 100) + "%" + "standard D-value" + str(
                                 percent * 100) + "%")
        if (flag):
            logger.info("=========================")
            logger.info("disk balanced")
            logger.info("=========================")
        else:
            if (ifseelog):
                logger.fatal("disk unbalanced ， datanode count:" + str(count))
        return flag, count


    def getNode(nodeList, num, reverse=False):
        # node list.reverse default asc
        if (len(nodeList) > 0):
            return sorted(nodeList, key=lambda node: convert(node.storeValue), reverse=reverse)[num]


    def getIndexShade(node, num, reverse=ifAvgReverse):
        # default asc
        return sorted(node.index_shade_list, reverse=reverse, key=lambda s: convert(s[3]))[num]


    def change(lowerList, higerList, ifExecute=False, ifReverse=ifReverse, f=move):
        # check if is null is defined
        toNode = getNode(lowerList, 0)
        for m in range(len(higerList) - 1, -1, -1):
            fromNode = getNode(higerList, m)
            for i in range(len(fromNode.index_shade_list) - 1):
                # moving from the lowlist and highlist
                indexShade = getIndexShade(fromNode, i, reverse=ifReverse)
                if (not toNode.checkIfConflict(indexShade) and convert(indexShade[3]) > 1 and (
                        # if the moving volume is too little it is meaningless
                        convert(toNode.storeValue) + convert(indexShade[3]) < (convert(fromNode.storeValue)))):
                    # log
                    logger.info("from " + fromNode.node + " to " + toNode.node + " moving index:" + indexShade[
                        0] + "    shade :" +
                                indexShade[1] + " moving size :" + indexShade[3] +
                                "  volume :" + toNode.storeValue + " volume after the moving :  " +
                                str(convert(toNode.storeValue) + convert(indexShade[3])) +
                                "kb")
                    # move
                    if (ifExecute):
                        f(url=url, index=indexShade[0], shard=indexShade[1], fromnode=fromNode, tonode=toNode,
                          storeValue=indexShade[3])
                    cluster.move(indexShade[3])
                    fromNode.removeShade(indexShade)
                    toNode.addShade(indexShade)
                    return True
        return False


    def changeLowerWithAvg(lowList, avgList, percent, ifExecute=False, f=move):
        # the node that has the least storeVal
        toNode = getNode(lowList, 0)
        for m in range(len(avgList) - 1, -1, -1):
            fromNode = getNode(avgList, m)
            # advance little
            for i in range(len(fromNode.index_shade_list) - 1):
                indexShade = getIndexShade(fromNode, i)
                indexShadeValue = convert(indexShade[3])
                fromNodeValue = convert(fromNode.storeValue)
                lowLimit = avgMem * (1 - percent)
                # showCap(NodeList,percent)
                if (not toNode.checkIfConflict(indexShade) and (fromNodeValue - indexShadeValue) > lowLimit):
                    logger.info("from " + fromNode.node + " to " + toNode.node + " moving index:" + indexShade[
                        0] + "    shade :" +
                                indexShade[1] + " moving size :" + indexShade[3] +
                                "  volume :" + toNode.storeValue + " volume after the moving :  " +
                                str(convert(toNode.storeValue) + convert(indexShade[3])) +
                                "kb")

                    if (ifExecute):
                        f(url=url, index=indexShade[0], shard=indexShade[1], fromnode=fromNode, tonode=toNode,
                          storeValue=indexShade[3])
                    cluster.move(indexShade[3])
                    fromNode.removeShade(indexShade)
                    toNode.addShade(indexShade)
                    return True
        return False


    def changeHighListWithAvg(avgList, highNode, percent, ifExecute=False, f=move):
        # the node that has the least storeVal
        toNode = getNode(avgList, 0)
        for m in range(len(highNode) - 1, -1, -1):
            fromNode = getNode(highNode, m)
            # advancing on the little
            for i in range(len(fromNode.index_shade_list) - 1):
                indexShade = getIndexShade(fromNode, i)

                toNodeValue = convert(toNode.storeValue)
                indexShadeValue = convert(indexShade[3])
                upperLimit = avgMem * (1 + percent)
                if (not toNode.checkIfConflict(indexShade) and (toNodeValue + indexShadeValue) < upperLimit):
                    # showing it is convient to move
                    logger.info("from " + fromNode.node + " to " + toNode.node + " moving index:" + indexShade[
                        0] + "    shade :" +
                                indexShade[1] + " moving size :" + indexShade[3] +
                                "  volume :" + toNode.storeValue + " volume after the moving :  " +
                                str(convert(toNode.storeValue) + convert(indexShade[3])) +
                                "kb")

                    if (ifExecute):
                        f(url=url, index=indexShade[0], shard=indexShade[1], fromnode=fromNode, tonode=toNode,
                          storeValue=indexShade[3])
                    # after the moving
                    cluster.move(indexShade[3])
                    fromNode.removeShade(indexShade)
                    toNode.addShade(indexShade)
                    return True
        return False


    def balanceDisk(percent, ifExecute=False):
        count = 0
        res = True
        lRes = True
        hRes = True
        # divid the datanode to 3 list :
        #   avgList : the storeValue is between  avgMem( 1 - percent) and avgMem( 1 + percent)
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
            # predit by the length
            if (res and not len(lUnBalanceList) == 0 and not len(hUnBalanceList) == 0):
                logger.debug("start balance between higher list and lower list  --------------- ")
                # showCap(NodeList,percent)
                res = change(lowerList=lUnBalanceList, higerList=hUnBalanceList, ifExecute=ifExecute)
                # showCap(NodeList,percent)
            elif (not res and not len(lUnBalanceList) == 0 and not len(avgList) == 0 and not len(hUnBalanceList) == 0):
                if (len(lUnBalanceList) >= len(hUnBalanceList)):
                    logger.debug("start balance between lowList and  avgList ########--------------- ")
                    result1 = changeLowerWithAvg(lowList=lUnBalanceList, avgList=avgList, percent=percent,
                                                 ifExecute=ifExecute)
                    # if once succeed u show give the avglist a chance
                    if result1:
                        res = True
                    else:
                        logger.debug("start balance highlist between avglist ########----------------")
                        result2 = changeHighListWithAvg(avgList=avgList, highNode=hUnBalanceList, percent=percent,
                                                        ifExecute=ifExecute)
                        if result2:
                            res = True
                        else:
                            logger.info("moving completed  , can not be fully balanced")
                            return

                else:
                    # balance with the higher list and avglist
                    logger.debug("start balance highlist between avglist @@@@@@@@@----------------")
                    result1 = changeHighListWithAvg(avgList=avgList, highNode=hUnBalanceList, percent=percent,
                                                    ifExecute=ifExecute)
                    if result1:
                        res = True
                    else:
                        logger.debug("start balance between lowList and  avgList @@@@@@@@@@--------------- ")
                        result2 = changeLowerWithAvg(lowList=lUnBalanceList, avgList=avgList, percent=percent,
                                                     ifExecute=ifExecute)
                        if result2:
                            res = True
                        else:
                            logger.info("moving completed  , can not be fully balanced")
                            return

            elif (hRes and not len(avgList) == 0 and len(lUnBalanceList) == 0 and not len(hUnBalanceList) == 0):
                logger.debug("start balance highlist between avglist ****************----------------")
                hRes = changeHighListWithAvg(avgList=avgList, highNode=hUnBalanceList, percent=percent,
                                             ifExecute=ifExecute)

            elif (lRes and not len(avgList) == 0 and not len(lUnBalanceList) == 0 and len(hUnBalanceList) == 0):
                logger.debug("start balance between lowList and  avgList *****************--------------- ")
                lRes = changeLowerWithAvg(lowList=lUnBalanceList, avgList=avgList, percent=percent, ifExecute=ifExecute)
            else:
                count += 1
                if (count > 2):
                    return


    # checkIfShardNumBalance(percent,True)
    # dorr-2 shards count:558.5999999999999kb   566.8kb
    # dorr-3 shards count: 556.4763671875kb 541.5763671875001kb
    # dorr-1 shards count: 545.0763671875001kb 551.7763671875kb
    def showShardDistribute(NodeList):
        for node in sorted(NodeList, key=lambda s: s.node):
            logger.info(node.node + "shards count:" + str(len(node.index_shade_list)) + " pri :" + str(
                len(node.index_shade_p_list)) + " replicas : " + str(len(node.index_shade_r_list)))
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
            checkIfDiskBalance(percent, ifseelog=True)
            showShardDistribute(NodeList)
        if (seeExchangePlan):
            showCap(NodeList, percent)
            showShardDistribute(NodeList)
            logger.info("-------------------------balance planning started------------------------------")
            balanceDisk(percent, ifExecute=False)
            logger.info("moving count :" + str(
                cluster.count) + " network_transmission : " + cluster.network_transmission + "will take time :" + str(
                convert(cluster.network_transmission) / waiting) + "s")
            logger.info("-------------------------balanced planning completed------------------------------")
            showCap(NodeList, percent)
            checkIfDiskBalance(percent, ifseelog=True)
            logger.info("------------------------shard distribution after the moving-----------------------------")
            showShardDistribute(NodeList)

        if (seeIfExecute):
            # isWantToExecute this scripts , set cluster.routing.allocation.enable transient none
            os.system(
                command="curl -H \"Content-Type: application/json\"  -X PUT http://" + url + ":9200/_cluster/settings?pretty -d '{\"transient\": {\"cluster.routing.allocation.enable\": \"none\"}}'")
            logger.info(
                "=====================set cluster.routing.allocation.enable transient none========================")
            showCap(NodeList, percent)
            logger.info("-------------------------executing the balanced schedule--------------------------------")
            balanceDisk(percent, ifExecute=seeIfExecute)
            os.system(
                command="curl -H \"Content-Type: application/json\"  -X PUT http://" + url + ":9200/_cluster/settings?pretty -d '{\"transient\": {\"cluster.routing.allocation.enable\": \"all\"}}'")
            logger.info("---------------------------finsh the balanced schedule----------------------------------")
            logger.info("-----------------------------checking the execution----------------------------------")
            res = es.cat.shards(format='json')
            NodeList2 = []
            for m in node_set:
                nod = Node_Shade(m)
                computeStoreSize(shards=res, node=m, nod=nod)
                NodeList2.append(nod)
            logger.info("---------------------------calculated volume----------------------------------")
            showCap(percent=percent, nodeList=NodeList)
            # ensurance the counting result
            logger.info("----------------------------actually volume-----------------------------------")
            showCap(percent=percent, nodeList=NodeList2)
            logger.info(
                "-----------------------datenode shard count distribution after the execution-----------------------------")
            showShardDistribute(NodeList2)


    if __name__ == '__main__':
        main()


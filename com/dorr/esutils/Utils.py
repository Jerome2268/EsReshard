def convert(store_size: str):
    # es 拿到的默认大小是none  ，而非 None
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

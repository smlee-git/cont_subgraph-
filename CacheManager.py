import pprint
import time
from operator import itemgetter
import operator
from collections import deque
uHit, pHit, request = 0, 0, 0

class Query():
    def __init__(self, id):
        self.id = id
        # self.nodes = []
        self.size = 0
        self.nodes = deque()

    def insertNode(self, node) :
        self.nodes.append(node)

#node
class Node() :
    def __init__(self, id, v1, v2, edge, parent) :
        self.id = id
        self.v1 = v1
        self.v2 = v2
        self.edge = edge
        self.parent = parent
        # self.children = []
        self.children = deque()
        self.size = 0

    def isMatch(self, v1, v2, edge) :
        if self.v1 == v1 and self.v2 == v2 and self.edge == edge :
            return self
        return None

    def isContainNode(self, v1, v2, edge) :
        for c in self.children :
            if c.v1 == v1 and c.v2 == v2 and c.edge == edge :
                return c
        return None

#root
class Trie() :
    def __init__(self, node) :
        self.node = node

    def searchAndPrint(self, node) :
        print(node.id, node.v1, node.edge, node.v2)
        for c in node.children :
            self.searchAndPrint(c)

queryIndex = {}
edgeIndex = {}
tries = {}
fpTable = {"freq" : {}, "join" : {}}


smax = 0
qmax = 0

nid = 0

with open("query3.txt", "r") as f:
    lines = f.readlines()
    qset = set([])
    qsize = 0
    qprev = 0
    for line in lines :
        ls = line.strip().split(" ")
        queryId = ls[-1]
        if queryId not in queryIndex :
            queryIndex[queryId] = Query(queryId)
            if qprev != 0 :
                queryIndex[qprev].size = qsize*len(qset)
                qset = set([])
                qsize = 0

        numEdge = (len(ls)-2)//2
        qsize += (numEdge*2) + 1
        qprev = queryId
        #loop edge
        prev = None
        for i in range(numEdge) :
            v1 = ls[(i*2)]
            v2 = ls[(i*2)+2]
            edge = ls[(i*2)+1]
            qset.add(v1)
            qset.add(v2)
            qset.add(edge)
            edgeId = edge+v1+v2
            if edgeId not in edgeIndex :
                # edgeIndex[edgeId] = []
                edgeIndex[edgeId] = deque()

            edgeIndex[edgeId].append(queryIndex[queryId])

            #fpTable ?????????
            # if edgeId not in fpTable["freq"] :
            #     # edgeIndex[edgeId] = []
            #     fpTable["freq"][edgeId] = 0
            # if edgeId not in fpTable["join"] :
            #     # edgeIndex[edgeId] = []
            #     fpTable["join"][edgeId] = 0

            #?????????
            n = None
            if i == 0 :
                #??????????????????
                if edgeId not in tries :
                    n = Node(nid, v1, v2, edge, None)
                    nid += 1
                    tries[edgeId] = Trie(n)
                #??????  trie??? ?????????
                else :
                    n = tries[edgeId].node
            else :
                #?????? ??? ????????? ??? ?????????
                temp = prev.isContainNode(v1, v2, edge)
                if temp is not None :
                    n = temp
                #????????? ??????????????????
                else :
                    n = Node(nid, v1, v2, edge, prev)
                    nid += 1
                    prev.children.append(n)

            prev = n
            queryIndex[queryId].nodes.append(n)


            # node ?????? ????????? ????????? ????????? ?????????. ???/
            f = open("nodes/"f"{n.id}_result", "w")
            f = open("nodes/"f"{n.id}_edge", "w")
            f.close()

    queryIndex[qprev].size = qsize*len(qset)

    for eid in edgeIndex :
        qtmp = 0
        # print(tries[eid].node)
        for q in edgeIndex[eid] :
            qtmp += q.size

        # qs =
        for n in edgeIndex[eid][0].nodes :
            if (n.edge + n.v1 + n.v2) == eid :
                n.size = qtmp/len(edgeIndex[eid])
                if n.size > qmax :
                    qmax = n.size
                break

def POP(t, uCache) :
    for k in uCache :
        if k != 'total_size' :

            a = uCache[k]['last_hit'] - uCache[k]['load_time'] +1
            j = uCache[k]['joins']
            h = uCache[k]['hits']
            s = uCache[k]['edge_size'] + uCache[k]['result_size']+1
            qs = uCache[k]['qsize']
            pusq = j/h + smax/s + qs
            pop = h/a
            uCache[k]['TTL'] = pop
    return

def POU(t, uCache) :
    for k in uCache :
        if k != 'total_size' :
            a = uCache[k]['last_hit'] - uCache[k]['load_time'] +1
            j = uCache[k]['joins']
            h = uCache[k]['hits']
            s = uCache[k]['edge_size'] + uCache[k]['result_size']+1
            qs = uCache[k]['qsize']
            pusq = smax/s + qs
            pou = h/a * j/h
            uCache[k]['TTL'] = pou
    return

def PUS(t, uCache) :
    global smax
    for k in uCache :
        if k != 'total_size' :
            a = uCache[k]['last_hit'] - uCache[k]['load_time'] +1
            j = uCache[k]['joins']
            h = uCache[k]['hits']
            s = uCache[k]['edge_size'] + uCache[k]['result_size']+1
            qs = uCache[k]['qsize']
            pusq = qs
            pou = h/a + j/h
            pus = pou * smax/s
            uCache[k]['TTL'] = pus
    return


def PUSQ(t, uCache) :
    global smax, qmax
    # global qmax
    for k in uCache :
        if k != 'total_size':
            a = uCache[k]['last_hit'] - uCache[k]['load_time'] +1
            j = uCache[k]['joins']
            h = uCache[k]['hits']
            s = uCache[k]['edge_size'] + uCache[k]['result_size']+1
            qs = uCache[k]['qsize']
            pop = h/a
            # pusq = pop + j/h + s/smax + qs
            pusq = pop + j/h + smax/s + qs
            uCache[k]['TTL'] = pusq
    return

switcher = {
    'POP' : POP,
    'POU' : POU,
    'PUS' : PUS,
    'PUSQ' : PUSQ
}


def loadPrefetchedCache(t, pCache, uCache, nodeId, th, qsize) :
    global qmax
    # ?????? ????????? prefetch ?????? ????????? ?????? ?????? ?????? X
    if nodeId not in pCache and nodeId not in uCache:
        total_size = pCache['total_size']
        edge_list = deque()
        result_list = deque()
        e_size = 0
        r_size = 0
        with open(f"nodes/{nodeId}_edge", "r+") as f1 :
            while 1:
                line1 = f1.readline()
                if not line1 :
                    break
                e_v = line1.strip().split(" ")
                e_size += len(e_v)
                edge_list.append(list(map(int, e_v)))

        with open (f"nodes/{nodeId}_result", "r+") as f2:
            while 1 :
                line2 = f2.readline()
                if not line2 :
                    break
                r_v = line2.strip().split(" ")
                r_size += len(r_v)
                result_list.append(list(map(int, r_v)))

        e_size = e_size * 4
        r_size = r_size * 4
        size = e_size + r_size
        total_size += size

        if total_size < th :
            pCache[nodeId] = {"e":edge_list, "r":result_list, "TTL":0, "edge_size": e_size, "result_size": r_size, "load_time":t, "last_hit":0, "hits" :0, "joins":0, "qsize" : qsize/qmax}
        #     pCache["total_size"] = total_size
        # else :
        #     print('???????????? ?????????')

def addTotalSize(pCache, uCache, nodeId, size) :
    if nodeId in pCache :
        pCache['total_size'] += size
    if nodeId in uCache :
        uCache['total_size'] += size

def getDataFromCache(pCache, uCache, nodeId) :
    if nodeId in pCache :
        return pCache[nodeId]
    elif nodeId in uCache :
        return uCache[nodeId]
    else :
        return None

def LoadChildrens(t, childrens, pCache, uCache, th) :
    for children in childrens :
        loadPrefetchedCache(t, pCache, uCache, children.id, th, children.size)

def searchAndJoin(currentNode, joinResult, pCache, uCache, t) :
    global request, pHit, uHit, smax
    # print(childrenNode.id, childrenNode.v1, childrenNode.edge, childrenNode.v2)
    if (len(joinResult) > 0 and len(currentNode.children) > 0):
        for childrenNode in currentNode.children :
            # print(origin_edgeId)
            # jjoinResult = []
            jjoinResult = deque()
            request += 1
            # childrenNode freq +1
            # ceid = childrenNode.edge + childrenNode.v1 + childrenNode.v2
            # fpTable['freq'][ceid] += 1

            ccres = getDataFromCache(pCache, uCache, childrenNode.id)
            # print('join:', joinResult)
            # print('?????????:', childrenNode.id, ccres)

            if ccres is None :
                with open(f"nodes/{childrenNode.id}_edge", "r") as file:
                    lines = file.readlines()
                    for line in lines :
                        ls = line.strip().split(" ")
                        childrenNode_first_id = ls[0]
                        childrenNode_last_id = ls[1]
                        for j in joinResult :
                            # print('??????????????? ?????? :', childrenNode_first_id)
                            currentNode_last_id = j[-1]
                            # print('?????? ?????? ????????? :',currentNode_last_id)
                            if childrenNode_first_id == currentNode_last_id :
                                # childrenNode join +1
                                # print('===============================> ?????? ?????? ?????? (?????????)')
                                # fpTable['join'][ceid] += 1
                                res = j.copy()
                                res.append(childrenNode_last_id)
                                # print('=================================================res : ', res)
                                jjoinResult.append(res)
                                # print('res :', res)
                                with open(f"nodes/{childrenNode.id}_result", "a") as file2:
                                    #data = f"{joinResult[0]} {childrenNode_last_id}\n"
                                    # print(childrenNode.id)
                                    file2.writelines(" ".join(res)+"\n")
                                    file2.flush()
            else :
                if childrenNode.id in pCache :
                    pHit += 1
                else :
                    uHit += 1
                # ccres['TTL'] = t
                ccres['last_hit'] = t
                ccres['hits'] += 1
                # print('##############################', ccres)
                # print('#####################################ccres : ', ccres['e'])
                for line in ccres['e'] :
                    ls = line
                    # print('#####################################ls : ', ls)
                    childrenNode_first_id = ls[0]
                    childrenNode_last_id = ls[1]

                    for j in joinResult :
                        # print('=================================================rjjjjjjjjjj : ', j)
                        # print('??????????????? ?????? :', childrenNode_first_id)
                        currentNode_last_id = j[-1]
                        # print('?????? ?????? ????????? :',currentNode_last_id)
                        if childrenNode_first_id == currentNode_last_id :
                            ccres['joins'] += 1
                            # print('===============================> ?????? ?????? ?????? (??????)')
                            # fpTable['join'][ceid] += 1
                            # childrenNode join +1
                            res = j.copy()
                            res.append(childrenNode_last_id)
                            # print('=================================================res : ', res)
                            jjoinResult.append(res)

                if len(jjoinResult) > 0 :
                    # print(jjoinResult)
                    size = (len(jjoinResult[0])*len(jjoinResult))*4
                    ccres['r'] += jjoinResult
                    ccres['result_size'] += size

                    tmp = ccres['result_size'] + ccres['edge_size']
                    if tmp > smax :
                        smax = tmp
                    # ccres['res_size'] += size
                    addTotalSize(pCache, uCache, childrenNode.id, size)

            searchAndJoin(childrenNode, jjoinResult, pCache, uCache, t)

def writeCacheToFile(value, nId) :
    # print(f"=======================================> {k} {value[k]['TTL']}")
    # print(f"edge : {value['e']}")
    # print(f"result : {value['r']}")
    w_lines_e = ""
    w_lines_r = ""
    with open(f"nodes/{nId}_edge", "w") as f1 :
        f1.seek(0)
        for ch_lines_e in value['e']: #edge ??????
            new_line_e = " ".join(map(str, ch_lines_e))
            f1.writelines(new_line_e + "\n")
        f1.flush()

            # w_lines_e +=
    with open (f"nodes/{nId}_result", "w") as f2:
        f2.seek(0)
        for ch_lines_r in value['r']:#result ??????
            new_line_r = " ".join(map(str, ch_lines_r))
            # w_lines_r += new_line_r + "\n"
            f2.writelines(new_line_r + "\n")
        f2.flush()
            # print(w_lines_r)
        # print("k => ", nId , w_lines_e)
        #f1.seek(0)
        #f2.seek(0)
        # f2.write(w_lines_r)

def replaceCache(pCache, uCache, th, t) :
    # print('u :', uCache['total_size'])
    # print('p :', pCache['total_size'])
    pNodes = list(pCache.keys())
    pNodes.remove('total_size')

    for n in pNodes :
        res = pCache[n]
        # ttl = res['TTL']
        last_hit = res['last_hit']
        size = res['edge_size'] + res['result_size']

        del pCache[n]
        # pCache['total_size'] -= size

        if last_hit > 0 :
            uCache[n] = res
            addTotalSize(pCache, uCache, n, size)

    pCache['total_size'] = 0


    temp_uCache = uCache.copy()
    del temp_uCache['total_size']

    #4?????? policy => ?????? => ????????? 'TTL'??? ??????


    func = switcher.get(policy, lambda : 'Invalid')
    func(t, uCache)

    #TTL ???????????? ??????
    sorted_keys = sorted(temp_uCache, key=lambda x: uCache[x]['TTL'])
    # print('===============================================================')
    # for k in sorted_keys :
    #     if k != 'total_size' :
    #         print(uCache[k]['TTL'])
    w_lines_e, w_lines_r = "", ""

    for k in sorted_keys:
        if uCache['total_size'] >= (th) :
            writeCacheToFile(uCache[k], k)
            uCache['total_size'] -= (uCache[k]['edge_size']+uCache[k]['result_size'])
            del uCache[k]
        else :
            break

    # for k in uCache :
    #     if k != 'total_size' :
    #         uCache[k]['TTL'] -= 10

def replaceCache2(pCache, uCache, th) :
    # print('u :', uCache['total_size'])
    # print('p :', pCache['total_size'])
    pNodes = list(pCache.keys())
    pNodes.remove('total_size')

    for n in pNodes :
        res = pCache[n]
        ttl = res['TTL']
        size = res['edge_size'] + res['result_size']

        del pCache[n]
        pCache['total_size'] -= size

        if ttl > 0 :
            uCache[n] = res
            addTotalSize(pCache, uCache, n, size)

    temp_uCache = uCache.copy()
    del temp_uCache['total_size']

    sorted_keys = sorted(temp_uCache, key=lambda x: uCache[x]['TTL'])

    w_lines_e, w_lines_r = "", ""

    for k in sorted_keys:
        if uCache[k]['TTL'] <= 0 or uCache['total_size'] >= (th) :
            writeCacheToFile(uCache[k], k)
            uCache['total_size'] -= (uCache[k]['edge_size']+uCache[k]['result_size'])
            del uCache[k]
        else :
            break

    for k in uCache :
        if k != 'total_size' :
            uCache[k]['TTL'] -= 10

# policy = 'POP' #1.63 15.68
# policy = 'POU' #0.80 15.1
# policy = 'PUS' #0.75 14.34
policy = 'PUSQ' #0.75 14.28

# filename = "datafile_wikitalk.txt"
# filename = "datafile_cit_Patents.txt"
filename = "datafile_youtube.txt"

with open(filename, "r") as f:
    last_time = 0
    cnt = 0
    start = time.time()
    total_start = start
    windowTime = 0
    pCache = {'total_size' : 0}
    uCache = {'total_size' : 0}
    totalMemory = 1073741824 # 1GB
    percentage = 0.0001 # 1perent = 10M, 0.1 1M
    th = totalMemory * percentage
    alpha, beta = 0.0, 0.0 # alpha => uCache ??????, beta => pCache
    totalWindow = 100
    alpha = 0.4
    beta = 1-alpha

    while 1 :
        sstart = time.time()
        # joinResult = [] #???????????? ??????
        joinResult = deque()
        line = f.readline()
        ls = line.strip().split("\t")

        v1_id = ls[0]
        v2_id = ls[1]
        v1 = ls[2]
        v2 = ls[3]
        edge = ls[4]
        origin_edgeId = edge+v1+v2
        t = int(ls[5])

        if (t > 1 and last_time < t):

            # print("Time Stamp Changed !!", t)
            end = time.time()
            # ????????? ?????? ?????? ?????? ??????
            # print(f"interval {last_time}->{t} elapsed time => ",end - start," sec")

            windowTime += end - start

            if t % 10 == 0 :
                # print(pCache)
                print(windowTime)
                # print(uCache)
                # for k in uCache :
                #     if k is not 'total_size' :
                #         print(uCache[k]['TTL'])
                        # print('hits:', uCache[k]['hits'])
                        # print('joins:', uCache[k]['joins'])
                # if uCache['total_size'] > (th*alpha) or pCache['total_size'] > (th*beta):
                #     replaceCache(pCache, uCache, th*alpha)
                # for nid in uCache :
                #     if nid != 'total_size' :
                #         writeCacheToFile(uCache[nid], nid)
                windowTime = 0

            start = time.time()
            if(t > totalWindow):
                # print(uCache)
                for nid in uCache :
                    if nid != 'total_size' :
                        # print("uCache:Write to ", nid)
                        writeCacheToFile(uCache[nid], nid)

                for nid in pCache :
                    if nid != 'total_size' :
                        # print("pCache:Write to ", nid)
                        writeCacheToFile(pCache[nid], nid)
                break

        v1var_edgeId = edge+v1+'var'
        varv2_edgeId = edge+'var'+v2
        varvar_edgeId = edge+'var'+'var'
        v1v2_varId = 'var'+v1+v2
        v1var_varId = 'var'+v1+'var'
        varv2_varId = 'var'+'var'+v2
        varvar_varId = 'var'+'var'+'var'

        # edgeId_List = [origin_edgeId, v1var_edgeId, varv2_edgeId, varvar_edgeId]
        # edgeId_List = deque([origin_edgeId, v1var_edgeId, varv2_edgeId, varvar_edgeId])
        edgeId_List = deque([origin_edgeId, v1var_edgeId, varv2_edgeId, varvar_edgeId,v1v2_varId, v1var_varId, varv2_varId, varvar_varId])
        # graph data??? key??? edgeIndex??? ???????????? ????????? ?????? (?????? Edge ??????)
        currentNodes = deque()
        # currentNodes = []
        for edgeId in edgeId_List :
            # print('?????? edgeId : ', edgdId)
            if edgeId in edgeIndex :
                # print(edgeId)
                ltemp = len(edgeId) - edgeId[::-1].index('v')-1
                esindex = 3 if edgeId[0] == 'v' else 2
                v1 = edgeId[esindex: ltemp]
                v2 = edgeId[ltemp:]
                edge = edgeId[0:esindex]
                # print(edge, v1, v2)
                for q in edgeIndex[edgeId] : # edgeIndex????????? Query?????? ??????
                    for currentNode in queryIndex[q.id].nodes : # ??? Query????????? QueryID??? ????????? ?????? ????????? ?????? (Q1????????? n2, n4 ?????????)
                        if currentNode.isMatch(v1, v2, edge) : # ?????? ???????????? edgeId??? ???????????? ????????? ??????
                            #currentNode = n
                            if currentNode not in currentNodes :
                                currentNodes.append(currentNode)

        eend = time.time()
        # print('time0=> ', eend-sstart)
        # v1 = ls[2]
        # v2 = ls[3]
        sstart = time.time()
        for currentNode in currentNodes :
            # print(origin_edgeId)
            # joinResult = []
            joinResult = deque()
            # data = [int(v1_id), int(v2_id)]
            data = deque()
            data.append(int(v1_id))
            data.append(int(v2_id))

            # print("data = ===  ", data)
            # ?????? edge??? ????????? edge ????????? ??????
            # with open(f"nodes/{currentNode.id}_edge", "a") as file:
            #     #data = f"{v1_id} {v2_id}\n"
            #     file.write(data+"\n")
            request += 1
            # eid = currentNode.edge + currentNode.v1 + currentNode.v2
            # fpTable['freq'][eid] += 1


            cres = getDataFromCache(pCache, uCache, currentNode.id)
            if cres is None: # ????????? ????????? ????????? ?????? Load
                loadPrefetchedCache(t, pCache, uCache, currentNode.id, totalMemory, currentNode.size)
                cres = pCache[currentNode.id]
                cres["load_time"] = t
            else :
                if currentNode.id in pCache :
                    pHit += 1
                else :
                    uHit += 1

            cres["last_hit"] = t
            cres["hits"] += 1
            # cres['TTL'] = t
            cres['edge_size'] += 8
            # cres['res_size'] += 8
            cres['e'].append(data)
            addTotalSize(pCache, uCache, currentNode.id, 8)

            # ????????? ?????? ??? : ?????? edge ????????? ?????? node ?????? ????????? ?????? / joinResult??? node ?????? ??????
            if currentNode.parent is None :
                joinResult.append(data)
                cres['joins'] += 1
                # if cres is None :
                #     with open(f"nodes/{currentNode.id}_result", "a") as file:
                #         #data = f"{v1_id} {v2_id}\n"
                #         file.write(data+"\n")
                #         # print('?????? ?????? id : ', currentNode.id)
                #         # ?????? ????????? ?????? ?????? => ?????? ????????? ????????? Load
                # else :
                LoadChildrens(t, currentNode.children, pCache, uCache, th*beta)
                    # print(pCache)

            else :
                # ?????? ????????? ?????? ?????? =
                # print('?????? ?????? id : ', currentNode.id)
                # 1. ?????? ????????? ????????? Load
                loadPrefetchedCache(t, pCache, uCache, currentNode.parent.id, th*beta, currentNode.parent.size)
                pres = getDataFromCache(pCache, uCache, currentNode.parent.id)
                request += 1
                if currentNode.parent.id in pCache :
                    pHit += 1
                else :
                    uHit += 1
                # 2. ?????? ????????? ????????? ???????????? ??????
                if pres is not None :
                    if pres['result_size'] > 0 :
                    # print("????????? ???[]??? => ", currentNode.parent.id)
                    # 3. ?????? ???????????? ????????? ?????? ?????? ????????? ????????? Load
                        LoadChildrens (t, currentNode.children, pCache, uCache, th*beta)

                        # ?????? freq +1
                        # peid = currentNode.parent.edge + currentNode.parent.v1 + currentNode.parent.v2
                        # fpTable['freq'][peid] += 1
                        pres['last_hit'] = t
                        pres['hits'] += 1
                        # pres['TTL'] = t

                        # ?????? ????????? pCache??? ?????? ?????? HIT => ????????? ????????? ?????? ???????????? ?
                        # if currentNode.parent.id in pCache.keys():
                        edgeResult = pres['e']
                        nodeResult = pres['r']
                        # print('edgeResult : ', edgeResult)
                        # print('nodeResult : ', nodeResult)
                        for vid in nodeResult :
                            parent_last_id = vid[-1]
                            # print(vid)
                            if parent_last_id == v1_id :
                                data = vid.copy()
                                data.append(v2_id)
                                joinResult.append(data)
                                # print('??????~~~~~~~~~~~~~~~')
                                # fpTable['join'][eid] += 1
                                cres['joins'] += 1
                                # ?????? ?????? join +1
                                # print('???????????')

                    # print("ucache ??? ???????????? ??????")
                elif pres is None :
                    # print('????????? ??????')
                    with open(f"nodes/{currentNode.parent.id}_result", "r") as file:
                        lines = file.readlines()
                        for line in lines : # ????????? ????????? ???????????? ????????? ??????
                            line = list(map(int, line.strip().split(" "))) #????????? ???????????? ..
                            if len(line) != 0 : # ?????? ????????? ?????? ??? : ?????? node??? ????????? v_id??? ?????? edge??? ?????? v_id ????????? ??????
                                parent_last_id = line[-1]
                                # ?????? ????????? :
                                if parent_last_id == v1_id :
                                    # print('??????~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                                    # fpTable['join'][eid] += 1
                                    # ?????? ?????? join +1
                                    # ?????? node ?????? ????????? ?????? node ?????? + ?????? edge??? ????????? v_id ????????? ?????? / joinResult??? node ?????? ??????
                                    with open(f"nodes/{currentNode.id}_result", "a") as file2:
                                        # parent_result = list(map(int, line.split(" ")))
                                        line.append(v2_id)
                                        # data =
                                        # print('------------------------------------------', data)
                                        file2.write(" ".join(line)+"\n")
                                        file2.flush()
                                        #if data not in joinResult :
                                        joinResult.append(line)


            # join ????????? ?????? ????????? ?????? ?????? : ??????
            if len(joinResult) > 0 :
                print(joinResult)
                size = (len(joinResult[0])*len(joinResult))*4
                cres['r'] += joinResult
                cres['result_size'] += size
                tmp = cres['result_size'] + cres['edge_size']
                if tmp > smax :
                    smax = tmp
                # cres['res_size'] += size
                addTotalSize(pCache, uCache, currentNode.id, size)

            searchAndJoin(currentNode, joinResult, pCache, uCache, t)

        if uCache['total_size'] > (th*alpha) or pCache['total_size'] > (th*beta):
            replaceCache(pCache, uCache, th*alpha, t)
        # if pCache['total_size'] > (th*beta) :
        #     replaceCache(pCache, uCache, th*alpha, t)


        last_time = t
        cnt += 1

# ?????? ?????? ?????? ??????

end = time.time()
print('request :', request)
print('pHit :', pHit)
print('uHit :', uHit)
print('alpha, beta :', alpha, beta)
print('percentage :', percentage)
print('totalWindow :', totalWindow)
print('filename : ', filename)
# print(joinResult)
print(policy, end - total_start)
#print(f"total elapsed time => ",end - total_start," sec")

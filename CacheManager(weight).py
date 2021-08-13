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

with open("query.txt", "r") as f:
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

            #fpTable 초기화
            # if edgeId not in fpTable["freq"] :
            #     # edgeIndex[edgeId] = []
            #     fpTable["freq"][edgeId] = 0
            # if edgeId not in fpTable["join"] :
            #     # edgeIndex[edgeId] = []
            #     fpTable["join"][edgeId] = 0

            #초기화
            n = None
            if i == 0 :
                #새로만드는거
                if edgeId not in tries :
                    n = Node(nid, v1, v2, edge, None)
                    nid += 1
                    tries[edgeId] = Trie(n)
                #기존  trie에 있는거
                else :
                    n = tries[edgeId].node
            else :
                #자식 중 겹치는 거 있는지
                temp = prev.isContainNode(v1, v2, edge)
                if temp is not None :
                    n = temp
                #없으면 새로만드는거
                else :
                    n = Node(nid, v1, v2, edge, prev)
                    nid += 1
                    prev.children.append(n)

            prev = n
            queryIndex[queryId].nodes.append(n)


            # node 별로 파일을 강제로 하나씩 만들어. 넵/
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
            h = uCache[k]['hits']
            a = uCache[k]['last_hit']-uCache[k]['load_time'] +1
            pop = h/a

            uCache[k]['TTL'] = pop
    return

def POU(t, uCache) :
    for k in uCache :
        if k != 'total_size' :
            h = uCache[k]['hits']
            a = uCache[k]['last_hit'] - uCache[k]['load_time'] +1
            j = uCache[k]['joins']
            pou = h/a + j/h
            uCache[k]['TTL'] = pou
    return

def PUS(t, uCache) :
    global smax
    for k in uCache :
        if k != 'total_size' :
            a = uCache[k]['last_hit'] - uCache[k]['load_time'] +1
            j = uCache[k]['joins']
            s = uCache[k]['edge_size'] + uCache[k]['result_size']+1

            pus = j/a * smax/s
            uCache[k]['TTL'] = pus
    return


def PUSQ(t, uCache) :
    global smax, qmax
    # global qmax
    for k in uCache :
        if k != 'total_size' :
            a = uCache[k]['last_hit'] - uCache[k]['load_time'] +1
            j = uCache[k]['joins']
            h = uCache[k]['hits']
            s = uCache[k]['edge_size'] + uCache[k]['result_size']+1
            qs = uCache[k]['qsize']

            pusq = h/a + j/h + smax/s + qs/qmax
            uCache[k]['TTL'] = pusq
    return

switcher = {
    'POP' : POP,
    'POU' : POU,
    'PUS' : PUS,
    'PUSQ' : PUSQ
}


def loadPrefetchedCache(t, pCache, uCache, nodeId, th, qsize) :
    # 이미 노드가 prefetch 되어 있으면 다시 올릴 필요 X
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
            pCache[nodeId] = {"e":edge_list, "r":result_list, "TTL":0, "edge_size": e_size, "result_size": r_size, "load_time":t, "last_hit":0, "hits" :0, "joins":0, "qsize" : qsize}
        #     pCache["total_size"] = total_size
        # else :
        #     print('ㄴ머어서 안대내')

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
            # print('ㅋㅋㅋ:', childrenNode.id, ccres)

            if ccres is None :
                with open(f"nodes/{childrenNode.id}_edge", "r") as file:
                    lines = file.readlines()
                    for line in lines :
                        ls = line.strip().split(" ")
                        childrenNode_first_id = ls[0]
                        childrenNode_last_id = ls[1]
                        for j in joinResult :
                            # print('자식노드의 현재 :', childrenNode_first_id)
                            currentNode_last_id = j[-1]
                            # print('현재 노드 마지막 :',currentNode_last_id)
                            if childrenNode_first_id == currentNode_last_id :
                                # childrenNode join +1
                                # print('===============================> 자식 노드 조인 (디스크)')
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
                        # print('자식노드의 현재 :', childrenNode_first_id)
                        currentNode_last_id = j[-1]
                        # print('현재 노드 마지막 :',currentNode_last_id)
                        if childrenNode_first_id == currentNode_last_id :
                            ccres['joins'] += 1
                            # print('===============================> 자식 노드 조인 (캐시)')
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
        for ch_lines_e in value['e']: #edge 저장
            new_line_e = " ".join(map(str, ch_lines_e))
            f1.writelines(new_line_e + "\n")
        f1.flush()

            # w_lines_e +=
    with open (f"nodes/{nId}_result", "w") as f2:
        f2.seek(0)
        for ch_lines_r in value['r']:#result 저장
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

    #4가지 policy => 계산 => 결과를 'TTL'에 저장


    func = switcher.get(policy, lambda : 'Invalid')
    func(t, uCache)

    #TTL 기준으로 정렬
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
filename = "datafile_youtube.txt"
# filename = "datafile_cit_Patents.txt"

for i in range(1,11) :
    totalMemory = 1073741824 # 1GB
    percentage = 0.0001   # 1perent = 10M, 0.1 1M
    th = totalMemory * percentage
    alpha, beta = 0.0, 0.0 # alpha => uCache 용량, beta => pCache
    totalWindow = 500
    alpha = i * 0.1
    beta = 1-alpha
    timesum =0
    for nid in range(0, 65) :
        f = open("nodes/"f"{nid}_result", "w")
        f.close()
        f = open("nodes/"f"{nid}_edge", "w")
        f.close()

    with open(filename, "r") as f:
        last_time = 0
        cnt = 0
        start = time.time()
        total_start = start
        windowTime = 0
        pCache = {'total_size' : 0}
        uCache = {'total_size' : 0}

        while 1 :
            sstart = time.time()
            # joinResult = [] #조인결과 변수
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
                # 윈도우 별로 걸린 시간 측정
                # print(f"interval {last_time}->{t} elapsed time => ",end - start," sec")

                # print('freq :', fpTable['freq'])
                # print(f'{t-1} join :', fpTable['join'])

                windowTime += end - start
                timesum += windowTime
                # 여기에서 fpTable을 이용해 TTL을 계산


                # 헌 윈도우가 끝나면 fpTable 초기화 (한 윈도우가 끝나면 fpTable을 이용하여 TTL을 계산하고 초기화)
                # for k in fpTable :
                #     for v in fpTable[k] :
                #         fpTable[k][v] = 0


                if t % 10 == 0 :
                    # print(pCache)

                    # print(windowTime)
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

                    print("===========window========", alpha, ",,,", beta)
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
            # graph data의 key가 edgeIndex에 저장되어 있는지 확인 (유효 Edge 확인)
            currentNodes = deque()
            # currentNodes = []
            for edgeId in edgeId_List :
                # print('후보 edgeId : ', edgdId)
                if edgeId in edgeIndex :
                    # print(edgeId)
                    ltemp = len(edgeId) - edgeId[::-1].index('v')-1
                    esindex = 3 if edgeId[0] == 'v' else 2
                    v1 = edgeId[esindex: ltemp]
                    v2 = edgeId[ltemp:]
                    edge = edgeId[0:esindex]
                    # print(edge, v1, v2)
                    for q in edgeIndex[edgeId] : # edgeIndex로부터 Query객체 접근
                        for currentNode in queryIndex[q.id].nodes : # 각 Query객체의 QueryID를 이용해 노드 목록을 얻음 (Q1이라면 n2, n4 이런거)
                            if currentNode.isMatch(v1, v2, edge) : # 노드 목록들중 edgeId와 일치하는 노드를 찾음
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
                # 현재 edge는 무조건 edge 파일에 추가
                # with open(f"nodes/{currentNode.id}_edge", "a") as file:
                #     #data = f"{v1_id} {v2_id}\n"
                #     file.write(data+"\n")
                request += 1
                # eid = currentNode.edge + currentNode.v1 + currentNode.v2
                # fpTable['freq'][eid] += 1


                cres = getDataFromCache(pCache, uCache, currentNode.id)
                if cres is None: # 캐시에 없으면 파일로 부터 Load
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

                # 부모가 없을 때 : 현재 edge 파일을 현재 node 결과 파일에 기록 / joinResult에 node 결과 추가
                if currentNode.parent is None :
                    joinResult.append(data)
                    cres['joins'] += 1
                    # if cres is None :
                    #     with open(f"nodes/{currentNode.id}_result", "a") as file:
                    #         #data = f"{v1_id} {v2_id}\n"
                    #         file.write(data+"\n")
                    #         # print('현재 노드 id : ', currentNode.id)
                    #         # 부모 노드가 없는 경우 => 자식 노드의 결과를 Load
                    # else :
                    LoadChildrens(t, currentNode.children, pCache, uCache, th*beta)
                        # print(pCache)

                else :
                    # 부모 노드가 있는 경우 =
                    # print('현재 노드 id : ', currentNode.id)
                    # 1. 부모 노드의 결과를 Load
                    loadPrefetchedCache(t, pCache, uCache, currentNode.parent.id, th*beta, currentNode.parent.size)
                    pres = getDataFromCache(pCache, uCache, currentNode.parent.id)

                    # 2. 부모 노드의 결과가 존재하는 경우
                    if pres is not None :
                        if pres['result_size'] > 0 :
                        # print("조인에 사[]용 => ", currentNode.parent.id)
                        # 3. 자식 노드와의 조인을 위해 자식 노드의 결과를 Load
                            LoadChildrens (t, currentNode.children, pCache, uCache, th*beta)

                            # 부모 freq +1
                            # peid = currentNode.parent.edge + currentNode.parent.v1 + currentNode.parent.v2
                            # fpTable['freq'][peid] += 1
                            pres['last_hit'] = t
                            pres['hits'] += 1
                            pres['TTL'] = t

                            # 부모 노드가 pCache에 있는 경우 HIT => 결과를 가지고 와서 조인해봄 ?
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
                                    # print('조인~~~~~~~~~~~~~~~')
                                    # fpTable['join'][eid] += 1
                                    cres['joins'] += 1
                                    # 현재 노드 join +1
                                    # print('ㅈㅇㄷ??')

                        # print("ucache 에 현재노드 삽입")
                    elif pres is None :
                        # print('캐시에 업음')
                        with open(f"nodes/{currentNode.parent.id}_result", "r") as file:
                            lines = file.readlines()
                            for line in lines : # 부모의 정보를 읽어와서 한줄씩 처리
                                line = list(map(int, line.strip().split(" "))) #엔터가 들어가서 ..
                                if len(line) != 0 : # 부모 결과가 있을 때 : 부모 node의 마지막 v_id와 현재 edge의 처음 v_id 조인을 시도
                                    parent_last_id = line[-1]
                                    # 조인 성공시 :
                                    if parent_last_id == v1_id :
                                        # print('조인~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                                        # fpTable['join'][eid] += 1
                                        # 현재 노드 join +1
                                        # 현재 node 결과 파일에 부모 node 결과 + 현재 edge의 두번째 v_id 결과를 추가 / joinResult에 node 결과 추가
                                        with open(f"nodes/{currentNode.id}_result", "a") as file2:
                                            # parent_result = list(map(int, line.split(" ")))
                                            line.append(v2_id)
                                            # data =
                                            # print('------------------------------------------', data)
                                            file2.write(" ".join(line)+"\n")
                                            file2.flush()
                                            #if data not in joinResult :
                                            joinResult.append(line)


                # join 결과가 있고 자식이 있는 경우 : 반복
                if len(joinResult) > 0 :
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
            #     replaceCache(pCache, uCache, th*alpha)


            last_time = t
            cnt += 1
    print(timesum)

# 전체 걸린 시간 확인

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

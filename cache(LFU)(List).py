import pprint
import time
from operator import itemgetter
import operator
class Query():
    def __init__(self, id):
        self.id = id
        self.nodes = []

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
        self.children = []

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

#query("test")
nid = 0
with open("query.txt", "r") as f:
    lines = f.readlines()
    for line in lines :
        ls = line.strip().split(" ")
        queryId = ls[-1]
        if queryId not in queryIndex :
            queryIndex[queryId] = Query(queryId)

        numEdge = (len(ls)-2)//2

        #loop edge
        prev = None
        for i in range(numEdge) :
            v1 = ls[(i*2)]
            v2 = ls[(i*2)+2]
            edge = ls[(i*2)+1]
            edgeId = edge+v1+v2
            if edgeId not in edgeIndex :
                edgeIndex[edgeId] = []
            edgeIndex[edgeId].append(queryIndex[queryId])

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

# # # # # Trie 노드 목록 출력
# for t in tries :
#     print("----------Tries----------")
#     tries[t].searchAndPrint(tries[t].node)
#
# # 노드 id 출력
# for q in queryIndex :
#     print(q)
#     for n in queryIndex[q].nodes :
#         print(n.id)

# pprint.pprint(queryIndex)
# pprint.pprint(edgeIndex)


def loadPrefetchedCache(pCache, uCache, nodeId, th) :
    # 이미 노드가 prefetch 되어 있으면 다시 올릴 필요 X
    if nodeId not in pCache and nodeId not in uCache:
        total_size = pCache['total_size']
        with open(f"nodes/{nodeId}_edge", "r+") as f1, open (f"nodes/{nodeId}_result", "r+") as f2:
            while 1 :
                line1 = f1.readline()
                line2 = f2.readline()

                edge_list = []
                result_list = []
                e_size = 0
                r_size = 0
                #node_key = str(i)

                if not line1 :
                    break
                if not line2 :
                    break

                e_v = line1.strip().split(" ")
                e_size += len(e_v)
                edge_list.append(list(map(int, e_v)))

                e_size = e_size * 4

                r_v = line2.strip().split(" ")
                r_size += len(r_v)
                result_list.append(list(map(int, r_v)))
                #f2.writelines(" ".join(r_v))
                #f2.writelines("\n")

                r_size = r_size * 4

        size = e_size + r_size
        total_size += size

        if total_size < th :
            pCache[nodeId] = {"e":edge_list, "r":result_list, "TTL":0, "edge_size": e_size, "result_size": r_size}
            pCache["total_size"] = total_size

        # print(pCache)
    '''
    with open(edge_file[i], "w") as w1, open (result_file[i], "w") as w2:
        lines3 = w1.writelines
        lines4 = w2.writelines

        w1.writelines(" ".join(edge_list[j]))
        w1.writelines("\n")

        w2.writelines(" ".join(result_list[j]))
        w2.writelines("\n")

        j += 1
    '''

def addTotalSize(pCache, uCache, nodeId, size) :
    if nodeId in pCache :
        pCache['total_size'] += size
    if nodeId in uCache :
        uCache['total_size'] += size

# def subTotalSize(pCache, uCache, nodeId, size) :
#     if nodeId in pCache :
#         pCache['total_size'] -= size
#     elif nodeId in uCache :
#         uCache['total_size'] -= size

def getDataFromCache(pCache, uCache, nodeId) :
    if nodeId in pCache :
        return pCache[nodeId]
    elif nodeId in uCache :
        return uCache[nodeId]
    else :
        return None


def LoadChildrens(childrens, pCache, uCache, th) :
    for children in childrens :
        loadPrefetchedCache(pCache, uCache, children.id, th)

def searchAndJoin(currentNode, joinResult, pCache, uCache) :
    # print(childrenNode.id, childrenNode.v1, childrenNode.edge, childrenNode.v2)
    if (len(joinResult) > 0 and len(currentNode.children) > 0):
        for childrenNode in currentNode.children :
            jjoinResult = []
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
                                res = j.copy()
                                res.append(childrenNode_last_id)
                                # print('=================================================res : ', res)
                                jjoinResult.append(res)
                                # print('res :', res)
                                with open(f"nodes/{childrenNode.id}_result", "a") as file2:
                                    #data = f"{joinResult[0]} {childrenNode_last_id}\n"
                                    # print(childrenNode.id)
                                    file2.write(res+"\n")
            else :
                ccres['TTL'] += 10
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
                            res = j.copy()
                            res.append(childrenNode_last_id)
                            # print('=================================================res : ', res)
                            jjoinResult.append(res)

                if len(jjoinResult) > 0 :
                    # print(jjoinResult)
                    size = (len(jjoinResult[0])*len(jjoinResult))*4
                    ccres['r'] = ccres['r'] + jjoinResult
                    ccres['result_size'] += size
                    addTotalSize(pCache, uCache, childrenNode.id, size)

            searchAndJoin(childrenNode, jjoinResult, pCache, uCache)

def writeCacheToFile(value, nId) :
    # print(f"=======================================> {k} {value[k]['TTL']}")
    # print(f"edge : {value['e']}")
    # print(f"result : {value['r']}")
    w_lines_e = ""
    w_lines_r = ""
    with open(f"nodes/{nId}_edge", "w") as f1, open (f"nodes/{nId}_result", "w") as f2:
        for ch_lines_e in value['e']: #edge 저장
            new_line_e = " ".join(map(str, ch_lines_e))
            f1.write(new_line_e + "\n")
            # w_lines_e +=

        for ch_lines_r in value['r']:#result 저장
            new_line_r = " ".join(map(str, ch_lines_r))
            # w_lines_r += new_line_r + "\n"
            f2.write(new_line_r + "\n")
            # print(w_lines_r)
        # print("k => ", nId , w_lines_e)
        #f1.seek(0)
        #f2.seek(0)
        # f2.write(w_lines_r)
        f1.flush()
        f2.flush()

# pprint.pprint(edgeIndex)
for i in range(11) :
    totalMemory = 1073741824 # 1GB
    percentage = 0.000001  # 1perent = 10M, 0.1 1M
    th = totalMemory * percentage
    alpha, beta = 0.0, 0.0 # alpha => uCache 용량, beta => pCache
    beta = i*0.1
    alpha = 1-beta
    with open("datafile.txt", "r") as f:
        last_time = 0
        cnt = 0
        start = time.time()
        total_start = start
        windowTime = 0
        pCache = {'total_size' : 0}
        uCache = {'total_size' : 0}




        while 1 :
            sstart = time.time()
            joinResult = [] #조인결과 변수
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

                windowTime += end - start
                if t % 10 == 0 :
                    print(windowTime)
                    # for nid in uCache :
                    #     if nid != 'total_size' :
                    #         writeCacheToFile(uCache[nid], nid)
                    windowTime = 0
                start = time.time()
                if(t > 500):
                    for nid in uCache :
                        if nid != 'total_size' :
                            writeCacheToFile(uCache[nid], nid)
                    print("===========window========", beta, ",,,", alpha)
                    break

            v1var_edgeId = edge+v1+'var'
            varv2_edgeId = edge+'var'+v2
            varvar_edgeId = edge+'varvar'
            edgeId_List = [origin_edgeId, v1var_edgeId, varv2_edgeId, varvar_edgeId]
            # graph data의 key가 edgeIndex에 저장되어 있는지 확인 (유효 Edge 확인)

            currentNodes = []
            for edgeId in edgeId_List :
                # print('후보 edgeId : ', edgdId)
                if edgeId in edgeIndex :
                    #print(edgeId[::-1], (edgeId[::-1]).index('v'))
                    ltemp = len(edgeId) - edgeId[::-1].index('v')-1
                    v1 = edgeId[2: ltemp]
                    v2 = edgeId[ltemp:]

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
                joinResult = []
                data = [int(v1_id), int(v2_id)]
                # print("data = ===  ", data)
                # 현재 edge는 무조건 edge 파일에 추가
                # with open(f"nodes/{currentNode.id}_edge", "a") as file:
                #     #data = f"{v1_id} {v2_id}\n"
                #     file.write(data+"\n")
                cres = getDataFromCache(pCache, uCache, currentNode.id)
                if cres is None:
                    uCache[currentNode.id] = {'e' : [], 'r' : [], 'TTL' : 0, "edge_size": 0, "result_size": 0}
                    cres = uCache[currentNode.id]

                cres['TTL'] += 5
                cres['edge_size'] += 8
                cres['e'].append(data)
                addTotalSize(pCache, uCache, currentNode.id, 8)

                # 부모가 없을 때 : 현재 edge 파일을 현재 node 결과 파일에 기록 / joinResult에 node 결과 추가
                if currentNode.parent is None :
                    joinResult.append(data)
                    if cres is None :
                        with open(f"nodes/{currentNode.id}_result", "a") as file:
                            #data = f"{v1_id} {v2_id}\n"
                            file.write(data+"\n")
                            # print('현재 노드 id : ', currentNode.id)
                            # 부모 노드가 없는 경우 => 자식 노드의 결과를 Load
                    else :
                        LoadChildrens(currentNode.children, pCache, uCache, th*beta)
                        # print(pCache)
                else :
                    # 부모 노드가 있는 경우 =
                    # print('현재 노드 id : ', currentNode.id)
                    # 1. 부모 노드의 결과를 Load
                    loadPrefetchedCache(pCache, uCache, currentNode.parent.id, th*beta)
                    pres = getDataFromCache(pCache, uCache, currentNode.parent.id)
                    # 2. 부모 노드의 결과가 존재하는 경우
                    if pres is not None :
                        if pres['result_size'] > 0 :
                        # print("조인에 사[]용 => ", currentNode.parent.id)
                        # 3. 자식 노드와의 조인을 위해 자식 노드의 결과를 Load
                            LoadChildrens (currentNode.children, pCache, uCache, th*beta)

                            pres['TTL'] += 10
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
                                    # print('ㅈㅇㄷ??')

                        # print("ucache 에 현재노드 삽입")
                    elif pres is None :
                        # print('캐시에 업음')

                    # else :
                        with open(f"nodes/{currentNode.parent.id}_result", "r") as file:
                            lines = file.readlines()
                            for line in lines : # 부모의 정보를 읽어와서 한줄씩 처리
                                line = line.strip() #엔터가 들어가서 ..
                                if len(line) != 0 : # 부모 결과가 있을 때 : 부모 node의 마지막 v_id와 현재 edge의 처음 v_id 조인을 시도
                                    parent_last_id = line.split(" ")[-1]
                                    # 조인 성공시 :
                                    if parent_last_id == v1_id :
                                        # 현재 node 결과 파일에 부모 node 결과 + 현재 edge의 두번째 v_id 결과를 추가 / joinResult에 node 결과 추가
                                        with open(f"nodes/{currentNode.id}_result", "a") as file2:
                                            parent_result = line
                                            data = f"{parent_result} {v2_id}"
                                            # print('------------------------------------------', data)
                                            file2.write(data+"\n")
                                            #if data not in joinResult :
                                            joinResult.append(data)


                # join 결과가 있고 자식이 있는 경우 : 반복
                if len(joinResult) > 0 :
                    size = (len(joinResult[0])*len(joinResult))*4
                    cres['r'] = cres['r'] + joinResult
                    cres['result_size'] += size
                    addTotalSize(pCache, uCache, currentNode.id, size)

                searchAndJoin(currentNode, joinResult, pCache, uCache)


            # print(f"{cnt} {uCache}")
            pNodes = list(pCache.keys())
            pNodes.remove('total_size')

            eend = time.time()
            # print('time => ', eend-sstart)
            # print(f"{cnt} uCache : {uCache}")
            # print(f"{cnt} pCache : {pCache}")
            sstart = time.time()
            for n in pNodes :
                # print(pCache[n]['TTL'])
                res = pCache[n]
                ttl = res['TTL']
                size = res['edge_size'] + res['result_size']
                # print('pres :', res)
                del pCache[n]
                # pCache의 TTL이 0 이상이면 uCache로 이동
                if ttl > 0 :
                    # subTotalSize(pCache, uCache, n, size)
                    # del pCache[n]

                    uCache[n] = res
                    addTotalSize(pCache, uCache, n, size)

                    # print('ures :', uCache[n])
                # elif ttl == 0 :
            eend = time.time()
            # print('time2 => ', eend-sstart)
            pCache['total_size'] = 0

            # print(uCache)

            sort_strat_time = time.time()
            # print("### sort_strat_time : ", sort_strat_time)

            temp_uCache = uCache.copy()
            del temp_uCache['total_size']

            sorted_keys = sorted(temp_uCache, key=lambda x: uCache[x]['TTL'])
            # print(sorted_keys)

            w_lines_e, w_lines_r = "", ""

            sort_end_time = time.time()
            # print("### sort_end_time : ", sort_end_time-sort_strat_time)
            # uCache => TTL => 0

            wirte_start_time = time.time()
            for k in sorted_keys:
                if uCache[k]['TTL'] == 0 or uCache['total_size'] >= (th*alpha) :
                    writeCacheToFile(uCache[k], k)
                    uCache['total_size'] -= (uCache[k]['edge_size']+uCache[k]['result_size'])
                    del uCache[k]
                else :
                    break

            for k in uCache :
                if k != 'total_size' :
                    uCache[k]['TTL'] -= 1
                    # total size =>
                # del uCache[k]
            wirte_end_time = time.time()
            # print("### wirte_end_time : ", wirte_end_time-wirte_start_time)
            # 1. pCache => uCache (if pCache , ttl > 0 => uCache) pCache 삭제, uCache 추가, 사이즈변경대ㅑㅐ야대
            # 2. pCache ttl  == 0 , 그냥 삭제(사이즈가 계속 변경도ㅠㅣ야대)
            # 3. TTL에 따라 ucache 정리 (LRU) // TH , TTL 수ㅡㄴ으로 소팅 =->   TH보다 작을때까지 캐수ㅟ정리  => Ucache => File (Edge / Result)

            last_time = t
            cnt += 1

# 전체 걸린 시간 확인

end = time.time()
# print(joinResult)
print(end - total_start)
#print(f"total elapsed time => ",end - total_start," sec")

import random
import math
import os
import re

os.chdir("C:/Users/Hyeuk/code/data/")
data_path = os.getcwd()

edge_folder = os.path.join(data_path, 'edge')
result_folder = os.path.join(data_path, 'result')
edge_file = [os.path.join(edge_folder, s) for s in os.listdir(edge_folder)]
result_file = [os.path.join(result_folder, s) for s in os.listdir(result_folder)]

total_size = 0
node_dict = {}
j = 0
for i in range(0, len(edge_file)):

    with open(edge_file[i], "r+") as f1, open (result_file[i], "r+") as f2:

        lines1 = f1.readlines()
        lines2 = f2.readlines()
        edge_list = []
        result_list = []
        e_size = 0
        r_size = 0
        node_key = str(i)

        for line1 in lines1:
            e_v = line1.strip().split(" ")
            e_size += len(e_v)
            edge_list.append(list(map(int, e_v)))


        e_size = e_size * 4

        for line2 in lines2:
            r_v = line2.strip().split(" ")
            r_size += len(r_v)
            result_list.append(list(map(int, r_v)))
            #f2.writelines(" ".join(r_v))
            #f2.writelines("\n")

        r_size = r_size * 4

    node_dict[node_key] = {"e":edge_list, "r":result_list, "TTL":0, "edge_size": e_size, "result_size": r_size}
    size = e_size + r_size
    total_size += size
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


node_dict["total_size"] = total_size
print(node_dict)

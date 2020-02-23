
import numpy as np

edges = [[1,5],[1,2],[2,1],[7,2],[7,5],[4,7],[4,5],[5,4],[5,1],[6,5],[6,4],[3,6]]
n = 7
graph = np.zeros((7,7))

for i in range(len(edges)):
    edge = edges[i]
    v1 = edge[0]
    v2 = edge[1]
    graph[v1-1,v2-1] = 1

M = np.zeros((n,n))

for i in range(graph.shape[1]):
    row_sum = np.sum(graph[i, :])
    if row_sum == 0:
        M[i,:] = 1/n
    else:
        M[i,:] = graph[i, :]/row_sum

VP = np.ones(n)
alpha = 0.2
err = 1e-6

while err > 0:
    new_VP = np.matmul((alpha * (np.ones((n,n))/n) + (1-alpha)*M).T,VP)
    sum = np.sum(new_VP)
    new_VP = new_VP/sum

    temp = np.sum(np.abs(new_VP-VP))
    if temp < err:
        err = 0
    VP = new_VP

print(VP)
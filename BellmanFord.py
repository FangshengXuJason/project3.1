from typing import List
from math import log
from datetime import datetime

RATE_LIFETIME = 1.5

class BellmanFord:
    def __init__(self, currencies):
        self.currencies = currencies
        self.size = len(currencies)

    def negate_logarithm_convertor(self, graph) -> List[List[float]]:
        """ log of each rate in graph and negate it"""
        result = [[1 for c in range(self.size)] for r in range(self.size)]
        for row in range(self.size):
            for col in range(self.size):
                if self.expired(graph[row][col][1]):
                    result[row][col] = float("Inf")
                else:
                    result[row][col] = -log(graph[row][col][0])
        # for row in result:
        #     print(row)
        return result

    def expired(self, publish_time: datetime):
        duration = (datetime.utcnow() - publish_time).total_seconds()
        if duration >= RATE_LIFETIME:
            return True
        return False

    def arbitrage(self, rates_matrix):
        """ Calculates arbitrage situations and prints out the details of this calculations"""

        trans_graph = self.negate_logarithm_convertor(rates_matrix)
        source = 0
        n = len(trans_graph)
        min_dist = [float('inf')] * n

        pre = [-1] * n

        min_dist[source] = source

        # 'Relax edges |V-1| times'
        for _ in range(n - 1):
            for sourceCurr in range(n):
                for destCurr in range(n):
                    if min_dist[destCurr] > min_dist[sourceCurr] + trans_graph[sourceCurr][destCurr]:
                        min_dist[destCurr] = min_dist[sourceCurr] + trans_graph[sourceCurr][destCurr]
                        pre[destCurr] = sourceCurr

        # if we can still relax edges, then we have a negative cycle
        for sourceCurr in range(n):
            for destCurr in range(n):
                if min_dist[destCurr] > min_dist[sourceCurr] + trans_graph[sourceCurr][destCurr]:

                    print_cycle = [destCurr, sourceCurr]
                    while pre[sourceCurr] not in print_cycle:
                        # print("{1}\t-->\t\t{0}".format(self.currencies[pre[sourceCurr]], self.currencies[sourceCurr]))
                        print_cycle.append(pre[sourceCurr])
                        sourceCurr = pre[sourceCurr]

                    print_cycle.append(pre[sourceCurr])
                    print("Arbitrage Opportunity: ")
                    print(" --> ".join([self.currencies[p] for p in print_cycle[::-1]]))
                break  # stop searching for more negative cycle

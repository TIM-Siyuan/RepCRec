from collections import defaultdict
from src.model.Operation import Operation
from src.CustomizedConf import OperationType



class DeadLockDetector:

    def __init__(self, tm):
        self.tm = tm
        self.v_operation_dict = {}
        self.Trans_waitlist = {}

    def add_operation(self, operation):
        operation_type = operation.get_type()

        tid = operation.get_tid()
        if self.tm.transactions[tid].is_read_only():
            return

        vid = operation.get_vid()
        operation_list = self.v_operation_dict.get(vid, set())
        # Read operation
        if operation_type == OperationType.READ:
            for op in operation_list:
                if op.get_tid() == tid:
                    # Add operation to the dictionary
                    operation_list.add(operation)
                    self.v_operation_dict[vid] = operation_list
                    return

            for op in operation_list:
                if op.get_type() == OperationType.WRITE and op.get_tid() != tid:
                    waiting_ops = self.Trans_waitlist.get(tid, set())
                    waiting_ops.add(op.get_tid())
                    self.Trans_waitlist[tid] = waiting_ops
        # Write operation
        else:
            for op in operation_list:
                if op.get_tid() == tid and op.get_type == OperationType.WRITE:
                    operation_list.add(operation)
                    self.v_operation_dict[vid] = operation_list
                    return

            for op in operation_list:
                if op.get_tid() != tid and op.get_type() == OperationType.WRITE:
                    waiting_ops = self.Trans_waitlist.get(tid, set())
                    waiting_ops.add(op.get_tid())
                    self.Trans_waitlist[tid] = waiting_ops

        # Add operation to the dictionary
        s_operation_list = set(operation_list)
        s_operation_list.add(operation)
        self.v_operation_dict[vid] = s_operation_list

    #method get from geeksforgeeks.org to detect cycles
    def deadlock(self):
        graph = self.Trans_waitlist
        edge_list = []
        for key, values in graph.items():
            for value in values:
                edge_list.append([key, value])
        V = len(self.tm.transactions)
        G = Graph(V)
        for edge in edge_list:
            G.addEdge(edge[0], edge[1])
        if G.isCyclic() == 1:
            self.trace = self.getcycle()
            return True
        else:
            return False

    # Remove the aborted transaction from deadlockdetector
    def remove_transaction(self, tid):
        for v, op_list in self.v_operation_dict.items():
            new_op_list = []
            for op in op_list:
                if op.get_tid != tid:
                    new_op_list.append(op)
            self.v_operation_dict[v] = new_op_list
        self.Trans_waitlist.pop(tid, None)

    # get the tids in the cycle
    def getcycle(self):
        graph = self.Trans_waitlist
        edge_list = []
        for key, values in graph.items():
            for value in values:
                edge_list.append([key, value])
        v_set = set()
        start = []
        end = []
        for edge in edge_list:
            start.append(edge[0])
            end.append(edge[1])
            v_set.add(edge[0])
            v_set.add(edge[1])
        for i in range(len(v_set)):
            for v in end:
                if v not in start:
                    edge_list.remove(v)
        cycle = []
        for v in v_set:
            cycle.append(v)
        return cycle

#graph class get from geeksforgeeks.org to detect cycles
class Graph:
    def __init__(self, vertices):
        self.graph = defaultdict(list)
        self.V = vertices

    def addEdge(self, u, v):
        self.graph[u].append(v)

    def isCyclicUtil(self, v, visited, recStack):

        # Mark current node as visited and
        # adds to recursion stack
        visited[v] = True
        recStack[v] = True

        for neighbour in self.graph[v]:
            if visited[neighbour] == False:
                if self.isCyclicUtil(neighbour, visited, recStack) == True:
                    return True
            elif recStack[neighbour] == True:
                return True

        # The node needs to be poped from
        # recursion stack before function ends
        recStack[v] = False
        return False

    def isCyclic(self):
        visited = [False] * (self.V + 1)
        recStack = [False] * (self.V + 1)
        for node in range(self.V):
            if visited[node] == False:
                if self.isCyclicUtil(node, visited, recStack) == True:
                    return True
        return False

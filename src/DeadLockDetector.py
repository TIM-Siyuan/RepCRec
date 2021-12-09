from src.model.Operation import Operation
from src.CustomizedConf import OperationType
from collections import defaultdict


class DeadLockDetector:
    """
    A simple implementation of wait-for graph for deadlock detection
O
    :param self.tm: TransactionManager
    :param self.var_to_ops: A dictionary mapping variable id to a set of operations which want to access the variable
    :param self.wait_for: A dictionary tracking the wait-for edges, key is the from point, value is a set of transactions
    :param self.trace: A list contains a cycle in the wait-for graph if any circle exists
    """

    def __init__(self, tm):
        self.tm = tm
        # Key-Value pair, tracking the operations that access each variable, (variable id: set of operation)
        # if a variable id does not exist, then no transaction access this variable
        self.var_to_ops = {}

        # key value pair, tracking the wait-for-edges, (trans_id: set of transaction)
        self.wait_for = {}

        # a list to track the circle in current execution,
        # will be used to find the youngest transaction in the circle
        self.trace = []

    def add_operation(self, operation):
        """
        Add operation to self.var_to_ops dictionary, for example, if the operation want to access x1,
        we add the operation in this way self.var_to_ops["x1"].add(operation)
        Add new node in wait-for graph if the transactions does not exist
        ReadOnly operation will be ignored
        :param operation: Operation object
        :return: None
        """
        operation_type = operation.get_type()
        if not (operation_type == OperationType.WRITE or operation_type == OperationType.READ):
            return

        tid = operation.get_tid()
        if self.tm.transactions[tid].is_read_only():
            return

        vid = operation.get_vid()
        # All the operation on that vid
        operation_list = self.var_to_ops.get(vid, set())
        # Read Operation
        if operation_type == OperationType.READ:
            # Check if previous operation of the same transaction operated on the same variable
            # if so, no deadlock will be formed by adding this operation
            for op in operation_list:
                if op.get_tid() == tid:
                    # Add operation to the dictionary
                    operation_list.add(operation)
                    self.var_to_ops[vid] = operation_list
                    return

            # for any operation which is on the same variable,
            # if op is W and transaction id is different, then there should be a edge
            # For example, op is W(T1, x1, 10), the operation to be added is R(T2, x1)
            # then the edge is T2 -> T1
            for op in operation_list:
                if op.get_type() == OperationType.WRITE and op.get_tid() != tid:
                    waits = self.wait_for.get(tid, set())
                    waits.add(op.get_tid())
                    self.wait_for[tid] = waits
        # Case 2: operation is W
        else:
            # Check if previous operation of the same transaction operated on the same variable
            # if so, no deadlock will be formed by adding this operation
            for op in operation_list:
                if op.get_tid() == tid and op.get_type() == OperationType.WRITE:
                    # Add operation to the dictionary
                    operation_list.add(operation)
                    self.var_to_ops[vid] = operation_list
                    return

            # W operation will conflict with all other operation on the same variable
            for op in operation_list:
                if op.get_tid() != tid and op.get_type() == OperationType.WRITE:
                    waits = self.wait_for.get(tid, set())
                    waits.add(op.get_tid())
                    self.wait_for[tid] = waits

        # Add operation to the dictionary
        operation_list.add(operation)
        self.var_to_ops[vid] = operation_list




    def deadlock(self):
        graph = self.wait_for
        edge_list = []
        for key, values in graph.items():
            for value in values:
                edge_list.append([key, value])
        V = len(self.tm.transactions)
        G = Graph(V)
        for edge in edge_list:
            G.addEdge(edge[0], edge[1])
        if G.isCyclic() == 1:
            self.trace=self.getcycle()
            return True
        else:
            return False


    def remove_transaction(self, tid):
        """
        Remove wait-for node has the transaction_id, remove all operations belong to the transaction
        Typically, this function will be called when a transaction has been aborted or has committed
        :param tid: identifier of the transaction
        :return: None
        """
        # Modify var_to_trans
        for var, ops in self.var_to_ops.items():
            ops = {op for op in ops if op.get_tid() != tid}
            self.var_to_ops[var] = ops

        # Modify wait for graph, delete the node of given transaction id
        self.wait_for.pop(tid, None)

    def getcycle(self):
        graph = self.wait_for
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

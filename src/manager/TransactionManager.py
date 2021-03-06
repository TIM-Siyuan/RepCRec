from src.CustomizedConf import *
from src.DeadLockDetector import *
from src.model.Site import Site
from src.model.Transaction import Transaction
from src.model.Operation import Operation
from prettytable import PrettyTable


class TransactionManager:
    """
    The transaction manager distributes operation and hold the information of the entire simulation

    :param self.transactions: A dict to store running transactions
    :param self.wait_for_graph: A graph store the waiting sequence of operations to detect deadlock
    :param self.waiting_list: A waiting list contains all blocked operations
    :param self.waiting_trans: A waiting set store all waiting transactions
    :param self.sites: A list store all sites
    """

    def __init__(self):
        self.transactions = {}
        self.waiting_list = []
        self.waiting_trans = set()
        self.wait_for_graph = DeadLockDetector(self)
        self.sites = []

    def get_all_sites(self, sites):
        self.sites = sites

    def execute_operation(self, operations):
        """
        Process the new operation

        :param operations: new operation
        :return: None
        """
        # retry
        self.retry()
        is_succeed = self.assign_task(operations)
        if not is_succeed:
            self.waiting_list.append(operations)

        # detect deadlock and then abort the youngest transaction if deadlock happens
        if (OperationType.READ == operations.get_type()
            or OperationType.WRITE == operations.get_type()) \
                and self.wait_for_graph.deadlock():
            trans = self.find_youngest_trans(self.wait_for_graph.getcycle())[0]
            self.abort(trans)

    def retry(self):
        """
        retry blocked operations

        :return: None
        """
        blocked_list = []
        blocked_trans = set()
        for op in self.waiting_list:
            if not self.assign_task(op, True):
                blocked_list.append(op)
                if op.get_type() == OperationType.END and op.get_tid() not in blocked_trans:
                    continue
                blocked_trans.add(op.get_tid())

        self.waiting_list = blocked_list
        self.waiting_trans = blocked_trans

    def find_youngest_trans(self, cycle):
        """
        Find the youngest transaction

        :param cycle: The deadlock cycle
        :return: The youngest transaction
        """
        ages = [(tid, self.transactions[tid].time_stamp) for tid in cycle]
        return sorted(ages, key=lambda x: -x[1])[0]

    def abort(self, tid):
        """
        Abort the transaction
        :param tid: transaction_id
        :return: None
        """
        for site in self.sites:
            if site.get_status() == SiteStatus.UP:
                site.lock_manager.release_locks_by_trans(tid)
                site.data_manager.revert_trans_changes(tid)

        # Remove any blocked operation belongs to this transaction
        self.waiting_list = [op for op in self.waiting_list if op.get_tid != tid]
        # Remove transaction in wait for graph
        self.wait_for_graph.remove_transaction(tid)
        self.transactions.pop(tid)
        print(f"Transaction {tid} aborted")

    def assign_task(self, operation, is_retry=False):
        if OperationType.BEGIN == operation.get_type():
            return self.execute_begin(operation)
        elif OperationType.BEGINRO == operation.get_type():
            return self.execute_begin_read_only(operation)
        elif OperationType.WRITE == operation.get_type():
            return self.execute_write(operation, is_retry)
        elif OperationType.READ == operation.get_type():
            return self.execute_read(operation, is_retry)
        elif OperationType.RECOVER == operation.get_type():
            return self.execute_recover(operation)
        elif OperationType.FAIL == operation.get_type():
            return self.execute_fail(operation)
        elif OperationType.DUMP == operation.get_type():
            return self.execute_dump()
        elif OperationType.END == operation.get_type():
            return self.execute_end(operation, is_retry)

    def execute_begin(self, operation):
        """
        Initialize operation for manipulate

        :param operation: specific data from operations.
        :return: True or False
        """
        tid = operation.get_tid()
        time_stamp = operation.get_time()
        self.transactions[tid] = Transaction(tid, time_stamp)
        return True

    def execute_begin_read_only(self, operation):
        """
        Initialize the initial operation for read only

        :param operation: specific data from operations.
        :return: True or False
        """
        tid = operation.get_tid()

        time_stamp = operation.get_time()
        trans = Transaction(tid, time_stamp)
        trans.set_trans_type(TransactionType.RO)

        self.transactions[tid] = trans

        # take snapshot
        for site in self.sites:
            site.get_snapshot(time_stamp)
        return True

    def execute_read(self, operation, is_retry=False):
        """
        Execute the read operation, for both read and readonly

        :param operation: specific data from operations.
        :return: True or False
        """
        if not is_retry:
            self.save_to_transaction(operation)

        tid, vid = operation.get_tid(), operation.get_vid()
        headers = ["Transaction", "Site", "x" + str(vid)]
        # Case 1: read_only transaction
        if self.transactions[tid].transaction_type == TransactionType.RO:
            trans_time_stamp = self.transactions[tid].time_stamp
            # 1.1: If xi is not replicated and the site holding xi is up, then the read-only
            # transaction can read it. Because that is the only site that knows about xi.
            if vid % 2 == 1:
                site = self.sites[vid % num_sites]
                if site.status == SiteStatus.UP and vid in site.snapshots[trans_time_stamp]:
                    rows = [[tid, f"{site.sid}", f"{site.get_snapshot_variable(trans_time_stamp, vid)}"]]
                    print_table(headers, rows)
                    return True
                else:
                    print_result(["Transaction waits because of a site down"], [[tid]])
                    return False
            # 1.2: If xi is replicated then RO can read xi from site s if xi was committed
            # at s by some transaction T??? before RO began and s was up all the time
            # between the time when xi was commited and RO began. In that case
            # RO can read the version that T??? wrote. If there is no such site then
            # RO can abort.
            else:
                is_data_exist = False
                for site in self.sites:
                    # if the site has the variable is down, has -> True, we could retry latter
                    if site.status == SiteStatus.DOWN \
                            and trans_time_stamp in site.snapshots \
                            and vid in site.snapshots[trans_time_stamp]:
                        is_data_exist = True
                    elif trans_time_stamp in site.snapshots \
                            and vid in site.snapshots[trans_time_stamp]:
                        rows = [[tid, f"{site.sid}", f"{site.get_snapshot_variable(trans_time_stamp, vid)}"]]
                        print_table(headers, rows)
                        return True
                # No site has the variable in the snapshot
                if not is_data_exist:
                    self.abort(tid, AbortType.NO_DATA_FOR_READ_ONLY)
                    return True
        # Case 2: read-write transaction
        else:
            # 2.1 check specific site the index of variable read is odd (non-replicated)
            for site in self.generate_site_list(vid):
                if site.status == SiteStatus.UP \
                        and site.data_manager.is_available(vid) \
                        and site.lock_manager.acquire_read_lock(tid, vid):
                    return self.read_variable(tid, vid, site)
        return False

    def execute_write(self, operation, is_retry=False):
        """
        Execute Write operation

        :param operation: specific data from operations.
        :param retry: If the operation is a retry
        :return: True or False
        """
        if not is_retry:
            self.save_to_transaction(operation)

        tid, vid, value = operation.get_tid(), operation.get_vid(), operation.get_value()

        if vid % 2 != 0:
            # site list start from 0, so the index should minus 1, from vid%10+1 to vid%10
            site = self.sites[vid % num_sites]
            if site.status == SiteStatus.UP \
                    and site.lock_manager.acquire_write_lock(tid, vid):
                logs = site.data_manager.uncommitted_log.get(tid, {})
                logs[vid] = value
                site.data_manager.uncommitted_log[tid] = logs
                rows = [[f"{site.sid}"]]
                header = ["Sites affected by a write"]
                print_table(header, rows)
                return True
            else:
                print_table(["Transaction waits because of a site down"], [["T" + str(tid)]])
                return False
        else:
            locked_sites = []
            for site in self.sites:
                # check site status and append it to locked sites
                if site.status == SiteStatus.UP:
                    if site.lock_manager.acquire_write_lock(tid, vid):
                        locked_sites.append(site)
                        rows = [[f"{site.sid}"]]
                        header = ["Sites affected by a write"]
                        print_table(header, rows)

                    # release all locks added previously if it is conflicted
                    else:
                        for locked_site in locked_sites:
                            locked_site.lock_manager.release_lock(tid, vid)
                        print_table(["Transaction wait because of lock conflict"], [[tid]])
                        return False
            # Execute write operation
            if locked_sites:
                for locked_site in locked_sites:
                    logs = locked_site.data_manager.uncommitted_log.get(tid, {})
                    logs[vid] = value
                    locked_site.data_manager.uncommitted_log[tid] = logs
                return True

            # retry later if not available list now
            print_result(["Transaction waits because of a site down"], [["T" + str(tid)]])
            return False

    def execute_dump(self):
        rows = [site.print_all_sites() for site in self.sites]
        print_table(TABLE_HEADERS, rows)
        return True

    def execute_fail(self, operation):
        # get site_id
        site = self.sites[operation.get_sid() - 1]
        # get all trans in this site
        trans = site.lock_manager.get_all_transactions()
        for tid in trans:
            self.transactions[tid].set_trans_status(TransactionStatus.ABORTED)
        site.fail()
        return True

    def execute_recover(self, operation):
        site = self.sites[operation.get_sid() - 1]
        site.recover()
        return True

    def execute_end(self, operation, is_retry=False):
        if not is_retry:
            self.save_to_transaction(operation)

        tid = operation.get_tid()
        trans_start_time = self.transactions[tid].time_stamp

        # Two situation need to abort commit operation

        # 1. If a transaction T accesses an item (really accesses it, not just request
        # a lock) at a site and the site then fails, then T should continue to execute
        # and then abort only at its commit time (unless T is aborted earlier due to
        # deadlock).
        if self.transactions[tid].transaction_status == TransactionStatus.ABORTED:
            self.abort(tid)
            return True

        # 2. If there are blocked operation of the commit transaction, block the commit
        if tid in self.waiting_trans:
            return False
        # IOUtils.print_commit_result(tid)
        print(f"Transaction {tid} commit")
        # execute commit operation
        for site in self.sites:
            if site.status == SiteStatus.UP:
                if tid in site.data_manager.uncommitted_log:
                    logs = site.data_manager.uncommitted_log[tid]
                    for vid, val in logs.items():
                        site.data_manager.set_variable(vid, val)
                        site.data_manager.set_available(vid, True)
                    # committed, delete it from uncommitted_log
                    site.data_manager.uncommitted_log.pop(tid)
                # when readonly transaction end, delete the snapshot belongs to it
                elif trans_start_time in site.snapshots:
                    site.snapshots.pop(trans_start_time)
                # release all locks by this committed transaction
                site.lock_manager.release_locks_by_trans(tid)

        if tid in self.waiting_trans:
            self.transactions.pop(tid)
        # When transaction commit, we need to remove the transaction in the wait for graph
        self.wait_for_graph.remove_transaction(tid)

        return True

    def generate_site_list(self, vid):
        """
        check variable status and return site list
        :param vid: variable id
        :return: List
        """
        site_list = []

        if vid % 2 == 1:
            # non-replicated var find the only site
            site_list.append(self.sites[vid % num_sites])
        else:
            site_list.extend(self.sites)

        return site_list

    def save_to_transaction(self, operation):
        tid = operation.get_tid()
        if tid not in self.transactions:
            raise KeyError(f"Try to execute {operation.get_type()} in a non-existing transaction")

        self.transactions[tid].add_operation(operation)
        self.wait_for_graph.add_operation(operation)


    def read_variable(self, tid, vid, site):
        if tid in site.data_manager.uncommitted_log \
                and vid in site.data_manager.uncommitted_log[tid]:
            res = site.data_manager.uncommitted_log[tid][vid].get_value()
        else:
            res = site.data_manager.get_variable(vid).get_value()

        print("Read variable as follows:")
        print_table(["Transaction", "Site", f"x{vid}"], [[tid, f"{site.sid}", res]])

        return True


def print_table(headers, rows):
    """
    Print the query result using pretty table, only for dump operation now

    :param headers: table headers
    :param rows: table rows
    :return: None
    """
    table = PrettyTable()
    table.field_names = headers
    for row in rows:
        table.add_row(row)
    print(table)

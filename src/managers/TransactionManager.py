from src.models.enum import TransactionStatus, LockType, TransactionType
from src.models.Transaction import Transaction


class TransactionManager:
    def __init__(self, sites, transactions, data_locs, pending_list, waits_for, failure_history):
        self.sites = sites
        self.transactions = transactions
        self.data_locs = data_locs
        self.pending_list = pending_list
        self.waits_for = waits_for
        self.failure_history = failure_history

    def commit(self, transactionid, current_Time):
        transaction = self.transactions[transactionid]
        accessedSites = transaction.getAvailSites()

        if transaction.isReadOnly():
            transaction.setStatus(TransactionStatus.COMMITED)
            # outputprinter
            return True

        cancommit = True

        for key, value in accessedSites.items():
            site = self.sites[key]
            firstAccessTime = accessedSites[key]
            if hasFailureBetween(key, firstAccessTime, current_Time):
                cancommit = False
                break

        if not cancommit:
            return False

        updatedvalues = {}
        localcache = transaction.getCache()
        for key, value in localcache.items():
            site = self.sites[key]
            site.commit(transactionid, current_Time, updatedvalues)

        removeTransactionFromWaitsForGraph(transactionid)
        transaction.setStatus(TransactionStatus.COMMITED)
        return True

    def abort(self, transactionid):
        transaction = self.transactions[transactionid]
        accessedSites = transaction.getAvailSites()

        for key, value in accessedSites.items():
            site = self.sites[key]
            if site.isUP():
                site.abort(transactionid)

        removeTransactionFromWaitsForGraph(transactionid)
        transaction.setStatus(TransactionStatus.ABORTED)
        outputPrinter.printAbortSuccess(transactionid)

    def begin(self, operation):
        transactionid = operation.getTid()
        time = operation.getArrivingTime()
        transaction = Transaction(transactionid, time, TransactionType.RW, TransactionStatus.ACTIVE, None, None, None)
        self.transactions[transactionid] = transaction

    def beginRO(self, operation):
        transactionid = operation.getTid()
        time = operation.getArrivingTime()
        transaction = Transaction(transactionid, time, TransactionType.RO, TransactionStatus.ACTIVE, None, None, None)
        self.transactions[transactionid] = transaction

    # abort
    # begin
    # beginRO

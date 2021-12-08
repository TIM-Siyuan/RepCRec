class TransactionManager:
    def __init__(self, sites, transactions, data_locs, pending_list, waits_for, failure_history):
        self.sites = sites
        self.transactions = transactions
        self.data_locs = data_locs
        self.pending_list = pending_list
        self.waits_for = waits_for
        self.failure_history = failure_history


    # commit
    # abort
    # begin
    # beginRO

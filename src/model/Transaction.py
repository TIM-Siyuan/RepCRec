from src.CustomizedConf import TransactionStatus, TransactionType


class Transaction:
    """
       A class to represent transaction
    """
    def __init__(self, tid, time_stamp):
        self.tid = tid
        self.time_stamp = time_stamp
        self.transaction_type = TransactionType.RW
        self.transaction_status = TransactionStatus.ACTIVE
        self.operations = []

    # def __str__(self):
    #     return f"Identifier: {self.tid} & ReadOnly: {self.is_read_only} & " \
    #            f"Operations: {[str(op) for op in self.operations]}"

    def set_trans_type(self, trans_type):
        self.transaction_type = trans_type

    def set_trans_status(self, trans_status):
        self.transaction_status = trans_status


    # def add_operation(self, operation):
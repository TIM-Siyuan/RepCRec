from src.CustomizedConf import *
from src.model.DataCopy import DataCopy


class DataManager:
    """
        A class to manage data
    """
    def __init__(self, site_id):
        self.site_id = site_id
        self.uncommitted_log = {}
        self.data = {}
        for i in range(1, num_distinct_variables + 1):
            if i % 2 == NumType.EVEN:
                self.data[i] = DataCopy(DataType.REPLICATED, num_sites * i)
            if i % 2 == NumType.ODD and i % num_sites + 1 == self.site_id:
                self.data[i] = DataCopy(DataType.NONREPLICATED, num_sites * i)

    def set_variable(self, vid, val):
        self.data[vid] = val

    def set_available(self, vid, availability):
        self.data[vid].set_read_available(availability)

    def is_available(self, vid):
        return self.data[vid].is_read_available()

    def read(self, vid):
        return self.data[vid].get_latest_commit()

    def recover(self):
        """
        set variables readability (a read for a replicated variable x will not be allowed at a recovered site)

        :return: None
        """
        for key, value in self.data.items():
            if value.get_data_type() == DataType.NONREPLICATED:
                value.set_read_available(True)
            else:
                value.set_read_available(False)

    def fail(self):
        """
        set variables readability to False and clear all uncommitted log after a site failed

        :return: None
        """
        self.uncommitted_log = {}
        for key, value in self.data.items():
            value.set_read_available(False)

    def revert_trans_changes(self, tid):
        """
        Revert changes if transaction is going to be aborted

        :param tid
        :return: None
        """
        self.uncommitted_log.pop(tid, None)

    # def clear_uncommitted_changes(self):
    #     """
    #     Reset log to empty after a site fail
    #     :return: None
    #     """

    def get_variable(self, vid):
        """
        Read the value of given variable

        :param vid: variable id
        :return: value of the variable
        """
        return self.data[vid]
from src.CustomizedConf import *
from src.model.DataCopy import DataCopy


class DataManager:
    """
        A class to manage data
    """
    def __init__(self, site_id):
        self.site_id = site_id
        self.uncommitted_log = {}
        self.data = self.init_data(site_id)

    def set_variable(self, vid, val):
        self.data[vid].set_value(val)

    def set_available(self, vid, availability):
        self.data[vid].set_read_available(availability)

    def is_available(self, vid):
        return self.data[vid].is_read_available()

    def read(self, vid):
        return self.data[vid].get_latest_commit()

    def recover(self):
        """
        set variables read_ability (a read for a replicated variable x will not be allowed at a recovered site)

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

        :param transaction id
        :return: None
        """
        self.uncommitted_log.pop(tid, None)

    def get_variable(self, vid):
        """
        Read the value of given variable

        :param vid: variable id
        :return: data copy object
        """
        return self.data[vid]

    def init_data(self, site_id):
        data = {}
        for i in range(1, num_distinct_variables + 1):
            if i % 2 == 0:
                data[i] = DataCopy(DataType.REPLICATED, num_sites * i)
            if i % 2 == 1:
                if i % num_sites + 1 == site_id:
                    data[i] = DataCopy(DataType.NONREPLICATED, num_sites * i)
                else:
                    data[i] = DataCopy(DataType.NONREPLICATED, None)
        return data

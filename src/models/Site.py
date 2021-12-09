from src.manager.LockManager import LockManager
from src.manager.DataManager import DataManager
from src.CustomizedConf import SiteStatus


class Site:
    def __init__(self, sid):
        self.sid = sid
        self.data_manager = DataManager(sid)
        self.lock_manager = LockManager()
        self.status = SiteStatus.UP
        self.snapshots = {}

    def get_status(self):
        return self.status

    def recover(self):
        """
        Change a site to UP and close the readability of all replicated variables

        :return: None
        """
        self.status = SiteStatus.UP
        self.data_manager.recover()

    def fail(self):
        """
        Change site status to DOWN and clear all uncommitted changes in this site

        :return: None
        """
        self.status = SiteStatus.DOWN
        self.lock_manager.fail()
        self.data_manager.fail()

    def get_snapshot(self, time_stamp):
        """
        For multi-version consistency, take snapshot of current data

        :param tick: time
        :return: None
        """
        available_data = {}
        for idx, d in enumerate(self.data_manager.data):
            if d and self.data_manager.is_accessible[idx]:
                available_data[idx + 1] = d

        self.snapshots[time_stamp] = deepcopy(available_data)

    def get_snapshot_variable(self, time_stamp, vid):
        """
        Query variable data from snapshot of given tick

        :param time_stamp: time
        :param vid: variable id
        :return: variable value
        """
        return self.snapshots[time_stamp][vid]

    def print_all_sites(self):
        prefix = f"Site {self.sid} ({SiteStatus.UP.name if self.status == SiteStatus.UP else SiteStatus.DOWN.name})"
        return [prefix] + [val for val in self.data_manager.data]

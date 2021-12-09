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
        self.data_manager.uncommitted_log = {}
        self.lock_manager.fail()
        # self.data_manager.fail()
        self.data_manager.disable_accessibility()

    def get_snapshot(self, time_stamp):
        """
        For multi-version consistency, take snapshot of current data

        :param time_stamp: time
        :return: None
        """
        new_data = {}
        for i, data in enumerate(self.data_manager.data.values()):
            if data and self.data_manager.is_available(i + 1):
                new_data[i + 1] = data.get_value()

        self.snapshots[time_stamp] = deepcopy(new_data)

    def get_snapshot_variable(self, time_stamp, vid):
        """
        Query variable data from snapshot of given tick

        :param time_stamp: time
        :param vid: variable id
        :return: variable value
        """
        return self.snapshots[time_stamp][vid]

    def print_all_sites(self):
        col_title = f"Site {self.sid} ({SiteStatus.UP.name if self.status == SiteStatus.UP else SiteStatus.DOWN.name})"
        return [col_title] + [val.get_value() for val in self.data_manager.data.values()]


def deepcopy(obj):
    if isinstance(obj, dict):
        return {k: deepcopy(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [deepcopy(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple([deepcopy(item) for item in obj])
    else:
        return obj

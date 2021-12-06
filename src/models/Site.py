from src.managers import DataManager
from src.managers import LockManager
from src.models import site_status

class Site(object):
    def __init__(self, site_id):
        self.site_id = site_id
        self.data_manager = DataManager(site_id)
        self.lock_manager = LockManager()
        self.STATUS = site_status.UP

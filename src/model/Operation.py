import re

class Operation:
    def __init__(self, operation_type, tid, vid, value, sid, time):
        self.operation_type = operation_type
        self.tid = tid
        self.vid = vid
        self.value = value
        self.sid = sid
        self.time = time

    def get_type(self):
        return self.operation_type

    def get_tid(self):
        return self.tid

    def get_vid(self):
        return self.vid

    def get_sid(self):
        return self.sid

    def get_value(self):
        return self.value

    def get_time(self):
        return self.time

    def set_time(self, time):
        self.time = time
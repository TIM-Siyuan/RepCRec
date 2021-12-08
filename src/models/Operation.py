class Operation(object):

    def __init__(self, operation_type, tid, vid, value, sid, time):
        self.type = operation_type
        self.tid = tid
        self.vid = vid
        self.value = value
        self.sid = sid
        self.time = time


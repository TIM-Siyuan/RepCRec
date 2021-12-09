class operation(object):
    def __init__(self, operation_type, tid, vid, value, sid, time):
        self.type = operation_type
        self.tid = tid
        self.vid = vid
        self.value = value
        self.sid = sid
        self.time = time

    def getType(self):
        return self.type

    def getTid(self):
        return self.tid

    def getvid(self):
        return self.vid

    def getsid(self):
        return self.sid

    def getValuetoWrite(self):
        return self.value

    def getArrivingTime(self):
        return self.time

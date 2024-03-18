from collections import defaultdict
import threading  
class Locking:

    def __init__(self):
        self.lock = threading.Lock()
        self.reads = 0
        self.writes = False

    def get_locking(self):
        self.ready_lock.acquire()
        if self.readers > 0 or self.writers:
            self.ready_lock.release()
            return False
        self.writers = True
        self.ready_lock.release()
        return True

    def release_lock(self):
        self.ready_lock.acquire()
        self.writers = False
        self.ready_lock.release()
        return True

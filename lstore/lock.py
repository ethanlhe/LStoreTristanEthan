from collections import defaultdict
import threading  
class Locking:
    def __init__(self):
        self.lock = threading.Lock()
        self.active_readers = 0
        self.is_writer_active = False
        
    def acquire_read_lock(self):
        self.lock.acquire()
        if self.is_writer_active:
            self.lock.release()
            return False
        else:
            self.active_readers += 1
            self.lock.release()
            return True

    def acquire_write_lock(self):
        self.lock.acquire()
        if self.active_readers > 0 or self.is_writer_active:
            self.lock.release()
            return False
        else:
            self.is_writer_active = True
            self.lock.release()
            return True

    def release_read_lock(self):
        self.lock.acquire()
        self.active_readers -= 1
        self.lock.release()

    def release_write_lock(self):
        self.lock.acquire()
        self.is_writer_active = False
        self.lock.release()

from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import Locking

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.locks = {}  # Maps re

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))
        record_id = args[0]
        if record_id not in self.locks:
            self.locks[record_id] = Locking()


    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            record_id = args[0]  # Assuming the first argument is the record ID
            if record_id not in self.locks:
                self.locks[record_id] = Locking()
            lock = self.locks[record_id]
            # Acquire the appropriate lock
            if query.__name__ == "select":
                if not lock.acquire_read_lock():
                    return self.abort()
            else:
                if not lock.acquire_write_lock():
                    return self.abort()
            # Execute the query
            result = query(*args)
            if result == False:
                return self.abort()
        return self.commit()

    def abort(self):
        # Release all locks held by this transaction
        for record_id, lock in self.locks.items():
            if lock.is_writer_active:
                lock.release_write_lock()
            else:
                lock.release_read_lock()
        # Additional logic to undo changes can be added here
        return False



    def commit(self):
        for record_id, lock in self.locks.items():
            if lock.is_writer_active:
                lock.release_write_lock()
            else:
                lock.release_read_lock()
        return True

    

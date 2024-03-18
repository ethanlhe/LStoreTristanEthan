from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import Locking

class Transaction:
    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.target_table = None
        self.shared_locks_held = set()
        self.exclusive_locks_held = set()
        self.new_locks_created = set()

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        if not self.table:
            self.target_table = table


    def run(self):
        for arguments in self.queries:
            key = arguments[0]  # Presume the first argument is the record key
            if key not in self.target_table.lock_manager:
                self.target_table.lock_manager[key] = Locking()
                self.new_locks_created.add(key)
            
            if key not in self.exclusive_locks_held and key not in self.new_locks_created:
                lock_manager = self.target_table.lock_manager[key]
                if lock_manager.acquire_write_lock():
                    self.exclusive_locks_held.add(key)
                else:
                    return self.rollback()
    

    def abort(self):
        # Release all locks held by this transaction
        for record_id in self.locks_held:
            self.target_table.lock_manager.release_lock(self)
            # TODO Implement release lock, table methods
        self.locks_held = []
  
        return False
    

    def commit(self):
        for record_id in self.locks_held:
            self.table.lock_manager.release_lock(record_id, self)
        self.locks_held = []
        print("Transaction committed")

        return True
    

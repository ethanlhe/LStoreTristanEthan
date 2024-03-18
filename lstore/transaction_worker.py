from lstore.table import Table, Record
from lstore.index import Index
from lstore.locks import Locking
import threading

class TransactionWorker(threading.Thread):

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions):
        self.thread = None
        threading.Thread.__init__(self)
        self.transactions = transactions
        self.locks = {}
    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()
    

    """
    Waits for the worker to finish
    """
    def join(self):
        self.thread.join()


    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))


# Variables need to have commits and uncommitted versions
# Data mgrs need to have a variable "mode": normal, failed or recovered

import lock_table
from collections import deque

class data_mgr:
    def __init__(self, idx) -> None:
        # TODO: Figure out enumerations in Python, use for node modes
        self.mode = "normal"
        self.idx = idx

        # Initializing variables at site
        self.database = {}
        self.commited_after_recovery = deque()
        for var_idx in range(1, 21, 1):
            var = "x"+str(var_idx)
            if(var_idx%2 == 0):
                self.database[var] = deque([{
                    "commit_time": 0,
                    "value": var_idx * 10
                }])
            elif((var_idx%10)+1 == self.idx):
                self.database[var] = deque([{
                    "commit_time": 0,
                    "value": var_idx * 10
                }])
        
        self.lock_table = lock_table.lock_table(self.database)
        self.memory = {}
        self.failures = deque()

    def init_commited_after_recovery(self):
        self.commited_after_recovery = []

        if(self.idx%2 == 0):
            vars = [("x" + str(self.idx-1)),("x" + str(self.idx+9))]
            for var in vars:
                self.commited_after_recovery.append(var)

    def fail(self, time):
        self.failures.appendleft({
            "start": time
        })
        self.mode = "failed"
        self.memory = {}
        self.corrected_vars = deque()
        self.lock_table = lock_table.lock_table(self.database)
    
    def recover(self, time):
        self.failures[0]["end"] = time
        self.mode = "recovery"

    def write_var(self, trans, var, val, time):
        if(trans not in self.memory):
            self.memory[trans] = {}

        self.memory[trans][var] = {
            "op": "write",
            "val": val,
            "time": time
        }

        print("DM", self.idx, "writing", trans, var, str(self.memory[trans][var]))

    def read_latest_commit(self, var, time):
        commits = self.database[var]
        for commit in commits: # Go through commits in decreasing time order till you find latest preceding
            if(commit["commit_time"] <= time):
                print("DM", self.idx,  "reading", commit["value"])
                return commit

    def view_mem_val(self, trans, var):
        if(trans not in self.memory):
            print("mgr", self.idx, "trans", trans, "not in memory")
        else:
            if(var not in self.memory[trans]):
                print("mgr", self.idx, "var", var, "not in trans", trans, "memory")
            return self.memory[trans][var]

    def view_database(self):
        return self.database

    def view_lock_table(self):
        return self.lock_table
    
    def view_lock(self, var):
        return self.lock_table.view_lock(var)

    def has_lock(self, trans, var, lock):
        return self.lock_table.has_lock(trans, var, lock)

    def test_lock_var(self, trans, var, lock, to_lock = False):
        result = self.lock_table.test_lock_var(trans, var, lock, to_lock)
        return result

    def release_all_locks(self, trans):
        self.lock_table.unlock_all_vars(trans)

    def free_memory(self, trans):
        self.memory[trans] = {}
        
    def unlock_var(self, trans, var):
        result = self.lock_table.unlock_var(trans, var)
        return result

    def __str__(self) -> str:
        return "id: " + str(self.idx) + "\nmode: " + self.mode + "\ndatabase" + str(self.database) + "\n" + str(self.lock_table)

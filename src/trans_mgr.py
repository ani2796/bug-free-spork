import re
import data_mgr
import wf_graph
from collections import deque

class trans_mgr:
    def __init__(self):
        print("TM: Creating...")
        self.time = 0
        self.trans_set = {}
        self.vars = {}

        self.data_mgrs = [None] * 11        
        for mgr_idx in range(1, 11, 1):
            self.data_mgrs[mgr_idx] = data_mgr.data_mgr(mgr_idx)

        for var_idx in range(1, 21, 1):
            self.vars["x" + str(var_idx)] = {
                "queue": deque(),
                "curr": {}
            }

        # TODO: Initialize necessary data structures: wf graph

    def is_at_mgr(self, var, mgr_idx):
        var_idx = int(var[1:])

        
        # Even variables all sites, odd variables one site = (1 + idx) % 10
        if((var_idx % 2 == 0) or ((1 + var_idx) % 10 == mgr_idx)):
            return True
        return False

    def print_trans(self):
        print("TM: Printing transaction set...")
        print(self.trans_set)

    def commit_validation(self):
        print("Transaction Manager: Committing...")

    def write(self, trans, var, val):
        mgrs = self.data_mgrs
        for mgr_idx in range(1, 11, 1):
            if(self.is_at_mgr(var, mgr_idx) and not mgrs[mgr_idx].mode == "failed"):
                mgrs[mgr_idx].write_var(var, val, self.time)

    def operate(self, op):
        print("TM: Processing Operation...", op)
        self.time += 1

        # Incoming transaction is a "begin"
        if(op["cmd"] == "begin" or op["cmd"] == "beginRO"):
            # Add new transaction to trans_mgr's list
            new_trans = op["args"][0]
            self.trans_set[new_trans] = {
                "entry_time": self.time,
                "ro?": False if(op["cmd"] == "begin") else True,
                "ops": deque(),
                "locks": {}
            }
            print("TM: New transaction = ", self.trans_set[new_trans])

        elif(op["cmd"] == "W"):
            # TODO: If transaction already has lock, then write to all sites
            # TODO: Check if transaction can obtain write locks on all `up` sites
            # TODO: If so, obtain all write locks and write all sites
            # TODO: If not, wait
            trans = op["args"][0]
            var = op["args"][1]
            val = op["args"][2]

            self.trans_set[trans]["ops"].appendleft({
                "time": self.time,
                "trans": trans,
                "var": var,
                "val": val,
                "op": "write"
            })

            has_lock = False
            # Even if a single site has recorded the write lock, we can assume all site have also
            mgrs = self.data_mgrs
            for mgr_idx in range(1, 11, 1):
                if((not mgrs[mgr_idx] == "failed") and # Site up
                    self.is_at_mgr(var, mgr_idx) and # Site contains variable
                    mgrs[mgr_idx].has_lock(trans, var, "write")): # Variable has lock at site
                    has_lock = True
                    break

            if(has_lock):
                print("TM: Transaction already has W lock")
                self.write(trans, var, val)

            can_lock = True
            at_least_one_site = False

            for mgr_idx in range(1, 11, 1):
                if((not mgrs[mgr_idx].mode == "failed") and # Site up
                    self.is_at_mgr(var, mgr_idx)): # Site contains variable
                    can_lock = can_lock and mgrs[mgr_idx].test_lock_var(trans, var, "write", to_lock = False)
                    at_least_one_site = True

            print("can_lock:", can_lock, "at_least_one_site:", at_least_one_site)
            if(can_lock and at_least_one_site):
                print("TM: Transaction can and will lock at all available sites (there is at least one)")
                for mgr_idx in range(1, 11, 1):
                    if((not mgrs[mgr_idx] == "failed") and # Site up
                        self.is_at_mgr(var, mgr_idx)): # Site contains variable
                        mgrs[mgr_idx].test_lock_var(trans, var, "write", to_lock = True)
                    print(mgr_idx, mgrs[mgr_idx].lock_table)
                self.write(trans, var, val)
            else:
                self.vars[var]["queue"].append({
                    "trans": trans,
                    "lock": "write",
                    "time": self.time
                })
                print("TM: Variable not available at any site or cannot lock, adding to queue", var, self.vars[var]["queue"])
                print("TM: Adding var to waits-for graph")
                
        elif(op["cmd"] == "R"):
            trans = op["args"][0]
            var = op["args"][1]
            val = op["args"][2]
            mgrs = self.data_mgrs

            self.trans_set[trans]["ops"].appendleft({
                "time": self.time,
                "trans": trans,
                "var": var,
                "val": val,
                "op": "read"
            })

            if(not self.trans_set[trans]["ro?"]):
                if(not self.vars[var]["queue"]): # No transactions is queue for item
                    for mgr_idx in range(1, 11, 1):
                        if((not mgrs[mgr_idx] == "failed") and # Site up
                            self.is_at_mgr(var, mgrs[mgr_idx])): # Site contains variable
                            can_lock = mgrs[mgr_idx].test_lock_var(trans, var, "read", to_lock = False)
                            if(can_lock):
                                break
                else: # Transactions already in queue for item, so add this one too
                    self.vars[var]["queue"].append({
                        "trans": trans,
                        "lock": "read",
                        "time": self.time
                    })

                if(can_lock): # If locking possible, obtain read lock
                    mgrs[mgr_idx].test_lock_var(trans, var, "read", to_lock = True)
            else: # Transaction is RO, implement correct conditions
                pass


                        
            # TODO: Check if transaction is RO
            # TODO: If so, then refer (1)
            # TODO: Otherwise, refer (2)
            # TODO: (1) If variable Xi is not replicated, then read if site is up
            # TODO: (1) --- Find out what happens if Xi is not replicated and site is down ---
            # TODO: (1) If variable Xi is replicated, check sites in order. At site S, check: 
            # TODO:     (a) If Xi was committed at S by some T' before RO began
            # TODO:     (b) If S was up from Xi's commit by T' to RO start
            # TODO: (1) If the above conditions are met, read the version T' wrote
            # TODO: (1) If there is no S, then abort
            # TODO: (2) Check if transaction can obtain read locks on any `up` site
            # TODO: (2) If so, obtain lock at that site, obtain last committed value
            pass

        elif(op["cmd"] == "end"):
            # TODO: Perform commit time validation
            # TODO: If valid, commit
            # TODO: Else, abort
            pass
        
        elif(op["cmd"] == "dump"):
            for mgr_idx in range(1, 11, 1):
                print(mgr_idx, self.data_mgrs[mgr_idx])




# Create operation objects from groups
def make_operation(groups):
    return {
        "cmd": groups[0],
        "args": groups[1].split(',')
    }

if __name__ == "__main__":
    # Reading transactions from file
    trans_set_file = open("trans-sets/set-1", "r")
    tm = trans_mgr()

    # Regexes for comments, operations
    comment_regex = re.compile(r'( )*#(.)*')
    operation_regex = re.compile(r'(.+)\((.*)\)')

    # Getting all lines
    lines = trans_set_file.read().splitlines()

    # Grouping up
    for line in lines:
        if(comment_regex.match(line)):
            continue
        elif(groups := operation_regex.match(line).groups()):
            tm.operate(make_operation(groups))

    
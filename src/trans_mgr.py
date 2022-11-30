import re
import data_mgr
import wf_graph
from collections import deque

class trans_mgr:
    def __init__(self):
        print("TM: Creating...")
        self.time = 0
        self.trans_set = {}
        self.var_qs = {}
        self.possible_cycle = False
        self.attempt_qd = False

        self.data_mgrs = [None] * 11        
        for mgr_idx in range(1, 11, 1):
            self.data_mgrs[mgr_idx] = data_mgr.data_mgr(mgr_idx)

        for var_idx in range(1, 21, 1):
            self.var_qs["x" + str(var_idx)] = deque()

        self.wf_graph = wf_graph.wf_graph()

    def get_wf_graph(self):
        return self.wf_graph

    def is_at_mgr(self, var, mgr_idx):
        # ("var + \"" + var + "\"")
        var_idx = int(var[1:]) 
        
        # Even variables all sites, odd variables one site = 1 + (idx % 10)
        # ("TM: var_idx", str(1 + (var_idx % 10), "mgr_idx", mgr_idx)
        if((var_idx % 2 == 0) or ((1 + (var_idx % 10)) == mgr_idx)):
            return True
        return False

    def print_trans(self):
        print("TM: Printing transaction set", self.trans_set)

    def get_all_up_sites(self):
        up_sites = []
        for mgr_idx in range(1, 11, 1):
            mgr = self.data_mgrs[mgr_idx]
            if(mgr.mode == "normal" or mgr.mode == "recovery"):
                up_sites.append(mgr.idx)
        return up_sites

    def write(self, trans, var, val):
        mgrs = self.data_mgrs
        for mgr_idx in range(1, 11, 1):
            if(self.is_at_mgr(var, mgr_idx) and not mgrs[mgr_idx].mode == "failed"):
                mgrs[mgr_idx].write_var(trans, var, val, self.time)

    def ro_mgr_fail_check(self, mgr_idx, entry_time, var):
        mgr = self.data_mgrs[mgr_idx]
        last_commit_time = mgr.read_latest_commit(var, entry_time)["commit_time"]
        # ("TM: last commit time =", last_commit_time)

        for failure in mgr.failures:
            # ("TM:", mgr_idx, "failure", str(failure))
            if(failure["start"] > last_commit_time and failure["start"] < entry_time):
                return False
            if(("end" in failure) and (failure["end"] > last_commit_time and failure["end"] < entry_time)):
                return False
        return True

    def add_to_q(self, trans, var, lock, val = None):
        self.var_qs[var].appendleft({
            "trans": trans,
            "lock": lock,
            "time": self.time
        })
        if(val and lock == "write"):
            self.var_qs[var][0]["val"] = val

    def add_op(self, trans, var, op, mgrs, val = None):
        self.trans_set[trans]["ops"].appendleft({
            "time": self.time,
            "var": var,
            "op": op,
            "mgrs": mgrs
        })

        if(op == "write"):
            self.trans_set[trans]["ops"][0]["val"] = val

    def add_to_wf_graph(self, var):
        new_item = self.var_qs[var][0]
        trans = new_item["trans"]
        graph_change = False

        print("TM:", var, "queue =", self.var_qs[var])
        print("TM: attempting to add", new_item, "to wf graph")

        if(new_item["lock"] == "write"):
            for item in self.var_qs[var]:
                if(not item["trans"] == trans):
                    graph_change = True
                    self.wf_graph.add_edge(new_item["trans"], item["trans"])
            for mgr_idx in range(1, 11, 1):
                mgr = self.data_mgrs[mgr_idx]
                if(mgr.mode == "normal" and self.is_at_mgr(var, mgr_idx) and (mgr.view_lock(var)["type"] == "read" or mgr.view_lock(var)["type"] == "write")):
                    for trans in mgr.view_lock(var)["trans"]:
                        graph_change = True
                        self.wf_graph.add_edge(new_item["trans"], trans)
        elif(new_item["lock"] == "read"):
            for item in self.var_qs[var]:
                if(item["lock"] == "write" and (not item["trans"] == trans)):
                    graph_change = True
                    self.wf_graph.add_edge(new_item["trans"], item["trans"])
            for mgr_idx in range(1, 11, 1):
                mgr = self.data_mgrs[mgr_idx]
                if(mgr.mode == "normal" and self.is_at_mgr(var, mgr_idx) and mgr.view_lock(var)["type"] == "write"):
                    for trans in mgr.view_lock(var)["trans"]:
                        graph_change = True
                        self.wf_graph.add_edge(new_item["trans"], trans)
        
        if(graph_change):
            print("graph changed with edge", self.wf_graph)
        else:
            print("graph unchanged", self.wf_graph)

    def get_age(self, trans):
        print("trans", trans)
        if(trans in self.trans_set):
            return self.time - self.trans_set[trans]["entry_time"]
        return -1


    def commit_validation(self, trans):
        print("Committing", trans)
        trans_info = self.trans_set[trans]
        for op in trans_info["ops"]:
            print("op", op)
            op_time = op["time"]
            op_mgr_idxs = op["mgrs"]
            for mgr_idx in op_mgr_idxs:
                mgr = self.data_mgrs[mgr_idx]
                for fail in mgr.failures:
                    if(fail["start"] > op_time):
                        self.abort_trans(trans)
                        return False

        return True    


    def attempt_qd_ops(self):
        print("attempting queued operations")
        var_qs = self.var_qs
        for (var, q) in var_qs.items():
            print("var", var)
            if(len(q) == 0):
                continue
            else:
                for item in reversed(q):
                    print("item", item)
                    op = {
                        "cmd": "W" if(item["lock"] == "write") else "R",
                        "args": [item["trans"], var]
                    }
                    if(item["lock"] == "write"):
                        op["args"].append(item["val"])

                    print("queued op =", op)
                    if(self.operate(op, trial = True)):
                        self.operate(op)
                        self.var_qs[var].pop()
                    else:
                        break
                    
    def abort_trans(self, trans):
        print("TM: aborting", trans)
        self.wf_graph.remove_node(trans)
        print("TM: new wf graph", self.get_wf_graph())

        # Remove transaction from all queues if present
        for (var, q) in self.var_qs.items():
            del_idx = -1
            # ("TM: var", var, "queue =", q)
            for (idx, item) in enumerate(q):
                if(item["trans"] == trans):
                    del_idx = idx
                    break
            if(not del_idx == -1):
                # ("deleting", trans, "from", var, "q", self.var_qs[var])
                del self.var_qs[var][del_idx]
                # (self.var_qs[var])
                
        # Release all locks, release all instances in memory
        for mgr_idx in range(1, 11, 1):
            self.data_mgrs[mgr_idx].release_all_locks(trans)
            self.data_mgrs[mgr_idx].free_memory(trans)

        print("new queues", self.var_qs)
        self.attempt_qd_ops()

    def has_lock(self, trans, var, lock):
        mgrs = self.data_mgrs
        for mgr_idx in range(1, 11, 1):
            if((not mgrs[mgr_idx] == "failed") and # Site up
                self.is_at_mgr(var, mgr_idx) and # Site contains variable
                mgrs[mgr_idx].has_lock(trans, var, lock)): # Variable has lock at site
                return True
        return False

    def can_lock_all_and_at_least_one(self, trans, var):
        can_lock_all = True
        at_least_one = False
        mgrs = self.data_mgrs

        # Transaction doesn't already have lock, try to get lock
        for mgr_idx in range(1, 11, 1):
            if((mgrs[mgr_idx].mode == "normal" or mgrs[mgr_idx].mode == "recovery") # Site up
                and self.is_at_mgr(var, mgr_idx)): # Site contains variable
                # ("lock table", mgr_idx, mgrs[mgr_idx].lock_table)
                can_lock_all = can_lock_all and mgrs[mgr_idx].test_lock_var(trans, var, "write", to_lock = False)
                at_least_one = True
        return (can_lock_all, at_least_one)

    def w_lock_all_available_sites(self, trans, var):
        mgrs = self.data_mgrs
        print("TM: Transaction can and will lock at all available sites (and there is at least one)")

        for mgr_idx in range(1, 11, 1):
            if((mgrs[mgr_idx].mode == "normal") and # Site up
                self.is_at_mgr(var, mgr_idx)): # Site contains variable
                print("locking at", mgr_idx, self.data_mgrs[mgr_idx].test_lock_var(trans, var, "write", to_lock = True)) # Lock var

    def ro_transactions(self, trans, var, trial = False):
        var_idx = int(var[1:])
        entry_time = self.trans_set[trans]["entry_time"]
        print("TM: transaction is RO, fetching latest commit of", var)

        if(var_idx%2 == 1): # Not replicated, can always read this
            mgr_idx = (var_idx%10)+1
            if(self.data_mgrs[mgr_idx].mode == "failed"):
                if(trial):
                    return False
                self.add_to_q(trans, var, "RO")
            else:
                if(trial):
                    return True
                val = self.data_mgrs[mgr_idx].read_latest_commit(var, entry_time)
                print(trans, ": read", var, "=", val, "from", mgr_idx)

        else: # Replicated, check condition
            valid_mgrs = self.get_valid_ro_managers(var, entry_time)
            
            if(valid_mgrs):
                print("TM: RO", trans, "has at least one valid site to read from", str(valid_mgrs))
                mgr_to_read_from = None
                for mgr_idx in valid_mgrs:
                    if(self.data_mgrs[mgr_idx].mode == "normal"): # Found non-failed site
                        mgr_to_read_from = mgr_idx
                        print("TM: RO", trans, "can read", var, "from mgr", mgr_idx, "value = ", self.data_mgrs[mgr_idx].read_latest_commit(var, entry_time))
                        break
                
                if(mgr_to_read_from):
                    if(trial):
                        return True
                    print("trans", trans, "read", var, "=", self.data_mgrs[mgr_to_read_from].read_latest_commit(var, entry_time), "from", mgr_to_read_from)
                
                if(not mgr_to_read_from): # Cannot see up site, so add to var queue
                    print("TM: RO", trans, "cannot see up site, so add to queue")
                    if(trial):
                        return False
                    self.add_to_q(trans, var, "RO")
                    # No need to add to wf graph since no locks obtained
            else:
                print("aborting transaction", trans)
                self.abort_trans(trans)


    def get_valid_ro_managers(self, var, entry_time):
        valid_mgrs = []
        for mgr_idx in range(1, 11, 1):
            if(self.ro_mgr_fail_check(mgr_idx, entry_time, var)):
                valid_mgrs.append(mgr_idx)
        return valid_mgrs

    def operate(self, op, trial = False):
        print("TM: processing operation", op)
        self.time += 1

        # Possible cycle from previous operation
        if(self.possible_cycle and not trial):
            # ("TM: cycle possible")
            (transs, is_cycle) = self.wf_graph.cycle_check()
            # ("TM: is there?", is_cycle, "transactions", str(transs))
            if(is_cycle):
                # Abort youngest transaction
                transs_ages = list(map(lambda t: (t, self.get_age(t)), transs))
                # ("TM: trans_set", self.trans_set, "time", self.time)
                # ("TM: trans with ages", transs_ages)
                # ("TM: wf graph", self.get_wf_graph())
                youngest_trans = min(transs_ages, key = lambda t: t[1])
                # ("TM: youngest", youngest_trans)
                self.abort_trans(youngest_trans[0])
            self.possible_cycle = False

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
            self.wf_graph.add_node(new_trans)
            print("TM: new transaction", self.trans_set[new_trans])

        elif(op["cmd"] == "fail"):
            mgr_idx = int(op["args"][0])
            self.data_mgrs[mgr_idx].fail(self.time)
            print("TM: failed", mgr_idx, "updating mgr failures with", self.data_mgrs[mgr_idx].failures[0])

        elif(op["cmd"] == "recover"):
            mgr_idx = int(op["args"][0])
            self.data_mgrs[mgr_idx].recover(self.time)
            print("TM: recovered", mgr_idx, "updating mgr failures with", self.data_mgrs[mgr_idx].failures[0])

        elif(op["cmd"] == "W"):
            trans = op["args"][0]
            var = op["args"][1]
            val = op["args"][2]

            has_lock = self.has_lock(trans, var, "write")
            # Even if a single site has recorded the write lock, we can assume all site have also
            
            if(has_lock):
                # ("TM: Transaction already has W lock")
                self.add_op(trans, var, "write", self.get_all_up_sites(), val)
                self.write(trans, var, val)
                return
            
            (can_lock_all, at_least_one) = self.can_lock_all_and_at_least_one(trans, var)
            print("TM: can_lock_all =", can_lock_all, "at_least_one =", at_least_one)

            if(can_lock_all and at_least_one):
                print(trans, ": write", val, "to", var)
                if(trial):
                    return True
                self.w_lock_all_available_sites(trans, var)
                self.add_op(trans, var, "write", self.get_all_up_sites(), val) # Update transactions operations
                self.write(trans, var, val) # Writing var

            elif(can_lock_all and (not at_least_one)): # No sites up
                # Add to queue, wf graph
                if(trial):
                    return False
                self.add_to_q(trans, var, "write", val)
                self.add_to_wf_graph(var)
                self.possible_cycle = True

            elif((not can_lock_all)): # Conflicting lock
                if(trial):
                    return False
                self.add_to_q(trans, var, "write", val)
                self.add_to_wf_graph(var)
                self.possible_cycle = True

        elif(op["cmd"] == "R"):
            trans = op["args"][0]
            var = op["args"][1]
            var_idx = int(var[1:])

            if(self.trans_set[trans]["ro?"]):
                # Transaction is RO
                self.ro_transactions(trans, var, trial)

            else: # Transaction is R/W
                prev_lock = None
                mgr_with_lock = None

                for mgr_idx in range(1, 11, 1):
                    mgr = self.data_mgrs[mgr_idx]
                    if( (mgr.mode == "normal" or mgr.mode == "recovery") and 
                        self.is_at_mgr(var, mgr_idx) and 
                        mgr.has_lock(trans, var, "read")):
                        prev_lock = mgr.view_lock(var)
                        mgr_with_lock = mgr_idx
                        break
                
                if(mgr_with_lock): # Transaction already has lock, no need to try
                    print("TM:", trans, "already has R/W lock for", var, "at", mgr_with_lock)
                    if(prev_lock["type"] == "write"):
                        print(trans, "has W lock, must read", var, "from memory, value =", self.data_mgrs[mgr_with_lock].view_mem_val(trans, var))
                    else:
                        print(trans, "has R lock, must read", var, "from db, value =", self.data_mgrs[mgr_with_lock].read_latest_commit(trans, var))
                    self.add_op(trans, var, "read", [mgr_with_lock])
                    return
                
                # Try to obtain lock
                new_lock = None
                for mgr_idx in range(1, 11, 1):
                    mgr = self.data_mgrs[mgr_idx]
                    if((mgr.mode == "normal" or 
                        (mgr.mode == "recovery" and var in mgr.commited_after_recovery)) and # Site up
                        self.is_at_mgr(var, mgr_idx)): # Variable at site
                        if(self.data_mgrs[mgr_idx].test_lock_var(trans, var, "read", False)):
                            new_lock = mgr_idx
                            break
                
                if(new_lock): # Obtained lock at new_lock
                    print("TM: R locking", var, "at", new_lock, "reading value", val)
                    self.add_op(trans, var, "read", [new_lock])
                    if(trial):
                        return True
                    self.data_mgrs[mgr_idx].test_lock_var(trans, var, "read", True)
                    val = self.data_mgrs[mgr_idx].read_latest_commit(var, self.time)
                else:
                    print("TM: cannot R lock", var, "anywhere, adding to queue, wf graph")
                    if(trial):
                        return False
                    self.add_to_q(trans, var, "read")
                    self.add_to_wf_graph(var)

        elif(op["cmd"] == "end"):
            trans = op["args"][0]
            # TODO: Perform commit time validation
            # TODO: If valid, commit
            # TODO: Else, abort
            # TODO: 
            if(self.trans_set[trans]["ro?"]):
                print("no need any validation for RO transaction", trans)
            else:
                print("validating", trans, self.commit_validation(trans))

        elif(op["cmd"] == "dump"):
            for mgr_idx in range(1, 11, 1):
                mgr = self.data_mgrs[mgr_idx]
                print(mgr_idx, end=" ")
                for (var, q) in self.var_qs.items():
                    if(self.is_at_mgr(var, mgr_idx)):
                        print(var, ":", mgr.read_latest_commit(var, self.time), end=" ")
                print()
                        


# Create operation objects from groups
def make_operation(groups):
    op = {
        "cmd": groups[0],
        "args": list(map(lambda s: s.strip(), groups[1].split(',')))
    }
    # ("TM: op", op)
    return op

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

    
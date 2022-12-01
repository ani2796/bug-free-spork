import copy
import data_mgr
import wf_graph
from collections import deque

class trans_mgr:
    def __init__(self, out_file):
        # Initializing transaction manager
        self.time = 0
        self.trans_set = {}
        self.var_qs = {}
        self.possible_cycle = False
        self.out = out_file

        self.data_mgrs = [None] * 11        
        for mgr_idx in range(1, 11, 1):
            self.data_mgrs[mgr_idx] = data_mgr.data_mgr(mgr_idx)

        for var_idx in range(1, 21, 1):
            self.var_qs["x" + str(var_idx)] = deque()

        self.wf_graph = wf_graph.wf_graph()

    # Destructor (closing output file)
    def __del__(self):
        self.out.close()

    # Tells if `var` is at `mgr_idx`
    def is_at_mgr(self, var, mgr_idx):
        # ("is at mgr?", var, mgr_idx)
        var_idx = int(var[1:]) 
        # Even variables all sites, odd variables one site = 1 + (idx % 10)
        if((var_idx % 2 == 0) or ((1 + (var_idx % 10)) == mgr_idx)):
            return True
        return False

    # Return all sites in "normal" or "recovery" mode
    def get_all_up_sites(self):
        print("fetching all up sites")
        up_sites = []
        for mgr_idx in range(1, 11, 1):
            mgr = self.data_mgrs[mgr_idx]
            if(mgr.mode == "normal" or mgr.mode == "recovery"):
                up_sites.append(mgr.idx)
        return up_sites

    # Record `trans` writing `var` with `val` to all up sites
    def write(self, trans, var, val):
        print("writing", trans, var, val)
        self.out.write(trans + " writes " + var + " = " + str(val) + " to [")
        for mgr_idx in range(1, 11, 1):
            mgr = self.data_mgrs[mgr_idx]
            if(self.is_at_mgr(var, mgr_idx) and not mgr.mode == "failed"):
                mgr.write_var(trans, var, val, self.time)
                self.out.write(" " + str(mgr_idx))
        self.out.write(" ]\n")

    # Add `trans` to `var` queue for a `lock` (with `val` if write)
    def add_to_q(self, trans, var, lock, val = None):
        print("adding to queue", trans, var, lock, val)
        self.var_qs[var].appendleft({
            "trans": trans,
            "lock": lock,
            "time": self.time
        })
        if(val and lock == "write"):
            self.var_qs[var][0]["val"] = val

    # Record `op` on `var` by `trans` recording changes at `mgrs` (with `val` if write)
    def add_op(self, trans, var, op, mgrs, val = None):
        print("adding op", trans, var, op, mgrs, val)
        self.trans_set[trans]["ops"].appendleft({
            "time": self.time,
            "var": var,
            "op": op,
            "mgrs": mgrs
        })

        if(op == "write"):
            self.trans_set[trans]["ops"][0]["val"] = val

    # `var` queue has new op at 0, add to wf graph if needed
    # NOTE: No need to check for duplicated edges or loops, handled at graph
    def add_to_wf_graph(self, var):
        new_item = self.var_qs[var][0]
        trans = new_item["trans"]

        # Add conflicting transactions in queue
        for item in self.var_qs[var]:
            if( (new_item["lock"] == "write" and (not item["lock"] == "RO")) or 
                (new_item["lock"] == "read" and item["lock"] == "write")):
                print("adding edge from", trans, "to", item["trans"])
                self.wf_graph.add_edge(trans, item["trans"])

        # Add conflicting transactions currently holding the lock
        for mgr_idx in range(1, 11, 1):
            mgr = self.data_mgrs[mgr_idx]
            if(not self.is_at_mgr(var, mgr_idx)):
                continue
            lock = mgr.view_lock(var)
            if(((lock["type"] and new_item["lock"] == "write") or 
                (lock["type"] == "write" and new_item["lock"] == "read"))):
                for mgr_trans in lock["trans"]:
                    print("adding edge from", trans, "to", mgr_trans)
                    self.wf_graph.add_edge(trans, mgr_trans)

    # Return age of `trans` wrt to current time
    def get_age(self, trans):
        print("getting age")
        if(trans in self.trans_set):
            return self.time - self.trans_set[trans]["entry_time"]
        return None

    # Ensure that sites at which ops happened haven't failed since
    def commit_validation(self, trans):
        print("performing commit validation", trans)
        ops = self.trans_set[trans]["ops"]
        print("ops", ops)
        for op in ops:
            for mgr_idx in op["mgrs"]:
                print("mgr", mgr_idx, "failures", self.data_mgrs[mgr_idx].failures)
                for fail in self.data_mgrs[mgr_idx].failures:
                    if(fail["start"] > op["time"]):
                        return False
        return True    

    # Attempt queued operations
    def attempt_qd_ops(self):
        print("attempting queued ops")
        for (var, q) in self.var_qs.items():
            if(len(q) == 0):
                continue
            temp_q = copy.copy(q)
            for item in reversed(temp_q):
                print("attempting item", item)
                op = {
                    "cmd": "W" if(item["lock"] == "write") else "R",
                    "args": [item["trans"], var],
                    "head_of_q": True
                }
                if(item["lock"] == "write"):
                    op["args"].append(item["val"])

                print("can it happen?", self.operate(op, trial = True))
                if(not self.operate(op, trial = True)):
                    break
                self.operate(op)
                self.var_qs[var].pop()
                    
    # Remove `trans` from all var waiting queues
    def remove_from_qs(self, trans):
        print("removing", trans, "from queues")
        for (var, q) in self.var_qs.items():
            del_idx = -1
            for (idx, item) in enumerate(q):
                if(item["trans"] == trans):
                    del_idx = idx
                    break
            if(not del_idx == -1):
                print("deleting", trans, "from", var, "queue")
                del self.var_qs[var][del_idx]
  
    # Abort transaction, clean up its remnants
    def abort_trans(self, trans):
        print("aborting", trans)
        self.trans_set[trans]["aborted"] = True
        self.out.write("abort " + str(trans) + "\n")
        
        # remove all transaction nodes from wf graph
        self.wf_graph.remove_node(trans)
        # remove transaction from all queues if present
        self.remove_from_qs(trans)

        # release all locks, release all instances in memory
        for mgr_idx in range(1, 11, 1):
            self.data_mgrs[mgr_idx].release_all_locks(trans)
            self.data_mgrs[mgr_idx].free_memory(trans)

        # attempt queued ops in case any critical resource got freed
        self.attempt_qd_ops()

    # See if `trans` has `lock` on `var`
    def has_lock(self, trans, var, lock):
        mgrs = self.data_mgrs
        for mgr_idx in range(1, 11, 1):
            if( self.is_at_mgr(var, mgr_idx) and # Site contains variable
                mgrs[mgr_idx].has_lock(trans, var, lock)): # Variable has lock at site
                return True
        return False

    # Check if trans is at the head of `var`s queue
    def trans_head_of_var_q(self, trans, var):
        q = self.var_qs[var]
        if(q[len(q)-1]["trans"] == trans):
            return True
        return False

    def q_check(self, trans, var):
        return (len(self.var_qs[var]) == 0 or self.trans_head_of_var_q(trans, var))


    # Check if `trans` can obtain write lock on `var` at all sites, also checks if there is at least one
    def can_lock_all_and_at_least_one(self, trans, var):
        at_least_one = False

        # Seeing if there is at least one up site
        for mgr_idx in range(1, 11, 1):
            mgr = self.data_mgrs[mgr_idx]
            if(self.is_at_mgr(var, mgr_idx) and (not mgr.mode == "failed")): # Site contains variable
                at_least_one = True
                break
        
        if(not at_least_one): # No up site available to W, so add to queue
            return (False, False)

        # Queue must be empty or `trans` must be at head of queue
        can_lock_all = True
        for mgr_idx in range(1, 11, 1):
            mgr = self.data_mgrs[mgr_idx]
            if(not self.is_at_mgr(var, mgr_idx)):
                continue
            if( (mgr.mode == "normal" and self.q_check(trans, var)) or 
                (mgr.mode == "recovery" and ((var not in mgr.commited_after_recovery or self.q_check(trans, var))))):
                can_lock_all = can_lock_all and mgr.test_lock_var(trans, var, "write", to_lock = False)
        
        return (can_lock_all, True)

    # `trans` locks `var` at all available sites
    def w_lock_all_available_sites(self, trans, var):
        print("locking at all available sites")
        mgrs = self.data_mgrs

        for mgr_idx in range(1, 11, 1):
            if((not mgrs[mgr_idx].mode == "failed") and # Site up
                self.is_at_mgr(var, mgr_idx)): # Site contains variable
                print("locking at", mgr_idx)
                self.data_mgrs[mgr_idx].test_lock_var(trans, var, "write", to_lock = True) # Lock var
    
    # Check if `mgr_idx` has failed from latest commit of `var` till `entry_time`
    def ro_mgr_fail_check(self, mgr_idx, entry_time, var):
        print("read only mgr fail check", mgr_idx, entry_time, var)
        mgr = self.data_mgrs[mgr_idx]
        last_commit_time = mgr.read_latest_commit(var, entry_time)["commit_time"]
        for failure in mgr.failures:
            if( (failure["start"] > last_commit_time and failure["start"] < entry_time) or
                (("end" in failure) and (failure["end"] > last_commit_time and failure["end"] < entry_time))):
                return False
        return True

    # Get list of mgrs which have valid `var` value at `entry_time`
    def get_valid_ro_managers(self, var, entry_time):
        valid_mgrs = []
        for mgr_idx in range(1, 11, 1):
            if(self.ro_mgr_fail_check(mgr_idx, entry_time, var)):
                valid_mgrs.append(mgr_idx)
        return valid_mgrs

    # Processing RO trans `trans` request for `var`
    # On "trials", don't change anything, just see if possible
    def ro_transactions(self, trans, var, trial = False):
        print("transaction is RO, fetching latest commit of", var)
        var_idx = int(var[1:])
        entry_time = self.trans_set[trans]["entry_time"]

        if(var_idx%2 == 1): # Not replicated, can always read this
            mgr_idx = (var_idx%10)+1
            if(self.data_mgrs[mgr_idx].mode == "failed"):
                if(trial):
                    return False
                self.out.write("RO " + trans + " waiting for unreplicated var " + var + " at failed site " + mgr_idx + "\n")
                self.add_to_q(trans, var, "RO")
            else:
                if(trial):
                    return True
                val = self.data_mgrs[mgr_idx].read_latest_commit(var, entry_time)
                self.out.write("RO " + trans + " reads " + var + " = " + str(val["value"]) + " from " + str(mgr_idx) + "\n")

        else: # Replicated, check condition
            valid_mgrs = self.get_valid_ro_managers(var, entry_time)
            
            if(valid_mgrs):
                print("RO", trans, "has at least one valid site to read from", str(valid_mgrs))
                mgr_to_read_from = None
                for mgr_idx in valid_mgrs:
                    mgr = self.data_mgrs[mgr_idx]
                    if(mgr.mode == "normal" or (mgr.mode == "recovery" and var in mgr.commited_after_recovery)): # Found non-failed site or valid recovering site
                        mgr_to_read_from = mgr_idx
                        break

                if(mgr_to_read_from):
                    print("RO", trans, "can read", var, "from mgr", mgr_idx, "value = ", self.data_mgrs[mgr_idx].read_latest_commit(var, entry_time))
                    if(trial):
                        return True
                    val = self.data_mgrs[mgr_to_read_from].read_latest_commit(var, entry_time)
                    self.out.write("RO " + trans + " reads " + var + " = " + str(val["value"]) + " from " + str(mgr_to_read_from) + "\n")
                else: # Cannot see up site, so add to var queue
                    if(trial):
                        return False
                    self.out.write("RO " + trans + " cannot read " + var + " because all mgrs are down, so waiting\n")
                    self.add_to_q(trans, var, "RO")
                    # No need to add to wf graph since no locks obtained
            else:
                self.out.write("RO " + trans + " cannot read " + var + " because all mgrs failed between last commit and trans start, so aborting\n")
                self.abort_trans(trans)

    def file_dump(self):
        for mgr_idx in range(1, 11, 1):
            mgr_db = self.data_mgrs[mgr_idx].view_database()
            self.out.write("site " + str(mgr_idx) + " - ")
            for var in mgr_db:
                val = self.data_mgrs[mgr_idx].read_latest_commit(var, self.time)["value"]
                # self.out.write("\t" + var + ": " + str(val) + "\t")
                self.out.write("\t%s: %4d\t" % (var, int(val)))
            self.out.write("\n")

    def operate(self, op, trial = False):
        print("\nTM: processing operation", op)
        self.time += 1

        # Possible cycle from previous operation
        if(self.possible_cycle and not trial):
            print("TM: cycle possible")
            (transs, is_cycle) = self.wf_graph.cycle_check()
            print("TM: is there?", is_cycle, "transactions", str(transs))
            if(is_cycle):
                # Abort youngest transaction
                transs_ages = list(map(lambda t: (t, self.get_age(t)), transs))
                youngest_trans = min(transs_ages, key = lambda t: t[1])
                print("youngest", youngest_trans)
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
                "locks": {},
                "aborted": False
            }
            self.wf_graph.add_node(new_trans)
            print("new transaction", self.trans_set[new_trans])

        # Simulating site failure
        elif(op["cmd"] == "fail"):
            mgr_idx = int(op["args"][0])
            self.data_mgrs[mgr_idx].fail(self.time)
            print("failed", mgr_idx, "updating mgr failures with start time", self.data_mgrs[mgr_idx].failures[0])

        # Simulating site recovery
        elif(op["cmd"] == "recover"):
            mgr_idx = int(op["args"][0])
            self.data_mgrs[mgr_idx].recover(self.time)
            # ("recovered", mgr_idx, "updating mgr failures with end time", self.data_mgrs[mgr_idx].failures[0])
            # ("recovered mgr locks:", self.data_mgrs[mgr_idx].lock_table)
            self.attempt_qd_ops()

        # Incoming transaction write
        elif(op["cmd"] == "W"):
            print("TM: write op", op)
            trans = op["args"][0]
            var = op["args"][1]
            val = op["args"][2]

            has_lock = self.has_lock(trans, var, "write")

            # Even if a single site has recorded the write lock, we can assume all site have also
            if(has_lock):
                print("transaction already has W lock")
                self.add_op(trans, var, "write", self.get_all_up_sites(), val)
                self.write(trans, var, val)
                return
            
            (can_lock_all, at_least_one) = self.can_lock_all_and_at_least_one(trans, var)
            print("can_lock_all =", can_lock_all, "at_least_one =", at_least_one)

            if(can_lock_all and at_least_one):
                print(trans, ": write", val, "to", var)
                if(trial):
                    return True
                self.w_lock_all_available_sites(trans, var)
                self.add_op(trans, var, "write", self.get_all_up_sites(), val) # Update transactions operations
                self.write(trans, var, val) # Writing var
            else:
                if(trial):
                    return False
                if(not at_least_one): # No sites up
                    print("Add to queue, wf graph")
                    self.out.write(trans + " cannot write " + var + " = " + val + " because no sites up, so waiting\n")
                elif(not can_lock_all): # Conflicting lock
                    print("Conflicting lock")
                    self.out.write(trans + " cannot write " + var + " = " + val + " because of conflicting lock, so waiting\n")
                    
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
                    print(trans, "already has R/W lock for", var, "at", mgr_with_lock)
                    val = None
                    if(prev_lock["type"] == "write"):
                        val = self.data_mgrs[mgr_with_lock].view_mem_val(trans, var)
                        print(trans, "has W lock, must read", var, "from memory, value =", val)
                    else:
                        val = self.data_mgrs[mgr_with_lock].read_latest_commit(var, self.time)
                        print(trans, "has R lock, must read", var, "from db, value =", val)
                    self.out.write(trans + " reads " + var + " = " + val + " from " + new_lock + "\n")
                    self.add_op(trans, var, "read", [mgr_with_lock])
                    return
                
                # Try to obtain lock
                new_lock = None
                for mgr_idx in range(1, 11, 1):
                    mgr = self.data_mgrs[mgr_idx]
                    if((mgr.mode == "normal" or (mgr.mode == "recovery" and var in mgr.commited_after_recovery)) and # Site up
                        self.is_at_mgr(var, mgr_idx)): # Variable at site
                        print("mgr", mgr_idx, "has var", var, "and is not failed")
                        if(mgr.test_lock_var(trans, var, "read", False)):
                            new_lock = mgr_idx
                            break
                
                # Obtained lock at new_lock
                if(new_lock): 
                    if(trial):
                        print("R locking", var, "at", new_lock)
                        return True
                    self.data_mgrs[new_lock].test_lock_var(trans, var, "read", True)
                    val = self.data_mgrs[new_lock].read_latest_commit(var, self.time)
                    print("R locking", var, "at", new_lock, "reading value", val)
                    self.out.write(trans + " reads " + var + " = " + str(val["value"]) + " from " + str(new_lock) + "\n")
                    self.add_op(trans, var, "read", [new_lock])
                else:
                    print("cannot R lock", var, "anywhere, adding to queue, wf graph")
                    if(trial):
                        return False
                    self.out.write(trans + " cannot read " + var + ", so waiting\n")
                    self.add_to_q(trans, var, "read")
                    self.add_to_wf_graph(var)

        elif(op["cmd"] == "end"):
            trans = op["args"][0]

            if(self.trans_set[trans]["aborted"]):
                print(trans, "already aborted, so no need commit validation/ending")
                return
            
            if(self.trans_set[trans]["ro?"]):
                print("no need any validation for RO transaction", trans)
                self.out.write("RO commit " + trans + "\n")
            else:
                # ("validating", trans, self.commit_validation(trans))
                if(self.commit_validation(trans)):
                    self.out.write("commit " + trans + "\n")
                    for mgr_idx in range(1, 11, 1):
                        mgr = self.data_mgrs[mgr_idx]
                        if(not mgr.mode == "failed"):
                            print("committing at site", mgr_idx)                    
                            mgr.commit(trans, self.time)
                            mgr.release_all_locks(trans)
                            mgr.free_memory(trans)

                    self.remove_from_qs(trans)
                    # Once locks are released and queues are cleared, attempt remaining queued ops
                    self.attempt_qd_ops()
                else:
                    self.abort_trans(trans)

        elif(op["cmd"] == "dump"):
            for mgr_idx in range(1, 11, 1):
                mgr = self.data_mgrs[mgr_idx]
                print(mgr_idx, end=" ")
                for (var, q) in self.var_qs.items():
                    if(self.is_at_mgr(var, mgr_idx)):
                        print(var, ":", mgr.read_latest_commit(var, self.time), end=" ")
                print()

            self.file_dump()
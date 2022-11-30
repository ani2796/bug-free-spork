from collections import deque

class lock_table:
    def __init__(self, variables) -> None:
        self.locks = {}
        for var in variables:
            # TODO: Replace strings for R/W with enumerations
            self.locks[str(var)] = {
                "trans": deque(),
                "type": None
            }
        pass

    def __str__(self) -> str:
        return str(self.locks)

    # To return locks on particular variable to requester
    def view_lock(self, var):
        return self.locks[var]

    def has_lock(self, trans, var, lock):
        # ("lock table: locks", self.locks)
        if(trans in self.locks[var]["trans"]):
            if(self.locks[var]["type"] == "write"):
                return True
            if(self.locks[var]["type"] == "read" and lock == "read"):
                return True
        return False

    def test_lock_var(self, trans, var, lock, to_lock):
        # Trans exclusively holds R/W lock
        if(len(self.locks[var]["trans"]) == 1 and (trans in self.locks[var]["trans"])):
            # Upgrading lock
            # "Variable already holds lock (might upgrade)"
            if(lock == "write" and to_lock):
                self.locks[var]["type"] = "write"
        # No existing lock
        elif((not self.locks[var]["trans"])):
            # "No transactions hold locks on variable"
            if(to_lock):
                self.locks[var]["type"] = lock
                self.locks[var]["trans"].append(trans)
        # Requesting shared read lock
        elif(lock == "read" and self.locks[var]["type"] == "read"):
            if(to_lock):
                self.locks[var]["trans"].append(trans)
        # Any situation where lock already there and >=1 of the locks is W
        else:
            return False
        return True

    def unlock_all_vars(self, del_trans):
        for (var, lock) in self.locks.items():
            if(del_trans in lock["trans"]):
                self.locks[var] = {
                "trans": deque(),
                "type": None
            }

    def unlock_var(self, trans, var):
        if(trans in self.locks[var]["trans"]):
            self.locks[var]["trans"].remove(trans)
            if(len(self.locks[var]["trans"]) == 0):
                self.locks[var]["type"] = None
            return True
        return False
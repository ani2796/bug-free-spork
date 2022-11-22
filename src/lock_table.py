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

    def lock_var(self, trans, var, lock):
        # Illegal variable
        if(var not in self.locks):
            return False
        # Transaction requesting lock it already holds
        if(len(self.locks[var]["trans"]) == 1 and (trans in self.locks[var]["trans"])):
            # Upgrading lock (or letting it be the same)
            if(lock == "write"):
                self.locks[var]["type"] = "write"
        # No existing lock
        elif(not self.locks[var]["type"]):
            self.locks[var]["type"] = lock
            self.locks[var]["trans"].append(trans)
        # Requesting shared read lock
        elif(lock == "read" and self.locks[var]["type"] == "read"):
            self.locks[var]["trans"].append(trans)
        # Any situation where lock already there and >=1 of the locks is W
        else:
            return False
        return True

    def unlock_var(self, trans, var):
        if(trans in self.locks[var]["trans"]):
            self.locks[var]["trans"].remove(trans)
            if(len(self.locks[var]["trans"]) == 0):
                self.locks[var]["type"] = None
            return True
        return False
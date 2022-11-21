import re

class trans_mgr:
    def __init__(self):
        print("Creating transaction manager")
        self.time = 0
        self.trans_set = {}
        # TODO: Initialize necessary data structures: wf graph, sites, time
        # TODO: Initialize knowledge of which variable is at which site
        # TODO: Maintain list of handled transactions
    
    def commit_validation(self):
        print("Committing transaction")

    def operate(self, op):
        # TODO: This will be the most complicated part -- dealing with incoming operations
        self.time += 1
        print(op)

        # Incoming transaction is a "begin"
        if(op["cmd"] == "begin"):

            # Add new transaction to trans_mgr's list
            new_trans = op["args"][0]
            self.trans_set[new_trans] = {
                "entry_time": self.time
            }
            print("Creating new transaction", self.trans_set[new_trans])
        
        elif(op["cmd"] == "beginRO"):
            # TODO: Read committed variables at begin time
            pass
        elif(op["cmd"] == "end"):
            # TODO: Perform commit time validation
            # TODO: If valid, commit
            # TODO: Else, abort
            pass
        
        elif(op["cmd"] == "dump"):
            # TODO: Print committed values at all sites in variable order
            pass

        elif(op["cmd"] == "W"):
            # TODO: Check if transaction can obtain write locks on all `up` sites
            # TODO: If so, write all available copies
            # TODO: If not, release all locks
            pass
        elif(op["cmd"] == "R"):
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
            # TODO: (2) If so, 
            pass


# Create operation objects from groups
def make_operation(groups):
    return {
        "cmd": groups[0],
        "args": groups[1].split(',')
    }

if __name__ == "__main__":
    # print("Hello, world.")
    
    # TODO: Read from transaction set file, call TM on each operation
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
import re

class trans_mgr:
    def __init__(self):
        print("Creating transaction manager")
        # TODO: Initialize necessary data structures: wf graph, sites
        # TODO: Initialize knowledge of which variable is at which site
    
    def commit_validation(self):
        print("Committing transaction")

    def operate(self, op):
        # TODO: This will be the most complicated part -- dealing with incoming operations
        print(op)


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
# Distributed Replicated DB with Concurrency and Recovery

# The project is named `bug-free-spork` because that's the random name
# GitHub suggested, and I'm nothing if not a risk taker.

# The project consists of 5 modules: 
# 1. The transaction manager
# 2. The data manager
# 3. The lock table
# 4. The waits-for graph
# 5. The simulator


# SIMULATOR NOTES
# The simulator reads instructions and executes the series of commands
# written in the file. Lines starting with `#` are ignored.
# If the optional arg `-f <f_name>` is included along with the relative path of a
# file, the file alone will be simulated, and the output will be at <f_name-out>.
# The functions have been explained in-line, a broad overview of the modules is below.

# TRANSACTION MANAGER (TM)
# The TM executes input commands according to the current state of the system.
# It has access to data managers (DMs) 1 - 10, each with a DB, memory, and a lock table.
# Variable accesses are translated to the available copies algorithm.
# Each transaction is strict 2 phase locked, it releases all locks only on termination.
# The TM also keeps track of operations in queue for each variable, and executes them in case the queue changes.

# DATA MANAGER (DM)
# The DMs keep track of committed variables, and variables in memory represent changes by uncommitted transactions.
# There are 3 modes for each DM: "normal", "failed" and "recovery".
# Recovery mode keeps track of variables that need to be committed before read access.
# Once fully recovered, the DM's mode becomes normal again.
# The DM keeps a lock table, which is accessed by the TM to perform locking, testing locks and unlocking.
# On failure, memory is erased (uncommitted transactions are lost) and the lock table is also deleted.

# LOCK TABLE
# The lock table mainly tests, locks and unlocks variables for the DM.

# THE WAITS-FOR GRAPH
# The waits-for graph is a non-looping graph that adds transactions as nodes.
# It stores edges as an adjacency list.
# Cycles are checked using a stack-based DFS traversal.



import trans_mgr
import re
import os
import argparse

# Create operation objects from groups
def make_operation(groups):
    op = {
        "cmd": groups[0],
        "args": list(map(lambda s: s.strip(), groups[1].split(',')))
    }
    # ("TM: op", op)
    return op

def parse_input(in_path, out_path):
    # New transaction manager
    out_file = open(out_path, "w")
    tm = trans_mgr.trans_mgr(out_file)

    # Regexes for comments, operations
    comment_regex = re.compile(r'( )*#(.)*')
    operation_regex = re.compile(r'(.+)\((.*)\)')

    trans_set_file = open(in_path, "r")
    # Getting all lines
    lines = trans_set_file.read().splitlines()

    # Grouping up
    for line in lines:
        if(comment_regex.match(line)):
            continue
        elif(operation_regex.match(line).groups()):
            groups = operation_regex.match(line).groups()
            tm.operate(make_operation(groups))

def execute_single(file, in_dir = ".", out_dir = "."):
    in_file_path = in_dir + "/" + file
    out_file_path = out_dir + "/" + file + "-out"
    # ("parsing input for", in_file_path, out_file_path)
    parse_input(in_file_path, out_file_path)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", type=str, help="Run simulation for file")
    args = parser.parse_args()
    # (args.file)

    if(args.file and not os.path.isfile(args.file)):
        print("Invalid file path")
        exit(0)
    
    if(args.file):
        execute_single(args.file)
    # Reading transactions from file
    else:
        trans_set_path = "trans-sets"
        out_set_path = "trans-sets-out"

        if(not os.path.isdir(trans_set_path)):
            print("No dir called", trans_set_path)
            exit(0)
        # get all file names in the dir, execute sim for each file
        files = os.listdir(trans_set_path)

        for file in files:
            execute_single(file, in_dir = trans_set_path, out_dir = out_set_path)
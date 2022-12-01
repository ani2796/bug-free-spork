import trans_mgr
import re
import os

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
        elif(groups := operation_regex.match(line).groups()):
            tm.operate(make_operation(groups))

def execute_single(file):
    in_file_path = trans_set_path + "/" + file
    out_file_path = out_set_path + "/" + file + "-out"
    print("parsing input for", in_file_path, out_file_path)
    parse_input(in_file_path, out_file_path)


if __name__ == "__main__":
    # Reading transactions from file
    trans_set_path = "trans-sets"
    out_set_path = "trans-sets-out"

    if(not os.path.isdir(trans_set_path)):
        print("No dir called", trans_set_path)
        exit(0)

    single_file = False
    # get all file names in the dir, execute sim for each file
    files = os.listdir(trans_set_path)

    if(not single_file):
        for file in files:
            execute_single(file)
    else:
        file = "set-6"
        execute_single(file)
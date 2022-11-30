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

def parse_input(trans_set_file):
    # New transaction manager
    tm = trans_mgr.trans_mgr()

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



if __name__ == "__main__":
    # Reading transactions from file
    trans_set_path = "trans-sets"

    if(not os.path.isdir(trans_set_path)):
        print("No dir called", trans_set_path)
        exit(0)

    # get all file names in the dir, execute sim for each file
    files = os.listdir(trans_set_path)
    for file in files:
        file_path = trans_set_path + "/" + file
        print("parsing input for", file)
        parse_input(open(file_path, "r"))
        


    

    
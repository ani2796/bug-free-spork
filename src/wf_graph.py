from collections import deque
# Constructing waits-for graph
# Every time a transaction requests for a lock on a variable,
# TM checks that it can obtain a lock on that variable at all possible replicated spots

class wf_graph:
    def __init__(self) -> None:
        self.nodes = {}

    def __str__(self) -> str:
        return str(self.nodes)

    def add_node(self, node):
        self.nodes[node] = {
            "adj_list": deque(),
            "color": "red",
            "cycle?": False
        }
        # ("add node", self.nodes)


    def reset_graph(self):
        # ("reset graph")
        for (node, curr) in self.nodes.items():
            # ("node", node)
            self.nodes[node]["color"] = "red"
            self.nodes[node]["cycle?"] = False

    def add_edge(self, src, dest):
        self.nodes[src]["adj_list"].append(dest)
    

    def cycle_check(self):
        is_cycle = False
        for (node, curr) in self.nodes.items():
            # ("cc: node", node)
            if(curr["color"] == "red"):
                is_cycle = is_cycle or self.connected_cycle_check(node)
        
        # ("is cycle?", is_cycle)
        self.reset_graph()

        return is_cycle
    
    def connected_cycle_check(self, start):
        stack = deque({start})
        # ("ccc: wf graph stack", stack)

        while(stack):
            curr_node = self.nodes[stack[0]]
            # ("ccc: stack =", stack)
            # ("ccc: curr node =", stack[0], curr_node)
            self.nodes[stack[0]]["color"] = "green"
            is_end = True
            
            for adj in curr_node["adj_list"]:
                is_end = True
                adj_node = self.nodes[adj]
                # ("ccc: adj: ", adj, adj_node)
                if(adj_node["color"] == "green"):
                    if(adj in stack): # There is a cycle
                        return True
                else: # Unexplored node ==> cannot be part of a cycle yet
                    stack.appendleft(adj)
                    is_end = False
                    break
            if(is_end):
                stack.popleft()
            
        return False
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
        print("add node", self.nodes)

    def remove_node(self, del_node):
        if(del_node in self.nodes):
            self.nodes.pop(del_node)

        print("wf graph: removing", del_node, "result", self.nodes)
        
        for (node, curr) in self.nodes.items():
            print("wf graph: self.nodes", self.nodes)
            print("wf graph: node", node)
            if(del_node in self.nodes[node]["adj_list"]):
                self.nodes[node]["adj_list"].remove(del_node)
        print("wf graph: removed", node, "graph", self.nodes)

    def reset_graph(self):
        print("reset graph")
        for (node, curr) in self.nodes.items():
            print("node", node)
            self.nodes[node]["color"] = "red"
            self.nodes[node]["cycle?"] = False

    def add_edge(self, src, dest):
        if(not dest in self.nodes[src]["adj_list"]):
            self.nodes[src]["adj_list"].append(dest)
    

    def cycle_check(self):
        is_cycle = False
        cycle_nodes = None
        print("cc: before check, printing graph")
        print(self.nodes)

        for (node, curr) in self.nodes.items():
            print("cc: node", node)

            if(curr["color"] == "red"):
                (cycle_nodes, is_component_cycle) = self.connected_cycle_check(node)
                is_cycle = is_cycle or is_component_cycle
        
        print("is cycle?", is_cycle)
        self.reset_graph()

        return (cycle_nodes, is_cycle)
    
    def get_cycle(self, stack, cycle_start):
        cycle_nodes = deque()
        for node in stack:
            cycle_nodes.appendleft(node)
            if(node == cycle_start):
                break
        
        print("gc: stack =", stack)
        print("gc: cycle start =", cycle_start, ", end at curr")
        print("gc: cycle nodes =", cycle_nodes)
        return cycle_nodes

    def connected_cycle_check(self, start):
        stack = deque({start})
        print("ccc: wf graph stack", stack)

        while(stack):
            (node, curr) = (stack[0], self.nodes[stack[0]])
            print("ccc: stack =", stack)
            print("ccc: curr node =", node)
            self.nodes[stack[0]]["color"] = "green"
            is_end = True
            
            for adj in curr["adj_list"]:
                is_end = True
                adj_node = self.nodes[adj]
                print("ccc: adj: ", adj, adj_node)
                if(adj_node["color"] == "green"):
                    if(adj in stack): # There is a cycle
                        return (self.get_cycle(stack, adj), True)
                else: # Unexplored node ==> cannot be part of a cycle yet
                    stack.appendleft(adj)
                    is_end = False
                    break
            if(is_end):
                stack.popleft()
            
        return (None, False)
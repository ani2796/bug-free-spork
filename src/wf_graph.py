# Constructing waits-for graph
# Every time a transaction requests for a lock on a variable,
# TM checks that it can obtain a lock on that variable at all possible replicated spots

class wf_graph:
    def __init__(self) -> None:
        # TODO: Initialize waits for graph
        self.adj_list = {
            # TODO: Figure out how to initialize graph nodes in Python
            # TODO; Each node has a color (init to red) to assist with BFS exploration (turns green)
            # TODO: Node color reset to red after explorations
        }
        pass

    def cycle_check(self, node):
        # TODO: Check if the graph has a cycle that contains `node``
        # TODO: Cycle check using BFS
        # Steps:
        # 1. mark curent node green
        # 2. recursively go down each adj list node
        # 3. base case: all adj list nodes are red, simply return false
        # 4. if at some point you see a link back to `node`, return true
        # 5. if encounter green nodes, OR their exploration results with the others
        pass


# Variables need to have commits and uncommitted versions
# Data mgrs need to have a variable "mode": normal, failed or recovered

class data_mgr:
    def __init__(self, idx) -> None:
        # TODO: Initialize data manager with variables, lock table
        # TODO: Figure out enumerations in Python, use for node modes
        self.mode = "normal"
        self.id = idx

        # Initializing variables at site
        self.variables = {}
        for var_idx in range(1, 21, 1):
            var = "x"+str(var_idx)
            if(var_idx%2 == 0):
                self.variables[var] = [{
                    "commit_time": 0,
                    "value": var_idx * 10
                }]
            elif((var_idx%10)+1 == self.id):
                self.variables[var] = [{
                    "commit_time": 0,
                    "value": var_idx * 10
                }]
                

    def __str__(self) -> str:
        return "mode: " + self.mode + "\nvariables" + str(self.variables)

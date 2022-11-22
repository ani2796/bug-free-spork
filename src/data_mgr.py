# Variables need to have commits and uncommitted versions
# Data mgrs need to have a variable "mode": normal, failed or recovered

import lock_table

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
        
        self.lock_table = lock_table.lock_table(self.variables)
    
    def view_variables(self):
        return self.variables

    def view_lock_table(self):
        return self.lock_table
    
    def lock_var(self, trans, var, lock):
        print("Data Manager", self.id, "attempting lock ", trans, var, lock)
        result = self.lock_table.lock_var(trans, var, lock)
        print("Data Manager", self.id, "result: ", self.lock_table)
        return result

    def unlock_var(self, trans, var, lock):
        print("Data Manager", self.id, "attempting lock ", trans, var, lock)
        result = self.lock_table.unlock_var(trans, var)
        print("Data Manager", self.id, "result: ", self.lock_table)
        return result

    def __str__(self) -> str:
        return "id: " + str(self.id) + "\nmode: " + self.mode + "\nvariables" + str(self.variables) + "\n" + str(self.lock_table)

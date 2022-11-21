# Variables need to have commits and uncommitted versions
# Data mgrs need to have a variable "mode": normal, failed or recovered

class data_mgr:
    def __init__(self) -> None:
        # TODO: Initialize data manager with variables, lock table
        # TODO: Figure out enumerations in Python, use for node modes
        self.mode = "normal"
        self.variables = {}

    def __str__(self) -> str:
        return "mode: " + self.mode + "\nvariables" + str(self.variables)

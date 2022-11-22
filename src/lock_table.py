class lock_table:
    def __init__(self, variables) -> None:
        # TODO: Initialize lock table
        self.locks = {}
        for var in variables:
            # TODO: Replace strings for R/W with enumerations
            self.locks[str(var)] = {
                "trans": None,
                "read": False,
                "write": False,
            }
        pass

    def __str__(self) -> str:
        return str(self.locks)
        
    def lock_variable(self):
        # TODO: Implement variable locking
        pass

    def unlock_variable(self):
        # TODO: Implement variable unlocking
        pass
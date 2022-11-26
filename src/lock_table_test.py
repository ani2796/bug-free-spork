import pytest
import trans_mgr
import data_mgr
import lock_table
from collections import deque

# Sample test
def test_always_passes():
    assert True

def test_shared_read_locks():
    # Initiating TM
    tm = trans_mgr.trans_mgr()
    data_mgr = tm.data_mgrs[1]
    lock_table = data_mgr.view_lock_table()

    # T1, T2, T3 all share read lock on x2
    data_mgr.test_lock_var("T1", "x2", "read", to_lock = True)
    data_mgr.test_lock_var("T2", "x2", "read", to_lock = True)
    data_mgr.test_lock_var("T3", "x2", "read", to_lock = True)
    lock = lock_table.view_lock("x2")

    # Checking that 
    assert lock["trans"] == deque(['T1', 'T2', 'T3'])
    assert lock["type"] == "read"

def test_upgrade_lock():
    # Initiating TM
    tm = trans_mgr.trans_mgr()
    data_mgr = tm.data_mgrs[1]
    lock_table = data_mgr.view_lock_table()

    data_mgr.test_lock_var("T1", "x2", "read", to_lock = True)
    data_mgr.test_lock_var("T2", "x2", "read", to_lock = True)
    assert data_mgr.test_lock_var("T1", "x2", "write", to_lock = False) == False

    var_lock = lock_table.view_lock("x2")
    assert (var_lock["type"] == "read" and "T1" in var_lock["trans"] and "T2" in var_lock["trans"])
    assert lock_table.view_lock("x2")["type"] == "read"

    data_mgr.unlock_var("T2", "x2")
    assert  data_mgr.test_lock_var("T1", "x2", "write") == True

def test_exclusive_write_locks():
    # Initiating TM
    tm = trans_mgr.trans_mgr()
    data_mgr = tm.data_mgrs[1]

    data_mgr.test_lock_var("T1", "x2", "read", to_lock = True)
    assert data_mgr.test_lock_var("T2", "x2", "write", to_lock = False) == False

    data_mgr.unlock_var("T1", "x2")
    data_mgr.test_lock_var("T1", "x2", "write", to_lock = True)
    assert data_mgr.test_lock_var("T2", "x2", "write", to_lock = False) == False
    assert data_mgr.test_lock_var("T2", "x2", "read", to_lock = False) == False
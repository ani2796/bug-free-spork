import pytest
import trans_mgr
from collections import deque

# Sample test
def test_always_passes():
    assert True

def test_shared_read_locks():
    tm = trans_mgr.trans_mgr()
    data_mgr = tm.data_mgrs[1]

    data_mgr.lock_var("T1", "x2", "read")
    data_mgr.lock_var("T2", "x2", "read")
    data_mgr.lock_var("T3", "x2", "read")
    result = data_mgr.view_lock_table().view_lock("x2")
    print("locks on x2", result)

    assert data_mgr.view_lock_table().view_lock("x2")["trans"] == deque(['T1', 'T2', 'T3'])

def test_upgrade_lock():
    tm = trans_mgr.trans_mgr()
    data_mgr = tm.data_mgrs[1]

    data_mgr.lock_var("T1", "x2", "read")
    data_mgr.lock_var("T2", "x2", "read")
    assert data_mgr.lock_var("T1", "x2", "write") == False  

    data_mgr.unlock_var("T2", "x2", "read")    
    assert  data_mgr.lock_var("T1", "x2", "write") == True

def test_exclusive_write_locks():
    tm = trans_mgr.trans_mgr()
    data_mgr = tm.data_mgrs[1]

    data_mgr.lock_var("T1", "x2", "read")
    assert data_mgr.lock_var("T2", "x2", "write") == False

    data_mgr.unlock_var("T1", "x2", "read")
    data_mgr.lock_var("T1", "x2", "write")
    assert data_mgr.lock_var("T2", "x2", "write") == False
    assert data_mgr.lock_var("T2", "x2", "read") == False
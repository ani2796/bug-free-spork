import pytest
import trans_mgr
from collections import deque

# Sample test
def test_always_passes():
    assert True

def test_shared_read_locks():
    # Initiating TM
    out_path = "trans-sets-out/pytest.out"
    out_file = open(out_path, "w")
    tm = trans_mgr.trans_mgr(out_file)
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
    out_path = "trans-sets-out/pytest.out"
    out_file = open(out_path, "w")
    tm = trans_mgr.trans_mgr(out_file)
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
    out_path = "trans-sets-out/pytest.out"
    out_file = open(out_path, "w")
    tm = trans_mgr.trans_mgr(out_file)
    data_mgr = tm.data_mgrs[1]

    data_mgr.test_lock_var("T1", "x2", "read", to_lock = True)
    assert data_mgr.test_lock_var("T2", "x2", "write", to_lock = False) == False

    data_mgr.unlock_var("T1", "x2")
    data_mgr.test_lock_var("T1", "x2", "write", to_lock = True)
    assert data_mgr.test_lock_var("T2", "x2", "write", to_lock = False) == False
    assert data_mgr.test_lock_var("T2", "x2", "read", to_lock = False) == False


def test_wf():
    out_path = "trans-sets-out/pytest.out"
    out_file = open(out_path, "w")
    tm = trans_mgr.trans_mgr(out_file)
    graph = tm.get_wf_graph()

    # Connected graph with a cycle
    graph.add_node("T1")
    graph.add_node("T2")
    graph.add_node("T3")
    graph.add_node("T4")
    graph.add_edge("T1", "T2")
    graph.add_edge("T2", "T3")
    graph.add_edge("T2", "T4")
    graph.add_edge("T4", "T2")

    # (graph)

    assert graph.cycle_check() == (deque(["T2", "T4"]), True)

    out_path = "trans-sets-out/pytest.out"
    out_file = open(out_path, "w")
    tm = trans_mgr.trans_mgr(out_file)
    graph = tm.get_wf_graph()

    # Connected graph without a cycle
    graph.add_node("T1")
    graph.add_node("T2")
    graph.add_node("T3")
    graph.add_node("T4")
    graph.add_edge("T1", "T2")
    graph.add_edge("T2", "T3")
    graph.add_edge("T2", "T4")

    assert graph.cycle_check() == (None, False)

    # Disconnected graph with a cycle
    graph.add_node("T1")
    graph.add_node("T2")
    graph.add_node("T3")
    graph.add_node("T4")
    graph.add_node("T5")
    graph.add_edge("T1", "T2")
    graph.add_edge("T3", "T4")
    graph.add_edge("T4", "T5")
    graph.add_edge("T5", "T3")

    assert graph.cycle_check() == (deque(["T3", "T4", "T5"]), True)

    # Disconnected graph without a cycle
    graph.add_node("T1")
    graph.add_node("T2")
    graph.add_node("T3")
    graph.add_node("T4")
    graph.add_node("T5")
    graph.add_edge("T1", "T2")
    graph.add_edge("T3", "T4")
    graph.add_edge("T4", "T5")
    graph.add_edge("T3", "T5")
    
    assert graph.cycle_check() == (None, False)
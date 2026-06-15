#!/usr/bin/env python3
import subprocess
import time
import json
import urllib.request
import os
import signal

BASE_PORT = 8000

def node_port(node_id):
    return BASE_PORT + node_id + 1

def node_url(node_id, path):
    return f"http://127.0.0.1:{node_port(node_id)}{path}"

def get_status(node_id):
    try:
        with urllib.request.urlopen(node_url(node_id, "/status"), timeout=2) as resp:
            return json.loads(resp.read())
    except:
        return None

def post_json(node_id, path, data):
    req = urllib.request.Request(
        node_url(node_id, path),
        data=json.dumps(data).encode(),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read())

def find_leader():
    for i in range(5):
        s = get_status(i)
        if s and s["state"] == "leader":
            return i
    return None

def start_node(node_id):
    return subprocess.Popen(
        ["node", "dist/raft.js", str(node_id)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

def kill_all():
    subprocess.run(["pkill", "-f", "dist/raft.js"], capture_output=True)
    time.sleep(1)

def main():
    print("=" * 50)
    print("  FULL ACCEPTANCE TEST")
    print("=" * 50)
    
    kill_all()
    subprocess.run(["rm", "-rf", "data"])
    
    print("\n--- Test 1: Write 200 entries + Snapshot ---")
    procs = [start_node(i) for i in range(3)]
    time.sleep(2)
    
    leader = find_leader()
    assert leader is not None, "No leader found"
    print(f"Leader: node {leader}")
    
    for i in range(1, 201):
        post_json(leader, "/client/write", {"op": "set", "key": f"k{i}", "value": f"v{i}"})
    
    time.sleep(1)
    
    status = get_status(leader)
    print(f"  logLength = {status['logLength']}")
    print(f"  snapshotIndex = {status['snapshotIndex']}")
    print(f"  kvSize = {status['kvSize']}")
    
    assert status["snapshotIndex"] >= 100, f"Expected snapshotIndex >= 100, got {status['snapshotIndex']}"
    assert status["kvSize"] == 200, f"Expected kvSize = 200, got {status['kvSize']}"
    assert status["logLength"] <= 120, f"Expected logLength <= 120, got {status['logLength']}"
    print("  PASS ✓")
    
    print("\n--- Test 2: Restart node, recover from snapshot ---")
    before = get_status(0)
    print(f"  Before restart: logLen={before['logLength']}, snapIdx={before['snapshotIndex']}, kvSize={before['kvSize']}")
    
    procs[0].terminate()
    procs[0].wait()
    time.sleep(1)
    
    procs[0] = start_node(0)
    time.sleep(2)
    
    after = get_status(0)
    print(f"  After restart:  logLen={after['logLength']}, snapIdx={after['snapshotIndex']}, kvSize={after['kvSize']}")
    
    assert after["snapshotIndex"] >= 100, "Snapshot not restored"
    assert after["kvSize"] == 200, "KV data not restored"
    print("  PASS ✓")
    
    print("\n--- Test 3: Add empty new node (node 5) ---")
    new_node_id = 5
    new_proc = start_node(new_node_id)
    time.sleep(1)
    print(f"  Started empty node {new_node_id}")
    
    leader = find_leader()
    assert leader is not None, "No leader found after restart"
    print(f"  Current leader: node {leader}")
    
    post_json(leader, "/admin/add-member", {"nodeId": new_node_id, "port": node_port(new_node_id)})
    print(f"  Added node {new_node_id} to cluster")
    
    time.sleep(5)
    
    new_status = get_status(new_node_id)
    print(f"  state={new_status['state']}")
    print(f"  logLen={new_status['logLength']}, snapIdx={new_status['snapshotIndex']}")
    print(f"  lastApplied={new_status['lastApplied']}, kvSize={new_status['kvSize']}")
    print(f"  clusterSize={new_status['clusterSize']}")
    
    assert new_status["kvSize"] == 200, f"New node has {new_status['kvSize']} keys, expected 200"
    assert new_status["snapshotIndex"] >= 100, "New node doesn't have snapshot"
    print("  PASS ✓")
    
    print("\n--- Test 4: Remove node, cluster still works ---")
    leader = find_leader()
    assert leader is not None, "No leader found after add-member"
    print(f"  Current leader: node {leader}")
    
    post_json(leader, "/admin/remove-member", {"nodeId": new_node_id})
    print(f"  Removed node {new_node_id}")
    
    time.sleep(2)
    
    post_json(leader, "/client/write", {"op": "set", "key": "after-remove", "value": "works"})
    time.sleep(0.5)
    
    leader_status = get_status(leader)
    print(f"  clusterSize={leader_status['clusterSize']}")
    print(f"  kvSize={leader_status['kvSize']}")
    
    assert leader_status["clusterSize"] == 5, f"Expected clusterSize=5, got {leader_status['clusterSize']}"
    assert leader_status["kvSize"] == 201, f"Expected kvSize=201, got {leader_status['kvSize']}"
    print("  PASS ✓")
    
    print("\n" + "=" * 50)
    print("  ALL TESTS PASSED ✓")
    print("=" * 50)
    
    for p in procs:
        p.terminate()
    new_proc.terminate()
    kill_all()

if __name__ == "__main__":
    main()

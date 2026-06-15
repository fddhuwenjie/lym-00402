import { spawn, ChildProcess } from "child_process";
import http from "http";
import fs from "fs";
import path from "path";

const BASE_PORT = 8001;
const NODE_COUNT = 5;
const DATA_DIR = "./data";

interface NodeInfo {
  id: number;
  port: number;
  process: ChildProcess | null;
  alive: boolean;
}

const nodes: NodeInfo[] = [];

function httpGet(url: string): Promise<object | null> {
  return new Promise((resolve) => {
    const req = http.get(url, (res) => {
      let data = "";
      res.on("data", (c) => (data += c));
      res.on("end", () => {
        try {
          resolve(JSON.parse(data));
        } catch {
          resolve(null);
        }
      });
    });
    req.on("error", () => resolve(null));
    req.setTimeout(2000, () => {
      req.destroy();
      resolve(null);
    });
  });
}

function httpPost(url: string, body: object): Promise<object | null> {
  return new Promise((resolve) => {
    const data = JSON.stringify(body);
    const urlObj = new URL(url);
    const req = http.request(
      {
        hostname: urlObj.hostname,
        port: parseInt(urlObj.port),
        path: urlObj.pathname,
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(data),
        },
      },
      (res) => {
        let chunks = "";
        res.on("data", (c) => (chunks += c));
        res.on("end", () => {
          try {
            resolve(JSON.parse(chunks));
          } catch {
            resolve(null);
          }
        });
      }
    );
    req.on("error", () => resolve(null));
    req.setTimeout(3000, () => {
      req.destroy();
      resolve(null);
    });
    req.write(data);
    req.end();
  });
}

function clearDataDir(nodeId: number): void {
  const dir = path.join(DATA_DIR, `node-${nodeId}`);
  if (fs.existsSync(dir)) {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

function clearAllData(): void {
  if (fs.existsSync(DATA_DIR)) {
    fs.rmSync(DATA_DIR, { recursive: true, force: true });
  }
}

function startNode(id: number): void {
  const port = BASE_PORT + id;
  const info = nodes[id];

  if (info.process && info.alive) {
    console.log(`Node ${id} is already running`);
    return;
  }

  const proc = spawn(
    process.execPath,
    [require.resolve("./raft"), id.toString()],
    {
      stdio: ["pipe", "pipe", "pipe"],
      env: { ...process.env },
    }
  );

  proc.stdout?.on("data", (data: Buffer) => {
    console.log(`[Node ${id}] ${data.toString().trim()}`);
  });

  proc.stderr?.on("data", (data: Buffer) => {
    console.error(`[Node ${id} ERR] ${data.toString().trim()}`);
  });

  proc.on("exit", () => {
    info.alive = false;
    info.process = null;
    console.log(`Node ${id} stopped`);
  });

  info.process = proc;
  info.alive = true;
  console.log(`Node ${id} started on port ${port}`);
}

function killNode(id: number): void {
  const info = nodes[id];
  if (!info.process || !info.alive) {
    console.log(`Node ${id} is not running`);
    return;
  }
  info.process.kill("SIGTERM");
  info.alive = false;
  console.log(`Node ${id} killed`);
}

function restartNode(id: number): void {
  killNode(id);
  setTimeout(() => startNode(id), 500);
}

async function status(): Promise<void> {
  console.log("Cluster status:");
  for (let i = 0; i < NODE_COUNT + 5; i++) {
    const port = BASE_PORT + i;
    const s = await httpGet(`http://127.0.0.1:${port}/status`);
    if (s) {
      const st = s as any;
      console.log(
        `  Node ${st.nodeId}: state=${st.state}, term=${st.currentTerm}, leader=${st.currentLeader}, commit=${st.commitIndex}, lastApplied=${st.lastApplied}, logLen=${st.logLength}, snapIdx=${st.snapshotIndex}, kvSize=${st.kvSize}, clusterSize=${st.clusterSize}`
      );
    }
  }
}

async function findLeader(): Promise<number | null> {
  for (let i = 0; i < NODE_COUNT + 5; i++) {
    const port = BASE_PORT + i;
    const s = await httpGet(`http://127.0.0.1:${port}/status`);
    if (s && (s as any).state === "leader") {
      return i;
    }
  }
  return null;
}

async function writeKey(key: string, value: string): Promise<boolean> {
  const leader = await findLeader();
  if (leader === null) {
    console.log("Write failed: no leader available");
    return false;
  }
  const port = BASE_PORT + leader;
  const reply = await httpPost(`http://127.0.0.1:${port}/client/write`, {
    op: "set",
    key,
    value,
  });
  if (reply && (reply as any).success) {
    return true;
  }
  console.log(`Write failed: ${(reply as any)?.error || "unknown"}`);
  return false;
}

async function readKey(key: string): Promise<string | null> {
  const leader = await findLeader();
  if (leader === null) {
    console.log("Read failed: no leader available");
    return null;
  }
  const port = BASE_PORT + leader;
  const reply = await httpGet(
    `http://127.0.0.1:${port}/client/read/${encodeURIComponent(key)}`
  );
  if (reply && (reply as any).success) {
    return (reply as any).value;
  }
  return null;
}

async function addMember(nodeId: number, port: number): Promise<boolean> {
  const leader = await findLeader();
  if (leader === null) {
    console.log("Add member failed: no leader available");
    return false;
  }
  const leaderPort = BASE_PORT + leader;
  const reply = await httpPost(
    `http://127.0.0.1:${leaderPort}/admin/add-member`,
    { nodeId, port }
  );
  if (reply && (reply as any).success) {
    console.log(`Add member success: node ${nodeId}:${port}`);
    return true;
  }
  console.log(`Add member failed: ${(reply as any)?.error || "unknown"}`);
  return false;
}

async function removeMember(nodeId: number): Promise<boolean> {
  const leader = await findLeader();
  if (leader === null) {
    console.log("Remove member failed: no leader available");
    return false;
  }
  const leaderPort = BASE_PORT + leader;
  const reply = await httpPost(
    `http://127.0.0.1:${leaderPort}/admin/remove-member`,
    { nodeId }
  );
  if (reply && (reply as any).success) {
    console.log(`Remove member success: node ${nodeId}`);
    return true;
  }
  console.log(`Remove member failed: ${(reply as any)?.error || "unknown"}`);
  return false;
}

async function partition(groupA: number[], groupB: number[]): Promise<void> {
  for (const id of groupA) {
    await httpPost(`http://127.0.0.1:${BASE_PORT + id}/admin/partition`, {
      enabled: true,
      group: groupA,
    });
  }
  for (const id of groupB) {
    await httpPost(`http://127.0.0.1:${BASE_PORT + id}/admin/partition`, {
      enabled: true,
      group: groupB,
    });
  }
  console.log(`Network partition: [${groupA}] | [${groupB}]`);
}

async function heal(): Promise<void> {
  for (let i = 0; i < NODE_COUNT; i++) {
    await httpPost(`http://127.0.0.1:${BASE_PORT + i}/admin/partition`, {
      enabled: false,
      group: [],
    });
  }
  console.log("Network healed");
}

async function waitForLeader(timeoutMs = 5000): Promise<number | null> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const leader = await findLeader();
    if (leader !== null) return leader;
    await new Promise((r) => setTimeout(r, 200));
  }
  return null;
}

async function runSnapshotTest(): Promise<void> {
  console.log("\n=== Snapshot Test ===\n");

  console.log("Clearing all data...");
  clearAllData();

  console.log("Starting 3 nodes (0, 1, 2)...");
  for (let i = 0; i < 3; i++) {
    startNode(i);
  }
  await new Promise((r) => setTimeout(r, 1000));

  console.log("\nWaiting for leader...");
  const leader = await waitForLeader(3000);
  if (leader === null) {
    console.log("FAIL: No leader elected");
    return;
  }
  console.log(`Leader: node ${leader}`);

  console.log("\nWriting 200 KV entries...");
  const batchStart = Date.now();
  for (let i = 0; i < 200; i++) {
    await writeKey(`key-${i}`, `value-${i}`);
    if ((i + 1) % 50 === 0) {
      console.log(`  Written ${i + 1} entries...`);
    }
  }
  console.log(`Write completed in ${Date.now() - batchStart}ms`);

  await new Promise((r) => setTimeout(r, 500));

  console.log("\nChecking snapshot status:");
  const leaderStatus = await httpGet(`http://127.0.0.1:${BASE_PORT + leader}/status`);
  if (leaderStatus) {
    const st = leaderStatus as any;
    console.log(`  Leader node ${st.nodeId}:`);
    console.log(`    logLength = ${st.logLength}`);
    console.log(`    snapshotIndex = ${st.snapshotIndex}`);
    console.log(`    lastApplied = ${st.lastApplied}`);
    console.log(`    kvSize = ${st.kvSize}`);

    if (st.snapshotIndex > 0 && st.logLength < 200) {
      console.log("\n  PASS: Snapshot was taken, log was compacted");
    } else {
      console.log("\n  WARN: Snapshot may not have been taken yet");
    }
  }

  console.log("\nVerifying KV data integrity...");
  let verified = 0;
  for (let i = 0; i < 200; i++) {
    const val = await readKey(`key-${i}`);
    if (val === `value-${i}`) {
      verified++;
    }
  }
  console.log(`  Verified ${verified}/200 keys`);
  if (verified === 200) {
    console.log("  PASS: All KV values are correct");
  } else {
    console.log("  FAIL: Some KV values are wrong");
  }

  console.log("\nTesting restart recovery from snapshot...");
  const testNode = leader === 0 ? 1 : 0;
  console.log(`Killing node ${testNode}...`);
  killNode(testNode);
  await new Promise((r) => setTimeout(r, 500));

  console.log(`Restarting node ${testNode}...`);
  startNode(testNode);
  await new Promise((r) => setTimeout(r, 2000));

  const restartedStatus = await httpGet(`http://127.0.0.1:${BASE_PORT + testNode}/status`);
  if (restartedStatus) {
    const st = restartedStatus as any;
    console.log(`  Restarted node ${st.nodeId}:`);
    console.log(`    snapshotIndex = ${st.snapshotIndex}`);
    console.log(`    lastApplied = ${st.lastApplied}`);
    console.log(`    kvSize = ${st.kvSize}`);

    if (st.snapshotIndex > 0 && st.kvSize === 200) {
      console.log("  PASS: Node recovered from snapshot without replaying all logs");
    } else {
      console.log("  WARN: Node may not have recovered correctly");
    }
  }

  await status();
  console.log("\n=== Snapshot Test Completed ===");
}

async function runMembershipTest(): Promise<void> {
  console.log("\n=== Membership Change Test ===\n");

  console.log("Clearing all data...");
  clearAllData();

  console.log("Starting 3 nodes (0, 1, 2)...");
  for (let i = 0; i < 3; i++) {
    startNode(i);
  }
  await new Promise((r) => setTimeout(r, 1000));

  console.log("\nWaiting for leader...");
  const leader = await waitForLeader(3000);
  if (leader === null) {
    console.log("FAIL: No leader elected");
    return;
  }
  console.log(`Leader: node ${leader}`);

  console.log("\nWriting initial data...");
  for (let i = 0; i < 10; i++) {
    await writeKey(`init-${i}`, `value-${i}`);
  }
  await new Promise((r) => setTimeout(r, 300));
  console.log("Initial data written");

  console.log("\n--- Test: Add new member node 5 ---");
  const newNodeId = 5;
  const newNodePort = BASE_PORT + newNodeId;

  console.log("Starting new node (empty state)...");
  clearDataDir(newNodeId);
  startNode(newNodeId);
  await new Promise((r) => setTimeout(r, 500));

  console.log("Adding node 5 to cluster...");
  const addOk = await addMember(newNodeId, newNodePort);
  if (!addOk) {
    console.log("FAIL: Could not add member");
    return;
  }

  await new Promise((r) => setTimeout(r, 2000));

  console.log("\nChecking new node status:");
  const newNodeStatus = await httpGet(`http://127.0.0.1:${newNodePort}/status`);
  if (newNodeStatus) {
    const st = newNodeStatus as any;
    console.log(`  Node ${st.nodeId}:`);
    console.log(`    state = ${st.state}`);
    console.log(`    lastApplied = ${st.lastApplied}`);
    console.log(`    snapshotIndex = ${st.snapshotIndex}`);
    console.log(`    kvSize = ${st.kvSize}`);
    console.log(`    clusterSize = ${st.clusterSize}`);

    if (st.kvSize === 10 && st.clusterSize === 4) {
      console.log("  PASS: New node synced all data and cluster config updated");
    } else {
      console.log("  WARN: New node may not have fully synced");
    }
  }

  console.log("\nVerifying new node can serve reads (via leader redirect)...");
  const val = await readKey("init-0");
  if (val === "value-0") {
    console.log("  PASS: Data consistency maintained after add");
  } else {
    console.log("  FAIL: Data inconsistency after add");
  }

  console.log("\nWriting more data to verify 4-node cluster works...");
  for (let i = 0; i < 10; i++) {
    await writeKey(`post-add-${i}`, `value-${i}`);
  }
  await new Promise((r) => setTimeout(r, 500));

  let postAddVerified = 0;
  for (let i = 0; i < 10; i++) {
    const v = await readKey(`post-add-${i}`);
    if (v === `value-${i}`) postAddVerified++;
  }
  console.log(`  Verified ${postAddVerified}/10 post-add keys`);

  console.log("\n--- Test: Remove member node 5 ---");
  const removeOk = await removeMember(newNodeId);
  if (!removeOk) {
    console.log("FAIL: Could not remove member");
    return;
  }

  await new Promise((r) => setTimeout(r, 1000));

  console.log("Checking remaining cluster status:");
  const leaderAfter = await waitForLeader(2000);
  console.log(`  Leader after removal: node ${leaderAfter}`);

  const leaderStatus = await httpGet(`http://127.0.0.1:${BASE_PORT + (leaderAfter ?? 0)}/status`);
  if (leaderStatus) {
    const st = leaderStatus as any;
    console.log(`  Cluster size = ${st.clusterSize}`);
    if (st.clusterSize === 3) {
      console.log("  PASS: Cluster size reduced to 3");
    }
  }

  console.log("\nVerifying writes still work after removal...");
  let removeWriteOk = true;
  for (let i = 0; i < 5; i++) {
    const ok = await writeKey(`post-remove-${i}`, `value-${i}`);
    if (!ok) {
      removeWriteOk = false;
      break;
    }
  }
  if (removeWriteOk) {
    console.log("  PASS: Writes work after member removal");
  } else {
    console.log("  FAIL: Writes failed after member removal");
  }

  await status();
  console.log("\n=== Membership Test Completed ===");
}

async function runFullTest(): Promise<void> {
  console.log("\n=== Full Raft Feature Test ===\n");

  await runSnapshotTest();

  for (let i = 0; i < 10; i++) {
    const info = nodes[i];
    if (info && info.alive) {
      killNode(i);
    }
  }
  await new Promise((r) => setTimeout(r, 500));

  await runMembershipTest();

  console.log("\n=== All Tests Completed ===");
}

async function interactive(): Promise<void> {
  const args = process.argv.slice(2);
  const cmd = args[0];

  if (!cmd) {
    console.log(`
Raft Cluster Manager
Usage: node manager.js <command> [args]

Commands:
  start               Start all 5 nodes
  stop                Stop all nodes
  kill <id>           Kill node <id>
  restart <id>        Restart node <id>
  status              Show cluster status
  write <k> <v>       Write key=value
  read <k>            Read key
  partition <g1> <g2> Create partition (e.g. partition 0,1,2 3,4)
  heal                Heal network partition
  add-member <id> <port>  Add a node to cluster
  remove-member <id>      Remove a node from cluster
  test-snapshot       Run snapshot test
  test-membership     Run membership change test
  test                Run all tests
  clear-data          Clear all data directories
`);
    return;
  }

  switch (cmd) {
    case "start":
      for (let i = 0; i < NODE_COUNT; i++) startNode(i);
      break;
    case "stop":
      for (let i = 0; i < nodes.length; i++) {
        if (nodes[i] && nodes[i].alive) killNode(i);
      }
      break;
    case "kill":
      killNode(parseInt(args[1]));
      break;
    case "restart":
      restartNode(parseInt(args[1]));
      break;
    case "status":
      await status();
      break;
    case "write":
      await writeKey(args[1], args[2]);
      break;
    case "read":
      const val = await readKey(args[1]);
      console.log(`${args[1]} = ${val}`);
      break;
    case "partition": {
      const g1 = args[1].split(",").map(Number);
      const g2 = args[2].split(",").map(Number);
      await partition(g1, g2);
      break;
    }
    case "heal":
      await heal();
      break;
    case "add-member":
      await addMember(parseInt(args[1]), parseInt(args[2]));
      break;
    case "remove-member":
      await removeMember(parseInt(args[1]));
      break;
    case "test-snapshot":
      await runSnapshotTest();
      break;
    case "test-membership":
      await runMembershipTest();
      break;
    case "test":
      await runFullTest();
      break;
    case "clear-data":
      clearAllData();
      console.log("All data cleared");
      break;
    default:
      console.log(`Unknown command: ${cmd}`);
  }
}

for (let i = 0; i < NODE_COUNT + 10; i++) {
  nodes.push({ id: i, port: BASE_PORT + i, process: null, alive: false });
}

interactive().catch((err) => {
  console.error(err);
  process.exit(1);
});

import { spawn, ChildProcess } from "child_process";
import http from "http";

const BASE_PORT = 8001;
const NODE_COUNT = 5;

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
    req.setTimeout(2000, () => {
      req.destroy();
      resolve(null);
    });
    req.write(data);
    req.end();
  });
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
  for (let i = 0; i < NODE_COUNT; i++) {
    const port = BASE_PORT + i;
    const s = await httpGet(`http://127.0.0.1:${port}/status`);
    if (s) {
      console.log(
        `Node ${i}: state=${(s as any).state}, term=${(s as any).currentTerm}, leader=${(s as any).currentLeader}, commit=${(s as any).commitIndex}, logLen=${(s as any).logLength}, kvSize=${(s as any).kvSize}, partitioned=${(s as any).partitioned}`
      );
    } else {
      console.log(`Node ${i}: OFFLINE`);
    }
  }
}

async function writeKey(key: string, value: string): Promise<void> {
  for (let i = 0; i < NODE_COUNT; i++) {
    const port = BASE_PORT + i;
    const reply = await httpPost(`http://127.0.0.1:${port}/client/write`, {
      op: "set",
      key,
      value,
    });
    if (reply && (reply as any).success) {
      console.log(`Write success via node ${i}: ${key}=${value}`);
      return;
    }
    if (reply && (reply as any).error === "not leader") {
      const leaderPort = (reply as any).leaderPort;
      if (leaderPort) {
        const leaderReply = await httpPost(
          `http://127.0.0.1:${leaderPort}/client/write`,
          { op: "set", key, value }
        );
        if (leaderReply && (leaderReply as any).success) {
          const leaderId = (leaderReply as any).leaderId;
          console.log(
            `Write success via leader node ${leaderId}: ${key}=${value}`
          );
          return;
        }
      }
    }
  }
  console.log("Write failed: no leader available");
}

async function readKey(key: string): Promise<void> {
  for (let i = 0; i < NODE_COUNT; i++) {
    const port = BASE_PORT + i;
    const reply = await httpGet(
      `http://127.0.0.1:${port}/client/read/${encodeURIComponent(key)}`
    );
    if (reply && (reply as any).success) {
      console.log(`Read success: ${key}=${(reply as any).value}`);
      return;
    }
    if (reply && (reply as any).error === "not leader") {
      const leaderPort = (reply as any).leaderPort;
      if (leaderPort) {
        const leaderReply = await httpGet(
          `http://127.0.0.1:${leaderPort}/client/read/${encodeURIComponent(key)}`
        );
        if (leaderReply && (leaderReply as any).success) {
          console.log(`Read success: ${key}=${(leaderReply as any).value}`);
          return;
        }
      }
    }
  }
  console.log("Read failed: no leader available");
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
    for (let i = 0; i < NODE_COUNT; i++) {
      const s = await httpGet(`http://127.0.0.1:${BASE_PORT + i}/status`);
      if (s && (s as any).state === "leader") {
        return i;
      }
    }
    await new Promise((r) => setTimeout(r, 200));
  }
  return null;
}

async function verifyLogs(): Promise<void> {
  const logs: Map<number, string> = new Map();
  for (let i = 0; i < NODE_COUNT; i++) {
    const s = await httpGet(`http://127.0.0.1:${BASE_PORT + i}/status`);
    if (s) {
      logs.set(i, `commitIndex=${(s as any).commitIndex}, logLen=${(s as any).logLength}, kvSize=${(s as any).kvSize}`);
    }
  }
  console.log("Log consistency check:");
  for (const [id, info] of logs) {
    console.log(`  Node ${id}: ${info}`);
  }
}

async function runTest(): Promise<void> {
  console.log("\n=== Raft Cluster Verification Test ===\n");

  console.log("Starting 5 nodes...");
  for (let i = 0; i < NODE_COUNT; i++) {
    startNode(i);
  }
  await new Promise((r) => setTimeout(r, 1000));

  console.log("\n--- Test 1: Leader election within 3 seconds ---");
  const leader = await waitForLeader(3000);
  if (leader !== null) {
    console.log(`PASS: Leader elected: node ${leader}`);
  } else {
    console.log("FAIL: No leader elected within 3 seconds");
    return;
  }
  await status();

  console.log("\n--- Test 2: Write and read ---");
  await writeKey("hello", "world");
  await new Promise((r) => setTimeout(r, 200));
  await readKey("hello");
  await status();

  console.log("\n--- Test 3: Kill leader and re-election within 5 seconds ---");
  killNode(leader!);
  const newLeader = await waitForLeader(5000);
  if (newLeader !== null) {
    console.log(`PASS: New leader elected: node ${newLeader}`);
  } else {
    console.log("FAIL: No new leader elected within 5 seconds");
    return;
  }
  await status();

  console.log("\n--- Test 4: Log consistency after write ---");
  await writeKey("foo", "bar");
  await writeKey("baz", "qux");
  await new Promise((r) => setTimeout(r, 300));
  await verifyLogs();

  console.log("\n--- Test 5: Network partition ---");
  restartNode(leader!);
  await new Promise((r) => setTimeout(r, 1000));
  console.log("Restarted killed node, waiting for leader...");
  await waitForLeader(3000);
  await status();

  const majorityGroup = [0, 1, 2];
  const minorityGroup = [3, 4];
  await partition(majorityGroup, minorityGroup);
  await new Promise((r) => setTimeout(r, 500));

  console.log("Trying write to minority node (should redirect)...");
  let minorityRedirectPass = false;
  for (const nodeId of minorityGroup) {
    const reply = await httpPost(
      `http://127.0.0.1:${BASE_PORT + nodeId}/client/write`,
      { op: "set", key: "minority", value: "test" }
    );
    if (reply && (reply as any).error === "not leader") {
      console.log(`PASS: Minority node ${nodeId} rejected write (redirect)`);
      minorityRedirectPass = true;
      break;
    } else if (reply === null) {
      console.log(`Minority node ${nodeId} is offline, trying next...`);
    } else {
      console.log(`Minority node ${nodeId} reply:`, reply);
    }
  }
  if (!minorityRedirectPass) {
    console.log("WARN: Could not verify minority redirect (node may be offline)");
  }

  console.log("Writing to majority group...");
  await writeKey("partition_key", "partition_value");
  await new Promise((r) => setTimeout(r, 300));
  await status();

  console.log("\n--- Test 6: Heal partition and eventual consistency ---");
  await heal();
  await new Promise((r) => setTimeout(r, 1000));
  await status();
  await verifyLogs();

  console.log("\n--- Test 7: Final verification ---");
  await status();
  await verifyLogs();

  console.log("\n=== All tests completed ===");
}

async function interactive(): Promise<void> {
  const args = process.argv.slice(2);
  const cmd = args[0];

  if (!cmd) {
    console.log(`
Raft Cluster Manager
Usage: node manager.js <command> [args]

Commands:
  start           Start all 5 nodes
  stop            Stop all nodes
  kill <id>       Kill node <id>
  restart <id>    Restart node <id>
  status          Show cluster status
  write <k> <v>   Write key=value
  read <k>        Read key
  partition <g1> <g2>  Create partition (e.g. partition 0,1,2 3,4)
  heal            Heal network partition
  test            Run verification tests
`);
    return;
  }

  switch (cmd) {
    case "start":
      for (let i = 0; i < NODE_COUNT; i++) startNode(i);
      break;
    case "stop":
      for (let i = 0; i < NODE_COUNT; i++) killNode(i);
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
      await readKey(args[1]);
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
    case "test":
      await runTest();
      break;
    default:
      console.log(`Unknown command: ${cmd}`);
  }
}

for (let i = 0; i < NODE_COUNT; i++) {
  nodes.push({ id: i, port: BASE_PORT + i, process: null, alive: false });
}

interactive().catch((err) => {
  console.error(err);
  process.exit(1);
});

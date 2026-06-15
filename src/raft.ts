import http from "http";
import {
  NodeState,
  LogEntry,
  KVCommand,
  RequestVoteArgs,
  RequestVoteReply,
  AppendEntriesArgs,
  AppendEntriesReply,
  NodeConfig,
  ClientWriteRequest,
  ClientWriteResponse,
  ClientReadResponse,
  StatusResponse,
} from "./types";

const ELECTION_TIMEOUT_MIN = 150;
const ELECTION_TIMEOUT_MAX = 300;
const HEARTBEAT_INTERVAL = 50;

function randomElectionTimeout(): number {
  return (
    ELECTION_TIMEOUT_MIN +
    Math.floor(Math.random() * (ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN))
  );
}

function rpcPost(
  host: string,
  port: number,
  path: string,
  body: object
): Promise<object | null> {
  return new Promise((resolve) => {
    const data = JSON.stringify(body);
    const req = http.request(
      {
        hostname: host,
        port,
        path,
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(data),
        },
        timeout: 500,
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
    req.on("timeout", () => {
      req.destroy();
      resolve(null);
    });
    req.write(data);
    req.end();
  });
}

export class RaftNode {
  nodeId: number;
  state: NodeState = NodeState.FOLLOWER;
  currentTerm = 0;
  votedFor: number | null = null;
  log: LogEntry[] = [];
  commitIndex = 0;
  lastApplied = 0;

  nextIndex: number[] = [];
  matchIndex: number[] = [];

  currentLeader: number | null = null;

  config: NodeConfig;

  private electionTimer: ReturnType<typeof setTimeout> | null = null;
  private heartbeatTimer: ReturnType<typeof setTimeout> | null = null;

  kvStore: Map<string, string> = new Map();

  partitioned = false;
  partitionGroup: number[] = [];

  private votesGranted = 0;
  private stopping = false;

  constructor(config: NodeConfig) {
    this.config = config;
    this.nodeId = config.nodeId;
    this.nextIndex = new Array(config.peerPorts.length + 1).fill(1);
    this.matchIndex = new Array(config.peerPorts.length + 1).fill(0);
  }

  private getPortForNode(nodeId: number): number {
    const ports = [this.config.port, ...this.config.peerPorts];
    const sortedPorts = [...ports].sort((a, b) => a - b);
    return sortedPorts[nodeId];
  }

  private canCommunicateWith(nodeId: number): boolean {
    if (!this.partitioned) return true;
    return this.partitionGroup.includes(nodeId);
  }

  start(): void {
    this.stopping = false;
    this.resetElectionTimer();
  }

  stop(): void {
    this.stopping = true;
    if (this.electionTimer) {
      clearTimeout(this.electionTimer);
      this.electionTimer = null;
    }
    if (this.heartbeatTimer) {
      clearTimeout(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  private resetElectionTimer(): void {
    if (this.electionTimer) {
      clearTimeout(this.electionTimer);
    }
    const timeout = randomElectionTimeout();
    this.electionTimer = setTimeout(() => {
      if (!this.stopping) {
        this.startElection();
      }
    }, timeout);
  }

  private resetHeartbeatTimer(): void {
    if (this.heartbeatTimer) {
      clearTimeout(this.heartbeatTimer);
    }
    this.heartbeatTimer = setTimeout(() => {
      if (!this.stopping && this.state === NodeState.LEADER) {
        this.sendHeartbeats();
        this.resetHeartbeatTimer();
      }
    }, HEARTBEAT_INTERVAL);
  }

  private becomeFollower(term: number): void {
    this.state = NodeState.FOLLOWER;
    this.currentTerm = term;
    this.votedFor = null;
    if (this.heartbeatTimer) {
      clearTimeout(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    this.resetElectionTimer();
  }

  private startElection(): void {
    this.state = NodeState.CANDIDATE;
    this.currentTerm += 1;
    this.votedFor = this.nodeId;
    this.votesGranted = 1;
    this.currentLeader = null;

    this.resetElectionTimer();

    const lastLogIndex = this.log.length;
    const lastLogTerm = lastLogIndex > 0 ? this.log[lastLogIndex - 1].term : 0;

    const args: RequestVoteArgs = {
      term: this.currentTerm,
      candidateId: this.nodeId,
      lastLogIndex,
      lastLogTerm,
    };

    const totalNodes = this.config.peerPorts.length + 1;
    const majority = Math.floor(totalNodes / 2) + 1;

    for (let peerIdx = 0; peerIdx < totalNodes; peerIdx++) {
      if (peerIdx === this.nodeId) continue;
      if (!this.canCommunicateWith(peerIdx)) continue;

      const port = this.getPortForNode(peerIdx);
      rpcPost(this.config.host, port, "/raft/requestVote", args).then(
        (reply) => {
          if (!reply) return;
          const r = reply as RequestVoteReply;
          if (r.term > this.currentTerm) {
            this.becomeFollower(r.term);
            return;
          }
          if (
            this.state === NodeState.CANDIDATE &&
            this.currentTerm === args.term &&
            r.voteGranted
          ) {
            this.votesGranted += 1;
            if (this.votesGranted >= majority) {
              this.becomeLeader();
            }
          }
        }
      );
    }
  }

  private becomeLeader(): void {
    this.state = NodeState.LEADER;
    this.currentLeader = this.nodeId;

    const totalNodes = this.config.peerPorts.length + 1;
    this.nextIndex = new Array(totalNodes).fill(this.log.length + 1);
    this.matchIndex = new Array(totalNodes).fill(0);
    this.matchIndex[this.nodeId] = this.log.length;

    if (this.electionTimer) {
      clearTimeout(this.electionTimer);
      this.electionTimer = null;
    }

    this.sendHeartbeats();
    this.resetHeartbeatTimer();
  }

  private sendHeartbeats(): void {
    if (this.state !== NodeState.LEADER) return;

    const totalNodes = this.config.peerPorts.length + 1;

    for (let peerIdx = 0; peerIdx < totalNodes; peerIdx++) {
      if (peerIdx === this.nodeId) continue;

      this.sendAppendEntries(peerIdx);
    }
  }

  private sendAppendEntries(peerIdx: number): void {
    if (this.state !== NodeState.LEADER) return;
    if (!this.canCommunicateWith(peerIdx)) return;

    const prevLogIndex = this.nextIndex[peerIdx] - 1;
    const prevLogTerm =
      prevLogIndex > 0 ? this.log[prevLogIndex - 1].term : 0;

    const entries: LogEntry[] = [];
    for (let i = this.nextIndex[peerIdx] - 1; i < this.log.length; i++) {
      entries.push(this.log[i]);
    }

    const args: AppendEntriesArgs = {
      term: this.currentTerm,
      leaderId: this.nodeId,
      prevLogIndex,
      prevLogTerm,
      entries,
      leaderCommit: this.commitIndex,
    };

    const port = this.getPortForNode(peerIdx);
    rpcPost(this.config.host, port, "/raft/appendEntries", args).then(
      (reply) => {
        if (!reply) return;
        const r = reply as AppendEntriesReply;

        if (r.term > this.currentTerm) {
          this.becomeFollower(r.term);
          return;
        }

        if (this.state !== NodeState.LEADER) return;

        if (r.success) {
          this.nextIndex[peerIdx] = prevLogIndex + entries.length + 1;
          this.matchIndex[peerIdx] = prevLogIndex + entries.length;
          this.tryAdvanceCommit();
        } else {
          if (r.conflictIndex !== undefined && r.conflictTerm !== undefined) {
            if (r.conflictTerm === 0) {
              this.nextIndex[peerIdx] = r.conflictIndex;
            } else {
              let foundConflict = false;
              for (
                let i = this.nextIndex[peerIdx] - 2;
                i >= 0;
                i--
              ) {
                if (this.log[i].term === r.conflictTerm) {
                  this.nextIndex[peerIdx] = i + 2;
                  foundConflict = true;
                  break;
                }
              }
              if (!foundConflict) {
                this.nextIndex[peerIdx] = r.conflictIndex;
              }
            }
          } else {
            this.nextIndex[peerIdx] = Math.max(1, this.nextIndex[peerIdx] - 1);
          }
        }
      }
    );
  }

  private tryAdvanceCommit(): void {
    const totalNodes = this.config.peerPorts.length + 1;

    for (let n = this.commitIndex + 1; n <= this.log.length; n++) {
      if (this.log[n - 1].term !== this.currentTerm) continue;

      let replicated = 1;
      for (let i = 0; i < totalNodes; i++) {
        if (i === this.nodeId) continue;
        if (this.matchIndex[i] >= n) replicated++;
      }

      if (replicated >= Math.floor(totalNodes / 2) + 1) {
        this.commitIndex = n;
      }
    }

    this.applyCommitted();
  }

  private applyCommitted(): void {
    while (this.lastApplied < this.commitIndex) {
      this.lastApplied += 1;
      const entry = this.log[this.lastApplied - 1];
      if (entry.command) {
        this.applyCommand(entry.command);
      }
    }
  }

  private applyCommand(cmd: KVCommand): void {
    switch (cmd.op) {
      case "set":
        if (cmd.value !== undefined) {
          this.kvStore.set(cmd.key, cmd.value);
        }
        break;
      case "delete":
        this.kvStore.delete(cmd.key);
        break;
    }
  }

  handleRequestVote(args: RequestVoteArgs): RequestVoteReply {
    if (this.partitioned && !this.partitionGroup.includes(args.candidateId)) {
      return { term: this.currentTerm, voteGranted: false };
    }

    if (args.term > this.currentTerm) {
      this.becomeFollower(args.term);
    }

    const reply: RequestVoteReply = {
      term: this.currentTerm,
      voteGranted: false,
    };

    if (args.term < this.currentTerm) {
      return reply;
    }

    if (this.votedFor !== null && this.votedFor !== args.candidateId) {
      return reply;
    }

    const lastLogIndex = this.log.length;
    const lastLogTerm = lastLogIndex > 0 ? this.log[lastLogIndex - 1].term : 0;

    if (args.lastLogTerm > lastLogTerm) {
    } else if (
      args.lastLogTerm === lastLogTerm &&
      args.lastLogIndex >= lastLogIndex
    ) {
      // candidate's log is at least as long
    } else {
      return reply;
    }

    this.votedFor = args.candidateId;
    reply.voteGranted = true;
    this.resetElectionTimer();

    return reply;
  }

  handleAppendEntries(args: AppendEntriesArgs): AppendEntriesReply {
    if (this.partitioned && !this.partitionGroup.includes(args.leaderId)) {
      return { term: this.currentTerm, success: false };
    }

    if (args.term > this.currentTerm) {
      this.becomeFollower(args.term);
    }

    const reply: AppendEntriesReply = {
      term: this.currentTerm,
      success: false,
    };

    if (args.term < this.currentTerm) {
      return reply;
    }

    this.currentLeader = args.leaderId;
    this.resetElectionTimer();

    if (this.state === NodeState.CANDIDATE) {
      this.becomeFollower(args.term);
    }

    if (
      args.prevLogIndex > 0 &&
      (args.prevLogIndex > this.log.length ||
        this.log[args.prevLogIndex - 1].term !== args.prevLogTerm)
    ) {
      if (args.prevLogIndex > this.log.length) {
        reply.conflictIndex = this.log.length + 1;
        reply.conflictTerm = 0;
      } else {
        const conflictTerm = this.log[args.prevLogIndex - 1].term;
        let conflictIndex = 1;
        for (let i = 0; i < this.log.length; i++) {
          if (this.log[i].term === conflictTerm) {
            conflictIndex = i + 1;
            break;
          }
        }
        reply.conflictIndex = conflictIndex;
        reply.conflictTerm = conflictTerm;
      }
      return reply;
    }

    for (let i = 0; i < args.entries.length; i++) {
      const logIndex = args.prevLogIndex + 1 + i;
      if (logIndex <= this.log.length) {
        if (this.log[logIndex - 1].term !== args.entries[i].term) {
          this.log = this.log.slice(0, logIndex - 1);
          this.log.push(args.entries[i]);
        }
      } else {
        this.log.push(args.entries[i]);
      }
    }

    if (args.leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(args.leaderCommit, this.log.length);
      this.applyCommitted();
    }

    reply.success = true;
    return reply;
  }

  private isMinorityLeader(): boolean {
    if (!this.partitioned) return false;
    const totalNodes = this.config.peerPorts.length + 1;
    const majority = Math.floor(totalNodes / 2) + 1;
    return this.partitionGroup.length < majority;
  }

  proposeCommand(cmd: KVCommand): ClientWriteResponse {
    if (this.state !== NodeState.LEADER) {
      return {
        success: false,
        leaderId: this.currentLeader ?? undefined,
        leaderPort: this.currentLeader !== null ? this.getPortForNode(this.currentLeader) : undefined,
        error: "not leader",
      };
    }

    if (this.isMinorityLeader()) {
      return {
        success: false,
        leaderId: undefined,
        leaderPort: undefined,
        error: "not leader",
      };
    }

    const entry: LogEntry = {
      term: this.currentTerm,
      command: cmd,
    };
    this.log.push(entry);
    this.matchIndex[this.nodeId] = this.log.length;

    this.sendHeartbeats();

    return {
      success: true,
      leaderId: this.nodeId,
      leaderPort: this.config.port,
    };
  }

  readKey(key: string): ClientReadResponse {
    if (this.state !== NodeState.LEADER) {
      return {
        success: false,
        leaderId: this.currentLeader ?? undefined,
        leaderPort: this.currentLeader !== null ? this.getPortForNode(this.currentLeader) : undefined,
        error: "not leader",
      };
    }

    if (this.isMinorityLeader()) {
      return {
        success: false,
        leaderId: undefined,
        leaderPort: undefined,
        error: "not leader",
      };
    }

    const value = this.kvStore.has(key) ? this.kvStore.get(key)! : null;
    return {
      success: true,
      value,
      leaderId: this.nodeId,
      leaderPort: this.config.port,
    };
  }

  getStatus(): StatusResponse {
    return {
      nodeId: this.nodeId,
      state: this.state,
      currentTerm: this.currentTerm,
      currentLeader: this.currentLeader,
      logLength: this.log.length,
      commitIndex: this.commitIndex,
      lastApplied: this.lastApplied,
      kvSize: this.kvStore.size,
      partitioned: this.partitioned,
      partitionGroup: this.partitionGroup,
    };
  }

  setPartition(enabled: boolean, group: number[]): void {
    this.partitioned = enabled;
    this.partitionGroup = group;
  }
}

export function createServer(raftNode: RaftNode): http.Server {
  const server = http.createServer((req, res) => {
    if (req.method === "POST") {
      let body = "";
      req.on("data", (chunk) => (body += chunk));
      req.on("end", () => {
        try {
          const parsed = JSON.parse(body);
          handlePost(req.url!, parsed, res);
        } catch {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "bad request" }));
        }
      });
    } else if (req.method === "GET") {
      handleGet(req.url!, res);
    } else {
      res.writeHead(405);
      res.end();
    }
  });

  function handlePost(
    url: string,
    body: any,
    res: http.ServerResponse
  ): void {
    switch (url) {
      case "/raft/requestVote": {
        const reply = raftNode.handleRequestVote(body as RequestVoteArgs);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify(reply));
        break;
      }
      case "/raft/appendEntries": {
        const reply = raftNode.handleAppendEntries(body as AppendEntriesArgs);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify(reply));
        break;
      }
      case "/client/write": {
        const writeReq = body as ClientWriteRequest;
        const reply = raftNode.proposeCommand(writeReq);
        res.writeHead(reply.success ? 200 : 301, {
          "Content-Type": "application/json",
        });
        res.end(JSON.stringify(reply));
        break;
      }
      case "/admin/partition": {
        raftNode.setPartition(body.enabled, body.group || []);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({ ok: true, partitioned: raftNode.partitioned })
        );
        break;
      }
      case "/admin/stop": {
        raftNode.stop();
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true }));
        break;
      }
      case "/admin/start": {
        raftNode.start();
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true }));
        break;
      }
      default: {
        res.writeHead(404);
        res.end();
      }
    }
  }

  function handleGet(url: string, res: http.ServerResponse): void {
    if (url.startsWith("/client/read/")) {
      const key = decodeURIComponent(url.slice("/client/read/".length));
      const reply = raftNode.readKey(key);
      res.writeHead(reply.success ? 200 : 301, {
        "Content-Type": "application/json",
      });
      res.end(JSON.stringify(reply));
      return;
    }

    if (url === "/status") {
      const status = raftNode.getStatus();
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(status));
      return;
    }

    res.writeHead(404);
    res.end();
  }

  return server;
}

const BASE_PORT = 8001;

if (require.main === module) {
  const nodeId = parseInt(process.argv[2], 10);
  if (isNaN(nodeId) || nodeId < 0 || nodeId > 4) {
    console.error("Usage: node server.js <nodeId 0-4>");
    process.exit(1);
  }

  const port = BASE_PORT + nodeId;
  const peerPorts: number[] = [];
  for (let i = 0; i < 5; i++) {
    if (i !== nodeId) {
      peerPorts.push(BASE_PORT + i);
    }
  }

  const config: NodeConfig = {
    nodeId,
    port,
    peerPorts,
    host: "127.0.0.1",
  };

  const raftNode = new RaftNode(config);
  const server = createServer(raftNode);

  server.listen(port, () => {
    console.log(`Raft node ${nodeId} listening on port ${port}`);
  });

  raftNode.start();

  process.on("SIGINT", () => {
    raftNode.stop();
    server.close();
    process.exit(0);
  });

  process.on("SIGTERM", () => {
    raftNode.stop();
    server.close();
    process.exit(0);
  });
}

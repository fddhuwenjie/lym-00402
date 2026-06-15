import http from "http";
import fs from "fs";
import path from "path";
import {
  NodeState,
  LogEntry,
  KVCommand,
  ConfigChangeCommand,
  ClusterConfig,
  ClusterNode,
  RequestVoteArgs,
  RequestVoteReply,
  AppendEntriesArgs,
  AppendEntriesReply,
  InstallSnapshotArgs,
  InstallSnapshotReply,
  SnapshotData,
  NodeConfig,
  ClientWriteRequest,
  ClientWriteResponse,
  ClientReadResponse,
  AddMemberRequest,
  RemoveMemberRequest,
  AdminResponse,
  StatusResponse,
} from "./types";

const ELECTION_TIMEOUT_MIN = 150;
const ELECTION_TIMEOUT_MAX = 300;
const HEARTBEAT_INTERVAL = 50;
const SNAPSHOT_THRESHOLD = 100;

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
        timeout: 2000,
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

  snapshotIndex = 0;
  snapshotTerm = 0;

  clusterConfig: ClusterConfig;

  private dataDir: string;

  constructor(config: NodeConfig) {
    this.config = config;
    this.nodeId = config.nodeId;
    this.dataDir = config.dataDir || `./data/node-${config.nodeId}`;
    this.ensureDataDir();

    this.clusterConfig = {
      nodes: [
        { nodeId: config.nodeId, port: config.port },
        ...config.peerPorts.map((port, idx) => ({
          nodeId: this.getNodeIdFromPort(port),
          port,
        })),
      ].sort((a, b) => a.nodeId - b.nodeId),
    };

    this.initIndices();
    this.loadSnapshot();
  }

  private getNodeIdFromPort(port: number): number {
    const basePort = 8001;
    return port - basePort;
  }

  private ensureDataDir(): void {
    if (!fs.existsSync(this.dataDir)) {
      fs.mkdirSync(this.dataDir, { recursive: true });
    }
  }

  private get snapshotPath(): string {
    return path.join(this.dataDir, "snapshot.json");
  }

  private initIndices(): void {
    const totalNodes = this.clusterConfig.nodes.length;
    const maxNodeId = Math.max(...this.clusterConfig.nodes.map((n) => n.nodeId));
    this.nextIndex = new Array(maxNodeId + 1).fill(1);
    this.matchIndex = new Array(maxNodeId + 1).fill(0);
  }

  private getPortForNode(nodeId: number): number | null {
    const node = this.clusterConfig.nodes.find((n) => n.nodeId === nodeId);
    return node ? node.port : null;
  }

  private isClusterMember(nodeId: number): boolean {
    return this.clusterConfig.nodes.some((n) => n.nodeId === nodeId);
  }

  private clusterSize(): number {
    return this.clusterConfig.nodes.length;
  }

  private majority(): number {
    return Math.floor(this.clusterSize() / 2) + 1;
  }

  private canCommunicateWith(nodeId: number): boolean {
    if (!this.partitioned) return true;
    return this.partitionGroup.includes(nodeId);
  }

  private getLogEntry(index: number): LogEntry | null {
    if (index <= this.snapshotIndex) return null;
    const arrIndex = index - this.snapshotIndex - 1;
    if (arrIndex < 0 || arrIndex >= this.log.length) return null;
    return this.log[arrIndex];
  }

  private getLogTerm(index: number): number {
    if (index === 0) return 0;
    if (index === this.snapshotIndex) return this.snapshotTerm;
    const entry = this.getLogEntry(index);
    return entry ? entry.term : 0;
  }

  private lastLogIndex(): number {
    return this.snapshotIndex + this.log.length;
  }

  private lastLogTerm(): number {
    if (this.log.length > 0) {
      return this.log[this.log.length - 1].term;
    }
    return this.snapshotTerm;
  }

  private saveSnapshot(): void {
    const data: SnapshotData = {
      lastIncludedIndex: this.snapshotIndex,
      lastIncludedTerm: this.snapshotTerm,
      kvStore: Object.fromEntries(this.kvStore),
      clusterConfig: JSON.parse(JSON.stringify(this.clusterConfig)),
    };
    try {
      fs.writeFileSync(this.snapshotPath, JSON.stringify(data, null, 2));
    } catch (e) {
      console.error(`[Node ${this.nodeId}] Failed to save snapshot:`, e);
    }
  }

  private loadSnapshot(): void {
    try {
      if (!fs.existsSync(this.snapshotPath)) return;
      const raw = fs.readFileSync(this.snapshotPath, "utf-8");
      const data: SnapshotData = JSON.parse(raw);

      this.snapshotIndex = data.lastIncludedIndex;
      this.snapshotTerm = data.lastIncludedTerm;
      this.kvStore = new Map(Object.entries(data.kvStore));
      this.clusterConfig = data.clusterConfig;
      this.lastApplied = data.lastIncludedIndex;
      this.commitIndex = Math.max(this.commitIndex, data.lastIncludedIndex);
      this.initIndices();

      console.log(
        `[Node ${this.nodeId}] Loaded snapshot: index=${data.lastIncludedIndex}, term=${data.lastIncludedTerm}, kvSize=${this.kvStore.size}`
      );
    } catch (e) {
      console.error(`[Node ${this.nodeId}] Failed to load snapshot:`, e);
    }
  }

  private takeSnapshot(): void {
    const index = this.lastApplied;
    if (index <= this.snapshotIndex) return;

    const term = this.getLogTerm(index);
    const keepFromArrIndex = index - this.snapshotIndex;

    this.snapshotIndex = index;
    this.snapshotTerm = term;

    this.log = this.log.slice(keepFromArrIndex);

    this.saveSnapshot();

    console.log(
      `[Node ${this.nodeId}] Snapshot taken: index=${this.snapshotIndex}, term=${this.snapshotTerm}, remainingLog=${this.log.length}`
    );
  }

  private maybeSnapshot(): void {
    if (this.log.length > SNAPSHOT_THRESHOLD) {
      this.takeSnapshot();
    }
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
    this.saveSnapshot();
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

    const lastLogIdx = this.lastLogIndex();
    const lastLogTrm = this.lastLogTerm();

    const args: RequestVoteArgs = {
      term: this.currentTerm,
      candidateId: this.nodeId,
      lastLogIndex: lastLogIdx,
      lastLogTerm: lastLogTrm,
    };

    for (const node of this.clusterConfig.nodes) {
      if (node.nodeId === this.nodeId) continue;
      if (!this.canCommunicateWith(node.nodeId)) continue;

      rpcPost(this.config.host, node.port, "/raft/requestVote", args).then(
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
            if (this.votesGranted >= this.majority()) {
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

    const maxNodeId = Math.max(...this.clusterConfig.nodes.map((n) => n.nodeId));
    this.nextIndex = new Array(maxNodeId + 1).fill(this.lastLogIndex() + 1);
    this.matchIndex = new Array(maxNodeId + 1).fill(0);
    this.matchIndex[this.nodeId] = this.lastLogIndex();

    if (this.electionTimer) {
      clearTimeout(this.electionTimer);
      this.electionTimer = null;
    }

    this.sendHeartbeats();
    this.resetHeartbeatTimer();
  }

  private sendHeartbeats(): void {
    if (this.state !== NodeState.LEADER) return;

    for (const node of this.clusterConfig.nodes) {
      if (node.nodeId === this.nodeId) continue;
      this.sendAppendEntries(node.nodeId);
    }
  }

  private sendAppendEntries(peerId: number): void {
    if (this.state !== NodeState.LEADER) return;
    if (!this.canCommunicateWith(peerId)) return;

    const peerNextIndex = this.nextIndex[peerId] || 1;

    const prevLogIndex = Math.max(0, peerNextIndex - 1);
    const prevLogTerm = this.getLogTerm(Math.max(0, prevLogIndex));

    const entries: LogEntry[] = [];
    for (let i = peerNextIndex; i <= this.lastLogIndex(); i++) {
      const entry = this.getLogEntry(i);
      if (entry) entries.push(entry);
    }

    const args: AppendEntriesArgs = {
      term: this.currentTerm,
      leaderId: this.nodeId,
      prevLogIndex,
      prevLogTerm,
      entries,
      leaderCommit: this.commitIndex,
    };

    const port = this.getPortForNode(peerId);
    if (!port) return;

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
          this.nextIndex[peerId] = prevLogIndex + entries.length + 1;
          this.matchIndex[peerId] = prevLogIndex + entries.length;
          this.tryAdvanceCommit();
        } else {
          if (r.conflictIndex !== undefined && r.conflictTerm !== undefined) {
            if (r.conflictTerm === 0) {
              this.nextIndex[peerId] = Math.max(1, r.conflictIndex);
            } else {
              let found = false;
              for (let i = this.lastLogIndex(); i > this.snapshotIndex; i--) {
                if (this.getLogTerm(i) === r.conflictTerm) {
                  this.nextIndex[peerId] = i + 1;
                  found = true;
                  break;
                }
              }
              if (!found) {
                this.nextIndex[peerId] = Math.max(1, r.conflictIndex);
              }
            }
          } else {
            this.nextIndex[peerId] = Math.max(1, this.nextIndex[peerId] - 1);
          }
          
          if (this.nextIndex[peerId] <= this.snapshotIndex) {
            this.sendInstallSnapshot(peerId);
          }
        }
      }
    );
  }

  private sendInstallSnapshot(peerId: number): void {
    if (this.state !== NodeState.LEADER) return;
    if (!this.canCommunicateWith(peerId)) return;

    const data: SnapshotData = {
      lastIncludedIndex: this.snapshotIndex,
      lastIncludedTerm: this.snapshotTerm,
      kvStore: Object.fromEntries(this.kvStore),
      clusterConfig: JSON.parse(JSON.stringify(this.clusterConfig)),
    };

    const args: InstallSnapshotArgs = {
      term: this.currentTerm,
      leaderId: this.nodeId,
      lastIncludedIndex: this.snapshotIndex,
      lastIncludedTerm: this.snapshotTerm,
      data,
    };

    const port = this.getPortForNode(peerId);
    if (!port) return;

    console.log(
      `[Node ${this.nodeId}] Sending snapshot to node ${peerId}: index=${this.snapshotIndex}`
    );

    rpcPost(this.config.host, port, "/raft/installSnapshot", args).then(
      (reply) => {
        if (!reply) return;
        const r = reply as InstallSnapshotReply;

        if (r.term > this.currentTerm) {
          this.becomeFollower(r.term);
          return;
        }

        if (this.state !== NodeState.LEADER) return;

        if (r.success) {
          this.nextIndex[peerId] = this.snapshotIndex + 1;
          this.matchIndex[peerId] = this.snapshotIndex;
          this.tryAdvanceCommit();
        }
      }
    );
  }

  handleInstallSnapshot(args: InstallSnapshotArgs): InstallSnapshotReply {
    const reply: InstallSnapshotReply = {
      term: this.currentTerm,
      success: false,
    };

    if (args.term < this.currentTerm) {
      return reply;
    }

    if (args.term > this.currentTerm) {
      this.becomeFollower(args.term);
    }

    this.currentLeader = args.leaderId;
    this.resetElectionTimer();

    if (args.lastIncludedIndex <= this.snapshotIndex) {
      reply.success = true;
      return reply;
    }

    this.snapshotIndex = args.data.lastIncludedIndex;
    this.snapshotTerm = args.data.lastIncludedTerm;
    this.kvStore = new Map(Object.entries(args.data.kvStore));
    this.clusterConfig = args.data.clusterConfig;
    this.lastApplied = args.data.lastIncludedIndex;
    this.commitIndex = Math.max(this.commitIndex, args.data.lastIncludedIndex);
    this.log = [];
    this.initIndices();

    this.saveSnapshot();

    reply.term = this.currentTerm;
    reply.success = true;

    console.log(
      `[Node ${this.nodeId}] Installed snapshot: index=${this.snapshotIndex}, term=${this.snapshotTerm}, kvSize=${this.kvStore.size}`
    );

    return reply;
  }

  private tryAdvanceCommit(): void {
    for (let n = this.commitIndex + 1; n <= this.lastLogIndex(); n++) {
      if (this.getLogTerm(n) !== this.currentTerm) continue;

      let replicated = 1;
      for (const node of this.clusterConfig.nodes) {
        if (node.nodeId === this.nodeId) continue;
        if (this.matchIndex[node.nodeId] >= n) replicated++;
      }

      if (replicated >= this.majority()) {
        this.commitIndex = n;
      }
    }

    this.applyCommitted();
  }

  private applyCommitted(): void {
    while (this.lastApplied < this.commitIndex) {
      this.lastApplied += 1;
      const entry = this.getLogEntry(this.lastApplied);
      if (entry && entry.command) {
        this.applyCommand(entry.command);
      }
    }
    this.maybeSnapshot();
  }

  private applyCommand(cmd: KVCommand | ConfigChangeCommand): void {
    if ("op" in cmd && (cmd as KVCommand).op === "set") {
      const kvCmd = cmd as KVCommand;
      if (kvCmd.value !== undefined) {
        this.kvStore.set(kvCmd.key, kvCmd.value);
      }
    } else if ("op" in cmd && (cmd as KVCommand).op === "delete") {
      const kvCmd = cmd as KVCommand;
      this.kvStore.delete(kvCmd.key);
    } else if ("op" in cmd && (cmd as ConfigChangeCommand).op === "config_change") {
      const cfgCmd = cmd as ConfigChangeCommand;
      this.applyConfigChange(cfgCmd.newConfig);
    }
  }

  private applyConfigChange(newConfig: ClusterConfig): void {
    console.log(
      `[Node ${this.nodeId}] Applying config change: ${newConfig.nodes.map(n => `node${n.nodeId}:${n.port}`).join(', ')}`
    );
    this.clusterConfig = newConfig;
    
    const maxNodeId = Math.max(...this.clusterConfig.nodes.map((n) => n.nodeId));
    
    const oldNextIndex = [...this.nextIndex];
    const oldMatchIndex = [...this.matchIndex];
    
    this.nextIndex = new Array(maxNodeId + 1).fill(this.lastLogIndex() + 1);
    this.matchIndex = new Array(maxNodeId + 1).fill(0);
    
    for (const node of this.clusterConfig.nodes) {
      if (node.nodeId < oldNextIndex.length) {
        this.nextIndex[node.nodeId] = oldNextIndex[node.nodeId] || this.lastLogIndex() + 1;
        this.matchIndex[node.nodeId] = oldMatchIndex[node.nodeId] || 0;
      }
    }
    this.matchIndex[this.nodeId] = this.lastLogIndex();
  }

  handleRequestVote(args: RequestVoteArgs): RequestVoteReply {
    if (this.partitioned && !this.partitionGroup.includes(args.candidateId)) {
      return { term: this.currentTerm, voteGranted: false };
    }

    if (!this.isClusterMember(args.candidateId) && args.candidateId !== this.nodeId) {
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

    const lastIdx = this.lastLogIndex();
    const lastTrm = this.lastLogTerm();

    const candidateUpToDate =
      args.lastLogTerm > lastTrm ||
      (args.lastLogTerm === lastTrm && args.lastLogIndex >= lastIdx);

    if (!candidateUpToDate) {
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

    if (args.prevLogIndex < this.snapshotIndex) {
      reply.success = false;
      reply.conflictIndex = this.snapshotIndex + 1;
      reply.conflictTerm = 0;
      return reply;
    }

    if (args.prevLogIndex > 0) {
      const prevTerm = this.getLogTerm(args.prevLogIndex);
      if (args.prevLogIndex > this.lastLogIndex() || prevTerm !== args.prevLogTerm) {
        if (args.prevLogIndex > this.lastLogIndex()) {
          reply.conflictIndex = this.lastLogIndex() + 1;
          reply.conflictTerm = 0;
        } else {
          const conflictTerm = this.getLogTerm(args.prevLogIndex);
          let conflictIndex = this.snapshotIndex + 1;
          for (let i = this.snapshotIndex + 1; i <= this.lastLogIndex(); i++) {
            if (this.getLogTerm(i) === conflictTerm) {
              conflictIndex = i;
              break;
            }
          }
          reply.conflictIndex = conflictIndex;
          reply.conflictTerm = conflictTerm;
        }
        return reply;
      }
    }

    for (let i = 0; i < args.entries.length; i++) {
      const logIndex = args.prevLogIndex + 1 + i;
      
      if (logIndex <= this.snapshotIndex) continue;

      if (logIndex <= this.lastLogIndex()) {
        const existing = this.getLogEntry(logIndex);
        if (existing && existing.term !== args.entries[i].term) {
          const truncateIdx = logIndex - this.snapshotIndex - 1;
          this.log = this.log.slice(0, Math.max(0, truncateIdx));
          this.log.push(args.entries[i]);
        }
      } else {
        this.log.push(args.entries[i]);
      }
    }

    if (args.leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(args.leaderCommit, this.lastLogIndex());
      this.applyCommitted();
    }

    reply.success = true;
    return reply;
  }

  private isMinorityLeader(): boolean {
    if (!this.partitioned) return false;
    return this.partitionGroup.length < this.majority();
  }

  proposeCommand(cmd: KVCommand): ClientWriteResponse {
    if (this.state !== NodeState.LEADER) {
      return {
        success: false,
        leaderId: this.currentLeader ?? undefined,
        leaderPort: this.currentLeader !== null ? this.getPortForNode(this.currentLeader) ?? undefined : undefined,
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
    this.matchIndex[this.nodeId] = this.lastLogIndex();

    this.sendHeartbeats();

    return {
      success: true,
      leaderId: this.nodeId,
      leaderPort: this.config.port,
    };
  }

  addMember(req: AddMemberRequest): AdminResponse {
    if (this.state !== NodeState.LEADER) {
      return {
        success: false,
        error: "not leader",
      };
    }

    if (this.isClusterMember(req.nodeId)) {
      return {
        success: false,
        error: "node already in cluster",
      };
    }

    const newConfig: ClusterConfig = {
      nodes: [
        ...this.clusterConfig.nodes,
        { nodeId: req.nodeId, port: req.port },
      ].sort((a, b) => a.nodeId - b.nodeId),
    };

    const configCmd: ConfigChangeCommand = {
      op: "config_change",
      newConfig,
    };

    const entry: LogEntry = {
      term: this.currentTerm,
      command: configCmd,
    };
    this.log.push(entry);
    this.matchIndex[this.nodeId] = this.lastLogIndex();

    this.applyConfigChange(newConfig);

    const maxNodeId = Math.max(...this.clusterConfig.nodes.map((n) => n.nodeId));
    if (this.nextIndex.length <= maxNodeId) {
      const newNext = new Array(maxNodeId + 1).fill(this.lastLogIndex() + 1);
      const newMatch = new Array(maxNodeId + 1).fill(0);
      for (let i = 0; i < this.nextIndex.length; i++) {
        newNext[i] = this.nextIndex[i];
        newMatch[i] = this.matchIndex[i];
      }
      this.nextIndex = newNext;
      this.matchIndex = newMatch;
      this.matchIndex[this.nodeId] = this.lastLogIndex();
    }
    this.nextIndex[req.nodeId] = 1;
    this.matchIndex[req.nodeId] = 0;

    this.sendHeartbeats();

    console.log(
      `[Node ${this.nodeId}] Proposed add-member: node ${req.nodeId}:${req.port}`
    );

    return { success: true };
  }

  removeMember(req: RemoveMemberRequest): AdminResponse {
    if (this.state !== NodeState.LEADER) {
      return {
        success: false,
        error: "not leader",
      };
    }

    if (!this.isClusterMember(req.nodeId)) {
      return {
        success: false,
        error: "node not in cluster",
      };
    }

    if (req.nodeId === this.nodeId) {
      return {
        success: false,
        error: "cannot remove self",
      };
    }

    const newConfig: ClusterConfig = {
      nodes: this.clusterConfig.nodes.filter((n) => n.nodeId !== req.nodeId),
    };

    const configCmd: ConfigChangeCommand = {
      op: "config_change",
      newConfig,
    };

    const entry: LogEntry = {
      term: this.currentTerm,
      command: configCmd,
    };
    this.log.push(entry);
    this.matchIndex[this.nodeId] = this.lastLogIndex();

    this.applyConfigChange(newConfig);

    this.sendHeartbeats();

    console.log(
      `[Node ${this.nodeId}] Proposed remove-member: node ${req.nodeId}`
    );

    return { success: true };
  }

  readKey(key: string): ClientReadResponse {
    if (this.state !== NodeState.LEADER) {
      return {
        success: false,
        leaderId: this.currentLeader ?? undefined,
        leaderPort: this.currentLeader !== null ? this.getPortForNode(this.currentLeader) ?? undefined : undefined,
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
      snapshotIndex: this.snapshotIndex,
      snapshotTerm: this.snapshotTerm,
      clusterSize: this.clusterSize(),
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
      case "/raft/installSnapshot": {
        const reply = raftNode.handleInstallSnapshot(body as InstallSnapshotArgs);
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
      case "/admin/add-member": {
        const reply = raftNode.addMember(body as AddMemberRequest);
        res.writeHead(reply.success ? 200 : 400, {
          "Content-Type": "application/json",
        });
        res.end(JSON.stringify(reply));
        break;
      }
      case "/admin/remove-member": {
        const reply = raftNode.removeMember(body as RemoveMemberRequest);
        res.writeHead(reply.success ? 200 : 400, {
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
const TOTAL_NODES = 5;

if (require.main === module) {
  const nodeId = parseInt(process.argv[2], 10);
  if (isNaN(nodeId) || nodeId < 0 || nodeId > 9) {
    console.error("Usage: node raft.js <nodeId>");
    process.exit(1);
  }

  const port = BASE_PORT + nodeId;
  const peerPorts: number[] = [];
  for (let i = 0; i < TOTAL_NODES; i++) {
    if (i !== nodeId) {
      peerPorts.push(BASE_PORT + i);
    }
  }

  const config: NodeConfig = {
    nodeId,
    port,
    peerPorts,
    host: "127.0.0.1",
    dataDir: `./data/node-${nodeId}`,
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

export enum NodeState {
  FOLLOWER = "follower",
  CANDIDATE = "candidate",
  LEADER = "leader",
}

export interface LogEntry {
  term: number;
  command: KVCommand | ConfigChangeCommand | null;
}

export interface KVCommand {
  op: "set" | "delete";
  key: string;
  value?: string;
}

export interface ConfigChangeCommand {
  op: "config_change";
  newConfig: ClusterConfig;
}

export interface ClusterConfig {
  nodes: ClusterNode[];
}

export interface ClusterNode {
  nodeId: number;
  port: number;
}

export interface RequestVoteArgs {
  term: number;
  candidateId: number;
  lastLogIndex: number;
  lastLogTerm: number;
}

export interface RequestVoteReply {
  term: number;
  voteGranted: boolean;
}

export interface AppendEntriesArgs {
  term: number;
  leaderId: number;
  prevLogIndex: number;
  prevLogTerm: number;
  entries: LogEntry[];
  leaderCommit: number;
}

export interface AppendEntriesReply {
  term: number;
  success: boolean;
  conflictIndex?: number;
  conflictTerm?: number;
}

export interface InstallSnapshotArgs {
  term: number;
  leaderId: number;
  lastIncludedIndex: number;
  lastIncludedTerm: number;
  data: SnapshotData;
}

export interface InstallSnapshotReply {
  term: number;
  success: boolean;
}

export interface SnapshotData {
  lastIncludedIndex: number;
  lastIncludedTerm: number;
  kvStore: Record<string, string>;
  clusterConfig: ClusterConfig;
}

export interface NodeConfig {
  nodeId: number;
  port: number;
  peerPorts: number[];
  host: string;
  dataDir?: string;
}

export interface ClientWriteRequest {
  op: "set" | "delete";
  key: string;
  value?: string;
}

export interface ClientWriteResponse {
  success: boolean;
  leaderId?: number;
  leaderPort?: number;
  error?: string;
}

export interface ClientReadResponse {
  success: boolean;
  value?: string | null;
  leaderId?: number;
  leaderPort?: number;
  error?: string;
}

export interface AddMemberRequest {
  nodeId: number;
  port: number;
}

export interface RemoveMemberRequest {
  nodeId: number;
}

export interface AdminResponse {
  success: boolean;
  error?: string;
}

export interface StatusResponse {
  nodeId: number;
  state: NodeState;
  currentTerm: number;
  currentLeader: number | null;
  logLength: number;
  commitIndex: number;
  lastApplied: number;
  kvSize: number;
  partitioned: boolean;
  partitionGroup: number[];
  snapshotIndex: number;
  snapshotTerm: number;
  clusterSize: number;
}

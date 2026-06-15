export enum NodeState {
  FOLLOWER = "follower",
  CANDIDATE = "candidate",
  LEADER = "leader",
}

export interface LogEntry {
  term: number;
  command: KVCommand | null;
}

export interface KVCommand {
  op: "set" | "delete";
  key: string;
  value?: string;
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

export interface NodeConfig {
  nodeId: number;
  port: number;
  peerPorts: number[];
  host: string;
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
}

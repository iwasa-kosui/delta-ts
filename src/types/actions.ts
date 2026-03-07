export interface DeletionVector {
  storageType: string;
  pathOrInlineDv: string;
  offset?: number;
  sizeInBytes: number;
  cardinality: number;
}

export interface FileStatistics {
  numRecords: number;
  minValues?: Record<string, unknown>;
  maxValues?: Record<string, unknown>;
  nullCount?: Record<string, unknown>;
}

export interface AddAction {
  path: string;
  partitionValues: Record<string, string | null>;
  size: number;
  modificationTime: number;
  dataChange: boolean;
  stats?: string;
  parsedStats?: FileStatistics;
  tags?: Record<string, string>;
  deletionVector?: DeletionVector;
}

export interface RemoveAction {
  path: string;
  deletionTimestamp?: number;
  dataChange: boolean;
  extendedFileMetadata?: boolean;
  partitionValues?: Record<string, string | null>;
  size?: number;
  deletionVector?: DeletionVector;
}

export interface MetadataAction {
  id: string;
  name?: string;
  description?: string;
  format: {
    provider: string;
    options?: Record<string, string>;
  };
  schemaString: string;
  partitionColumns: string[];
  configuration: Record<string, string>;
  createdTime?: number;
}

export interface ProtocolAction {
  minReaderVersion: number;
  minWriterVersion: number;
  readerFeatures?: string[];
  writerFeatures?: string[];
}

export interface CommitInfoAction {
  timestamp: number;
  operation: string;
  operationParameters?: Record<string, unknown>;
  readVersion?: number;
  isolationLevel?: string;
  isBlindAppend?: boolean;
  operationMetrics?: Record<string, string>;
  engineInfo?: string;
  txnId?: string;
}

export interface TxnAction {
  appId: string;
  version: number;
  lastUpdated?: number;
}

export interface DomainMetadataAction {
  domain: string;
  configuration: string;
  removed: boolean;
}

export type DeltaAction =
  | { readonly type: "add"; readonly add: AddAction }
  | { readonly type: "remove"; readonly remove: RemoveAction }
  | { readonly type: "metaData"; readonly metaData: MetadataAction }
  | { readonly type: "protocol"; readonly protocol: ProtocolAction }
  | { readonly type: "commitInfo"; readonly commitInfo: CommitInfoAction }
  | { readonly type: "txn"; readonly txn: TxnAction }
  | { readonly type: "domainMetadata"; readonly domainMetadata: DomainMetadataAction };

const ACTION_KEYS = [
  "add",
  "remove",
  "metaData",
  "protocol",
  "commitInfo",
  "txn",
  "domainMetadata",
] as const;

export const DeltaAction = {
  parse: (raw: Record<string, unknown>): DeltaAction | null => {
    for (const key of ACTION_KEYS) {
      if (raw[key] && typeof raw[key] === "object") {
        return { type: key, [key]: raw[key] } as unknown as DeltaAction;
      }
    }
    return null;
  },
};

export interface LastCheckpointInfo {
  version: number;
  size: number;
  parts?: number;
  sizeInBytes?: number;
  numOfAddFiles?: number;
}

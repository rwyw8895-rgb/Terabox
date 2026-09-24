export interface ProcessedFile {
  filename: string;
  sizeBytes: number;
  sizeFormatted: string;
  path?: string;
  isVideo: boolean;
  isZip: boolean;
  mimeType?: string;
  directUrl?: string;
  downloadUrl?: string;
  splitPartsCount?: number;
}

export interface DownloadJob {
  id: string;
  url: string;
  linkCounter?: number;
  status: 'pending' | 'resolving' | 'downloading' | 'unpacking' | 'uploading' | 'completed' | 'failed';
  progress: number;
  statusText: string;
  files: ProcessedFile[];
  chatId?: string;
  error?: string;
  retryCount?: number;
  maxRetries?: number;
  createdAt: number;
  completedAt?: number;
  logs: string[];
}

export interface BotStatus {
  hasToken: boolean;
  tokenMasked?: string;
  isOnline: boolean;
  isPolling: boolean;
  botInfo?: {
    id: number;
    username: string;
    first_name: string;
    can_join_groups: boolean;
  };
  hasApiCredentials: boolean;
  uploadLimitMb: number;
  dataDir: string;
  totalJobsCount: number;
  completedJobsCount: number;
}

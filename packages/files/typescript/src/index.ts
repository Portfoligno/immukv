/**
 * ImmuKV File Storage - File storage with audit logging using ImmuKV.
 */

export {
  FileClient,
  createFileClient,
  fileValueDecoder,
  fileValueEncoder,
} from "./client";

export {
  // Branded types
  type ContentHash,
  type FileVersionId,
  type FileS3Key,
  // Config types
  type FileStorageConfig,
  // Metadata types
  type FileMetadata,
  type DeletedFileMetadata,
  type FileValue,
  type FileEntry,
  // Options types
  type SetFileOptions,
  type GetFileOptions,
  type FileDownload,
  // Type guards
  isDeletedFile,
  isActiveFile,
  // Error types
  FileNotFoundError,
  FileDeletedError,
  IntegrityError,
  FileOrphanedError,
  ConfigurationError,
  MaxRetriesExceededError,
} from "./types";

export const VERSION = "__VERSION_EeEyfbyVyf4JmFfk__";

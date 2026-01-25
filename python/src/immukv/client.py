"""ImmuKV async client implementation."""

from __future__ import annotations

import json
import logging
import time
from contextlib import asynccontextmanager
from typing import (
    AsyncIterator,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    TypeVar,
    cast,
)

from botocore.exceptions import ClientError

from immukv._internal.async_s3_client import AsyncBrandedS3Client, create_async_s3_client
from immukv._internal.json_helpers import (
    dumps_canonical,
    entry_from_key_object,
    entry_from_log,
    get_int,
    get_str,
    strip_none_values,
)
from immukv._internal.types import (
    JSONValue,
    KeyObjectDict,
    LatestLogState,
    LogEntryDict,
    LogEntryForHash,
    OrphanStatus,
    hash_compute,
    hash_from_json,
    hash_genesis,
    sequence_from_json,
    sequence_initial,
    sequence_next,
    timestamp_now,
)
from immukv.json_helpers import ValueDecoder, ValueEncoder
from immukv._internal.s3_helpers import get_error_code
from immukv._internal.s3_types import (
    HeadObjectOutputs,
    LogKey,
    ObjectVersions,
    PutObjectOutputs,
    S3KeyPath,
    S3KeyPaths,
)
from immukv.types import (
    Config,
    Entry,
    Hash,
    KeyNotFoundError,
    KeyObjectETag,
    KeyVersionId,
    LogVersionId,
    ReadOnlyError,
    Sequence,
    TimestampMs,
)

logger = logging.getLogger(__name__)

# Type variables for generic key and value types
K = TypeVar("K", bound=str)
K2 = TypeVar("K2", bound=str)
V = TypeVar("V")
V2 = TypeVar("V2")


class ImmuKVClient(Generic[K, V]):
    """Async-only client for ImmuKV immutable key-value store.

    All public methods that perform S3 I/O are async coroutines.
    Uses aiobotocore for non-blocking S3 operations.

    Type Parameters:
        K: Key type (must be subtype of str)
        V: Value type (user-defined, encoded/decoded via codecs)

    Usage:
        async with ImmuKVClient.create(config, decoder, encoder) as client:
            entry = await client.set("sensor-01", {"temp": 20.5})
            result = await client.get("sensor-01")
            history, cursor = await client.history("sensor-01")

    Two-Phase Write Protocol:
        1. Pre-flight: Repair orphaned entry if detected
        2. Phase 1: Append to _log.json (ETag-based optimistic locking)
        3. Phase 2: Update keys/{key}.json (best effort, may orphan)
    """

    # Instance fields
    _config: Config
    _s3: "AsyncBrandedS3Client[str]"
    _log_key: S3KeyPath[LogKey]
    _value_decoder: ValueDecoder[V]
    _value_encoder: ValueEncoder[V]
    _last_repair_check_ms: int
    _can_write: Optional[bool]
    _latest_orphan_status: Optional[OrphanStatus[K, V]]
    _owns_session: bool

    def __init__(
        self,
        config: Config,
        value_decoder: ValueDecoder[V],
        value_encoder: ValueEncoder[V],
        s3_client: "AsyncBrandedS3Client[str]",
        owns_session: bool = True,
    ) -> None:
        """Initialize client (internal - prefer create() factory).

        Args:
            config: S3 bucket and prefix configuration
            value_decoder: Transforms JSONValue to user's V type
            value_encoder: Transforms user's V type to JSONValue
            s3_client: Async S3 client wrapper
            owns_session: Whether this client owns the aiobotocore session
        """
        self._config = config
        self._value_decoder = value_decoder
        self._value_encoder = value_encoder
        self._s3 = s3_client
        self._owns_session = owns_session
        self._log_key = cast(S3KeyPath[LogKey], S3KeyPaths.for_log(config.s3_prefix))
        self._last_repair_check_ms = 0
        self._can_write = None
        self._latest_orphan_status = None

    # =========================================================================
    # Factory Methods
    # =========================================================================

    @classmethod
    @asynccontextmanager
    async def create(
        cls,
        config: Config,
        value_decoder: ValueDecoder[V],
        value_encoder: ValueEncoder[V],
    ) -> AsyncIterator["ImmuKVClient[K, V]"]:
        """Create an async ImmuKVClient with managed lifecycle.

        This is the recommended way to create a client. The aiobotocore
        session is automatically cleaned up when the context exits.

        Args:
            config: S3 bucket and prefix configuration
            value_decoder: Transforms JSONValue to user's V type
            value_encoder: Transforms user's V type to JSONValue

        Yields:
            Configured ImmuKVClient ready for use

        Example:
            async with ImmuKVClient.create(config, decoder, encoder) as client:
                entry = await client.set("key", value)
                result = await client.get("key")
        """
        async with create_async_s3_client(config) as s3_client:
            yield cls(config, value_decoder, value_encoder, s3_client, owns_session=True)

    # =========================================================================
    # Core Write Operations
    # =========================================================================

    async def set(self, key: K, value: V) -> Entry[K, V]:
        """Write a new immutable entry for the given key.

        Executes the two-phase write protocol:
        1. Pre-flight: Detect and repair any orphaned entry
        2. Phase 1: Append entry to global log (commit point)
        3. Phase 2: Update key object for fast reads (best effort)

        Args:
            key: The key to write
            value: The value to store (will be encoded via value_encoder)

        Returns:
            Entry object with metadata (hash, sequence, timestamps, version IDs)

        Raises:
            ReadOnlyError: If client is in read-only mode
            Exception: If optimistic locking fails after max retries

        Note:
            Returns successfully even if Phase 2 fails. The entry always
            exists in the log. Orphaned entries are auto-repaired on next
            read operation.
        """
        # Check read-only mode at entry
        if self._config.read_only:
            raise ReadOnlyError("Cannot call set() in read-only mode")

        # Retry loop for optimistic locking on log writes
        max_retries = 10
        for attempt in range(max_retries):
            # ===== Pre-Flight: Repair (with ETag) =====
            result = await self._get_latest_and_repair()
            log_etag = result["log_etag"]
            prev_version_id = result["prev_version_id"]
            prev_hash = result["prev_hash"]
            sequence = result["sequence"]
            can_write = result["can_write"]
            orphan_status = result["orphan_status"]

            # Update cached state
            if can_write is not None:
                self._can_write = can_write
            if orphan_status is not None:
                self._latest_orphan_status = orphan_status
            self._last_repair_check_ms = int(time.time() * 1000)

            # ===== Write Phase 1: Append to Global Log (with optimistic locking) =====

            # Step 1: Get current key object ETag (for storing in log entry)
            key_path = S3KeyPaths.for_key(self._config.s3_prefix, key)
            current_key_etag: Optional[KeyObjectETag[K]] = None
            try:
                current_key = await self._s3.head_object(
                    bucket=self._config.s3_bucket, key=key_path
                )
                current_key_etag = HeadObjectOutputs.key_object_etag(current_key)
            except ClientError as e:  # type: ignore[misc]
                if get_error_code(e) in ["NoSuchKey", "404"]:
                    current_key_etag = None
                else:
                    raise

            # Step 2: Create new log entry
            new_sequence: Sequence[K] = (
                sequence_next(sequence) if sequence is not None else sequence_from_json(0)
            )
            timestamp_ms: TimestampMs[K] = timestamp_now()

            # Step 3: Encode value and calculate hash
            encoded_value: JSONValue = self._value_encoder(value)
            entry_for_hash: LogEntryForHash[K, JSONValue] = {
                "sequence": new_sequence,
                "key": key,
                "value": encoded_value,
                "timestamp_ms": timestamp_ms,
                "previous_hash": prev_hash,
            }
            entry_hash = self._calculate_hash(entry_for_hash)

            # Step 4: Create complete log entry (with current key object ETag)
            log_entry: LogEntryDict = {
                "sequence": new_sequence,
                "key": key,
                "value": encoded_value,
                "timestamp_ms": timestamp_ms,
                "previous_version_id": prev_version_id,
                "previous_hash": prev_hash,
                "hash": entry_hash,
                "previous_key_object_etag": current_key_etag,
            }

            # Step 5: Write to log with optimistic locking
            try:
                # Strip None values to match TypeScript's undefined behavior (omits field from JSON)
                log_entry_for_json = strip_none_values(cast(Dict[str, JSONValue], log_entry))

                if log_etag is not None:
                    # Update existing log - use IfMatch
                    response = await self._s3.put_object(
                        bucket=self._config.s3_bucket,
                        key=self._log_key,
                        body=dumps_canonical(cast(JSONValue, log_entry_for_json)),
                        content_type="application/json",
                        if_match=log_etag,
                    )
                else:
                    # First write - use if_none_match='*'
                    response = await self._s3.put_object(
                        bucket=self._config.s3_bucket,
                        key=self._log_key,
                        body=dumps_canonical(cast(JSONValue, log_entry_for_json)),
                        content_type="application/json",
                        if_none_match="*",
                    )

                new_log_version_id_opt: Optional[LogVersionId[K]] = PutObjectOutputs.log_version_id(
                    response
                )
                if new_log_version_id_opt is None:
                    raise ValueError(
                        "S3 response missing VersionId - versioning must be enabled on bucket"
                    )
                new_log_version_id: LogVersionId[K] = new_log_version_id_opt
                break  # Committed to log! Exit retry loop

            except ClientError as e:  # type: ignore[misc]
                if get_error_code(e) == "PreconditionFailed":
                    logger.debug(f"Log write conflict, retry {attempt + 1}/{max_retries}")
                    continue
                else:
                    raise

        else:
            raise Exception(f"Failed to write log after {max_retries} retries")

        # ===== Write Phase 2: Write Key Object (with conditional write) =====

        key_object_etag: Optional[KeyObjectETag[K]] = None
        try:
            # Create key object data - INCLUDES ALL FIELDS FROM LOG ENTRY
            key_data: KeyObjectDict = {
                "sequence": new_sequence,
                "key": key,
                "value": encoded_value,
                "timestamp_ms": timestamp_ms,
                "log_version_id": new_log_version_id,
                "hash": entry_hash,
                "previous_hash": prev_hash,
            }

            if current_key_etag is not None:
                # UPDATE existing key object - use IfMatch
                response = await self._s3.put_object(
                    bucket=self._config.s3_bucket,
                    key=key_path,
                    body=dumps_canonical(cast(JSONValue, key_data)),
                    content_type="application/json",
                    if_match=current_key_etag,
                )
                key_object_etag = PutObjectOutputs.key_object_etag(response)
            else:
                # CREATE new key object - use if_none_match='*'
                response = await self._s3.put_object(
                    bucket=self._config.s3_bucket,
                    key=key_path,
                    body=dumps_canonical(cast(JSONValue, key_data)),
                    content_type="application/json",
                    if_none_match="*",
                )
                key_object_etag = PutObjectOutputs.key_object_etag(response)

        except Exception as e:
            logger.warning(
                f"Failed to write key object for {key} (log version {new_log_version_id}): {e}. "
                "Entry committed to log but key object missing (orphaned temporarily)."
            )

        # Step 6: Return Entry
        return Entry(
            key=key,
            value=value,
            timestamp_ms=timestamp_ms,
            version_id=new_log_version_id,
            sequence=new_sequence,
            previous_version_id=prev_version_id,
            hash=entry_hash,
            previous_hash=prev_hash,
            previous_key_object_etag=key_object_etag,
        )

    # =========================================================================
    # Core Read Operations
    # =========================================================================

    async def get(self, key: K) -> Entry[K, V]:
        """Get the latest entry for a key.

        Fast path: Single S3 read from key object.
        With repair check: May also check log for orphaned entries.

        Args:
            key: The key to retrieve

        Returns:
            Entry with the latest value

        Raises:
            KeyNotFoundError: If key does not exist
        """
        # Conditional orphan check based on time interval
        current_time_ms = int(time.time() * 1000)
        time_since_last_check = current_time_ms - self._last_repair_check_ms

        # Check if we need to perform orphan repair check
        if time_since_last_check >= self._config.repair_check_interval_ms:
            # Skip repair attempt if we know we're read-only
            if not (self._can_write is False or self._config.read_only):
                # Perform orphan check and repair
                result = await self._get_latest_and_repair()
                if result["can_write"] is not None:
                    self._can_write = result["can_write"]
                if result["orphan_status"] is not None:
                    self._latest_orphan_status = result["orphan_status"]
                self._last_repair_check_ms = current_time_ms

        # Try to read from key object
        key_path = S3KeyPaths.for_key(self._config.s3_prefix, key)
        try:
            response = await self._s3.get_object(bucket=self._config.s3_bucket, key=key_path)
            data = _read_body_as_json(response["Body"])
            return entry_from_key_object(data, self._value_decoder)

        except ClientError as e:  # type: ignore[misc]
            if get_error_code(e) in ["NoSuchKey", "404"]:
                # Key object doesn't exist - check for orphan fallback
                if (
                    self._latest_orphan_status is not None
                    and self._latest_orphan_status.get("is_orphaned") is True
                    and self._latest_orphan_status.get("orphan_key") == key
                    and self._latest_orphan_status.get("orphan_entry") is not None
                    and (self._can_write is False or self._config.read_only)
                ):
                    # Return cached orphan entry (read-only mode)
                    return self._latest_orphan_status["orphan_entry"]  # type: ignore

                raise KeyNotFoundError(f"Key '{key}' not found")
            else:
                raise

    async def get_log_version(self, version_id: LogVersionId[K]) -> Entry[K, V]:
        """Get a specific log entry by S3 version ID.

        Useful for retrieving entries referenced in history or for
        debugging/auditing purposes.

        Args:
            version_id: The S3 version ID of the log entry

        Returns:
            Entry from the specified log version

        Raises:
            KeyNotFoundError: If version does not exist
        """
        try:
            response = await self._s3.get_object(
                bucket=self._config.s3_bucket, key=self._log_key, version_id=version_id
            )
            data = _read_body_as_json(response["Body"])
            return entry_from_log(data, version_id, self._value_decoder)
        except ClientError as e:  # type: ignore[misc]
            if get_error_code(e) in ["NoSuchKey", "NoSuchVersion", "404"]:
                raise KeyNotFoundError(f"Log version '{version_id}' not found")
            raise

    async def history(
        self,
        key: K,
        before_version_id: Optional[KeyVersionId[K]] = None,
        limit: Optional[int] = None,
    ) -> Tuple[List[Entry[K, V]], Optional[KeyVersionId[K]]]:
        """Get all entries for a key in descending order (newest first).

        Orphan-aware: Prepends orphaned entry if present (not yet in key object).

        Args:
            key: The key to retrieve history for
            before_version_id: Pagination cursor (exclusive). Pass the
                oldest_version_id from previous call to get next page.
            limit: Maximum entries to return. None for unlimited.

        Returns:
            Tuple of (entries, oldest_key_version_id for pagination).
            If oldest_key_version_id is None, no more pages exist.
        """
        key_path = S3KeyPaths.for_key(self._config.s3_prefix, key)
        entries: List[Entry[K, V]] = []

        # Check if we should prepend orphan entry
        prepend_orphan = False
        if (
            before_version_id is None
            and self._latest_orphan_status is not None
            and self._latest_orphan_status.get("is_orphaned") is True
            and self._latest_orphan_status.get("orphan_key") == key
            and self._latest_orphan_status.get("orphan_entry") is not None
        ):
            prepend_orphan = True
            entries.append(self._latest_orphan_status["orphan_entry"])  # type: ignore

        # List versions of key object
        try:
            key_marker: Optional[str] = None
            version_id_marker: Optional[str] = before_version_id
            last_key_version_id: Optional[KeyVersionId[K]] = None

            while True:
                page = await self._s3.list_object_versions(
                    bucket=self._config.s3_bucket,
                    prefix=key_path,
                    key_marker=S3KeyPath(key_marker) if key_marker is not None else None,
                    version_id_marker=version_id_marker,
                )
                versions_result = page.get("Versions")
                versions = versions_result if versions_result is not None else []
                for version in versions:
                    if version["Key"] != key_path:
                        continue

                    # Skip the before_version_id itself
                    if before_version_id is not None and version["VersionId"] == before_version_id:
                        continue

                    # Fetch version data
                    key_version_id: KeyVersionId[K] = ObjectVersions.key_version_id(version)
                    response = await self._s3.get_object(
                        bucket=self._config.s3_bucket,
                        key=key_path,
                        version_id=key_version_id,
                    )
                    data = _read_body_as_json(response["Body"])
                    entry: Entry[K, V] = entry_from_key_object(data, self._value_decoder)
                    entries.append(entry)
                    last_key_version_id = key_version_id

                    # Check limit
                    if limit is not None and len(entries) >= limit:
                        return (entries, last_key_version_id)

                # Check if more pages
                if not page.get("IsTruncated", False):
                    break

                key_marker = page.get("NextKeyMarker")
                version_id_marker = page.get("NextVersionIdMarker")

        except ClientError as e:  # type: ignore[misc]
            if get_error_code(e) in ["NoSuchKey", "404"]:
                # No key object exists - return orphan if available
                if prepend_orphan:
                    return (entries, None)
                return ([], None)
            raise

        # Return all entries (or empty if none found)
        oldest_key_version_id: Optional[KeyVersionId[K]] = (
            last_key_version_id if entries and not prepend_orphan else None
        )
        return (entries, oldest_key_version_id)

    async def log_entries(
        self,
        before_version_id: Optional[LogVersionId[K]] = None,
        limit: Optional[int] = None,
    ) -> List[Entry[K, V]]:
        """Get entries from the global log in descending order (newest first).

        Returns entries across all keys in the order they were written.
        Useful for replication, audit trails, or rebuilding state.

        Args:
            before_version_id: Pagination cursor (exclusive).
            limit: Maximum entries to return. None for unlimited.

        Returns:
            List of entries in descending chronological order
        """
        entries: List[Entry[K, V]] = []

        try:
            key_marker: Optional[str] = None
            version_id_marker: Optional[str] = before_version_id

            while True:
                page = await self._s3.list_object_versions(
                    bucket=self._config.s3_bucket,
                    prefix=self._log_key,
                    key_marker=S3KeyPath(key_marker) if key_marker is not None else None,
                    version_id_marker=version_id_marker,
                )
                versions_result = page.get("Versions")
                versions = versions_result if versions_result is not None else []
                for version in versions:
                    if version["Key"] != self._log_key:
                        continue

                    # Skip the before_version_id itself
                    if before_version_id is not None and version["VersionId"] == before_version_id:
                        continue

                    # Fetch version data
                    response = await self._s3.get_object(
                        bucket=self._config.s3_bucket,
                        key=self._log_key,
                        version_id=version["VersionId"],
                    )
                    data = _read_body_as_json(response["Body"])
                    version_id_log: LogVersionId[K] = ObjectVersions.log_version_id(version)
                    entry: Entry[K, V] = entry_from_log(data, version_id_log, self._value_decoder)
                    entries.append(entry)

                    # Check limit
                    if limit is not None and len(entries) >= limit:
                        return entries

                # Check if more pages
                if not page.get("IsTruncated", False):
                    break

                key_marker = page.get("NextKeyMarker")
                version_id_marker = page.get("NextVersionIdMarker")

        except ClientError as e:  # type: ignore[misc]
            if get_error_code(e) in ["NoSuchKey", "404"]:
                return []
            raise

        return entries

    async def list_keys(
        self,
        after_key: Optional[K] = None,
        limit: Optional[int] = None,
    ) -> List[K]:
        """List all keys in lexicographic order.

        Args:
            after_key: Pagination cursor (exclusive). Pass the last key
                from previous call to get next page.
            limit: Maximum keys to return. None for unlimited.

        Returns:
            List of key names in lexicographic order
        """
        keys: List[K] = []
        prefix = f"{self._config.s3_prefix}keys/"

        try:
            continuation_token: Optional[str] = None
            start_after = f"{prefix}{after_key}.json" if after_key is not None else prefix

            while True:
                response = await self._s3.list_objects_v2(
                    bucket=self._config.s3_bucket,
                    prefix=prefix,
                    start_after=start_after if continuation_token is None else None,
                    continuation_token=continuation_token,
                )

                contents = response.get("Contents")
                if contents is not None:
                    for obj in contents:
                        # Extract key name from path
                        key_name_str = cast(str, obj["Key"])[len(prefix) :]
                        if key_name_str.endswith(".json"):
                            key_name_str = key_name_str[:-5]
                            keys.append(cast(K, key_name_str))

                            # Check limit
                            if limit is not None and len(keys) >= limit:
                                return keys

                # Check if more pages
                if not response.get("IsTruncated", False):
                    break

                continuation_token = response.get("NextContinuationToken")

        except ClientError:  # type: ignore[misc]
            return []

        return keys

    # =========================================================================
    # Verification
    # =========================================================================

    def verify(self, entry: Entry[K, V]) -> bool:
        """Verify single entry integrity (sync - pure computation).

        Recomputes the SHA-256 hash of the entry and compares with the
        stored hash. This method is synchronous since it performs no I/O.

        Args:
            entry: The entry to verify

        Returns:
            True if computed hash matches stored hash
        """
        # Encode value back to JSON for hash verification
        entry_for_hash: LogEntryForHash[K, JSONValue] = {
            "sequence": entry.sequence,
            "key": entry.key,
            "value": self._value_encoder(entry.value),
            "timestamp_ms": entry.timestamp_ms,
            "previous_hash": entry.previous_hash,
        }
        expected_hash = self._calculate_hash(entry_for_hash)
        return entry.hash == expected_hash

    async def verify_log_chain(self, limit: Optional[int] = None) -> bool:
        """Verify the hash chain integrity of the log.

        Fetches log entries and verifies:
        1. Each entry's individual hash is correct
        2. Each entry's previous_hash matches the preceding entry's hash
        3. The chain ends with the genesis hash

        Args:
            limit: Only verify the last N entries. None for full verification.

        Returns:
            True if chain is valid, False if any integrity check fails
        """
        entries = await self.log_entries(None, limit)

        if not entries:
            return True

        # Verify each entry's hash
        for entry in entries:
            if not self.verify(entry):
                logger.error(f"Hash verification failed for entry {entry.sequence}")
                return False

        # Verify chain linkage (newest to oldest)
        for i in range(len(entries) - 1):
            current = entries[i]
            previous = entries[i + 1]

            if current.previous_hash != previous.hash:
                logger.error(
                    f"Chain broken between entry {current.sequence} and {previous.sequence}"
                )
                return False

        return True

    # =========================================================================
    # Client Configuration
    # =========================================================================

    @property
    def config(self) -> Config:
        """The client configuration (read-only)."""
        return self._config

    def with_codec(
        self,
        value_decoder: ValueDecoder[V2],
        value_encoder: ValueEncoder[V2],
    ) -> "ImmuKVClient[K2, V2]":
        """Create a new client with different codecs, sharing the S3 connection.

        Useful for working with multiple value types in the same bucket.
        The returned client shares the underlying aiobotocore session.

        Args:
            value_decoder: New decoder for value type V2
            value_encoder: New encoder for value type V2

        Returns:
            New client instance with specified codecs

        Note:
            The returned client does NOT own the session. Only the original
            client should be closed. Closing either affects both.
        """
        new_client: ImmuKVClient[K2, V2] = object.__new__(ImmuKVClient)  # type: ignore[type-var,misc]
        # Share immutable fields
        new_client._config = self._config
        new_client._s3 = self._s3
        new_client._log_key = self._log_key
        # Set new codec
        new_client._value_decoder = value_decoder  # type: ignore[assignment]
        new_client._value_encoder = value_encoder  # type: ignore[assignment]
        # Initialize mutable state
        new_client._last_repair_check_ms = 0
        new_client._can_write = None
        new_client._latest_orphan_status = None
        new_client._owns_session = False  # Shared session
        return new_client  # type: ignore[return-value]

    def get_s3_client(self) -> "AsyncBrandedS3Client[str]":
        """Get the underlying aiobotocore S3 client for extensions.

        Returns:
            The AsyncBrandedS3Client used by this ImmuKVClient

        Note:
            This is intended for advanced use cases like the immukv-files
            extension. The returned client shares lifecycle with this
            ImmuKVClient - do not close it independently.
        """
        return self._s3

    # =========================================================================
    # Resource Management
    # =========================================================================

    async def close(self) -> None:
        """Close client and cleanup resources.

        If this client owns the aiobotocore session (created via create()),
        the session will be closed. Shared clients (from with_codec) do not
        close the session.
        """
        if self._owns_session:
            await self._s3.close()

    async def __aenter__(self) -> "ImmuKVClient[K, V]":
        """Async context manager entry."""
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: object,
    ) -> None:
        """Async context manager exit."""
        await self.close()

    # =========================================================================
    # Private Helper Methods
    # =========================================================================

    def _calculate_hash(self, entry_for_hash: LogEntryForHash[K, JSONValue]) -> Hash[K]:
        """Calculate SHA-256 hash for a log entry.

        Hash Input Fields (in exact order):
        1. sequence - The entry number (integer)
        2. key - The key being written (string)
        3. value - The value being written (canonical JSON)
        4. timestamp_ms - The timestamp in epoch milliseconds (integer)
        5. previous_hash - The hash from the previous entry (string)

        Canonical String Format: <sequence>|<key>|<value_json>|<timestamp_ms>|<previous_hash>
        """
        return hash_compute(entry_for_hash)

    async def _get_latest_and_repair(self) -> LatestLogState[K, V]:
        """Get latest log state and repair orphaned entry if needed.

        Returns dict with:
            log_etag: ETag of current log (for optimistic locking)
            prev_version_id: Previous log version ID
            prev_hash: Previous entry hash
            sequence: Current sequence number
            can_write: Whether client has write permission (or None if unknown)
            orphan_status: Current orphan status (or None if unchanged)
        """
        # Try to read current log
        try:
            response = await self._s3.get_object(bucket=self._config.s3_bucket, key=self._log_key)
            log_etag = cast(str, response["ETag"])
            current_version_id: LogVersionId[K] = LogVersionId(response["VersionId"])
            data = _read_body_as_json(response["Body"])

            prev_version_id: LogVersionId[K] = current_version_id
            prev_hash: Hash[K] = hash_from_json(get_str(data, "hash"))
            sequence: Sequence[K] = sequence_from_json(get_int(data, "sequence"))

            # Create entry from latest log data
            latest_entry: Entry[K, V] = entry_from_log(
                data, current_version_id, self._value_decoder
            )

            # Try to repair orphan
            can_write, orphan_status = await self._repair_orphan(latest_entry)

            return {
                "log_etag": log_etag,
                "prev_version_id": prev_version_id,
                "prev_hash": prev_hash,
                "sequence": sequence,
                "can_write": can_write,
                "orphan_status": orphan_status,
            }

        except ClientError as e:  # type: ignore[misc]
            if get_error_code(e) in ["NoSuchKey", "404"]:
                # First entry - use genesis hash
                return {
                    "log_etag": None,
                    "prev_version_id": None,
                    "prev_hash": hash_genesis(),
                    "sequence": sequence_initial(),
                    "can_write": None,
                    "orphan_status": None,
                }
            raise

    async def _repair_orphan(
        self, latest_log: Entry[K, V]
    ) -> Tuple[Optional[bool], Optional[OrphanStatus[K, V]]]:
        """Repair orphaned log entry by propagating to key object.

        Uses stored previous ETag from log entry for idempotent conditional write.

        Args:
            latest_log: The latest log entry to repair

        Returns:
            Tuple of (can_write, orphan_status)
        """
        # Skip if in read-only mode or we know we can't write
        if self._config.read_only or self._can_write is False:
            # Check if this key object exists
            key_path = S3KeyPaths.for_key(self._config.s3_prefix, latest_log.key)
            try:
                await self._s3.head_object(bucket=self._config.s3_bucket, key=key_path)
                # Key object exists - not orphaned
                orphan_status: OrphanStatus[K, V] = {
                    "is_orphaned": False,
                    "orphan_key": None,
                    "orphan_entry": None,
                    "checked_at": int(time.time() * 1000),
                }
                return (self._can_write, orphan_status)
            except ClientError as e:  # type: ignore[misc]
                if get_error_code(e) in ["NoSuchKey", "404"]:
                    # Key object missing - orphaned
                    orphan_status = {
                        "is_orphaned": True,
                        "orphan_key": latest_log.key,
                        "orphan_entry": latest_log,
                        "checked_at": int(time.time() * 1000),
                    }
                    return (False, orphan_status)
                raise

        current_time_ms = int(time.time() * 1000)
        key_path = S3KeyPaths.for_key(self._config.s3_prefix, latest_log.key)

        # Prepare repair data - encode value back to JSON
        repair_data: KeyObjectDict = {
            "sequence": latest_log.sequence,
            "key": latest_log.key,
            "value": self._value_encoder(latest_log.value),
            "timestamp_ms": latest_log.timestamp_ms,
            "log_version_id": latest_log.version_id,
            "hash": latest_log.hash,
            "previous_hash": latest_log.previous_hash,
        }

        try:
            if latest_log.previous_key_object_etag is not None:
                # UPDATE with if_match=<previous_etag>
                await self._s3.put_object(
                    bucket=self._config.s3_bucket,
                    key=key_path,
                    body=dumps_canonical(cast(JSONValue, repair_data)),
                    content_type="application/json",
                    if_match=latest_log.previous_key_object_etag,
                )
                logger.info(f"Propagated log entry to key object for {latest_log.key}")
            else:
                # CREATE with if_none_match='*'
                await self._s3.put_object(
                    bucket=self._config.s3_bucket,
                    key=key_path,
                    body=dumps_canonical(cast(JSONValue, repair_data)),
                    content_type="application/json",
                    if_none_match="*",
                )
                logger.info(f"Created key object for {latest_log.key}")

            # Success
            orphan_status = {
                "is_orphaned": False,
                "orphan_key": None,
                "orphan_entry": None,
                "checked_at": current_time_ms,
            }
            return (True, orphan_status)

        except ClientError as e:  # type: ignore[misc]
            error_code = get_error_code(e)

            if error_code == "PreconditionFailed":
                # Already propagated by another client
                orphan_status = {
                    "is_orphaned": False,
                    "orphan_key": None,
                    "orphan_entry": None,
                    "checked_at": current_time_ms,
                }
                return (True, orphan_status)

            elif error_code in ["AccessDenied", "Forbidden"]:
                # No write permission - cache this
                orphan_status = {
                    "is_orphaned": True,
                    "orphan_key": latest_log.key,
                    "orphan_entry": latest_log,
                    "checked_at": current_time_ms,
                }
                logger.info("Read-only mode detected - orphan repair disabled")
                return (False, orphan_status)

            else:
                # Other error - log but don't fail
                logger.warning(f"Pre-flight repair failed: {e}")
                return (None, None)


def _read_body_as_json(body: object) -> Dict[str, JSONValue]:
    """Read body content (bytes for async, streaming for sync) and parse as JSON.

    For async client, the body is pre-read bytes from aiobotocore.
    """
    if isinstance(body, bytes):
        json_str = body.decode("utf-8")
    else:
        # Fallback for streaming body (shouldn't happen with async client)
        body_data = body.read()  # type: ignore[attr-defined,misc]
        json_str = body_data.decode("utf-8") if isinstance(body_data, bytes) else body_data  # type: ignore[misc]
    return cast(Dict[str, JSONValue], json.loads(json_str))

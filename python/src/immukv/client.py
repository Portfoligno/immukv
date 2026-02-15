"""ImmuKV client implementation."""

import asyncio
import json
import logging
import threading
import time
from collections.abc import Coroutine
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Any, Dict, Generic, List, Optional, Tuple, TypeVar, cast

from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client

from immukv._internal.json_helpers import (
    dumps_canonical,
    entry_from_key_object,
    entry_from_log,
    get_int,
    get_str,
    raw_entry_from_log,
    strip_none_values,
)
from immukv._internal.types import (
    JSONValue,
    KeyObjectDict,
    LatestLogState,
    LogEntryDict,
    LogEntryForHash,
    OrphanStatus,
    RawEntry,
    hash_compute,
    hash_from_json,
    hash_genesis,
    sequence_from_json,
    sequence_initial,
    sequence_next,
    timestamp_now,
)
from immukv.json_helpers import ValueDecoder, ValueEncoder
from immukv._internal.s3_client import BrandedS3Client
from immukv._internal.s3_helpers import get_error_code, read_body_as_json
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
    CredentialProvider,
    Entry,
    Hash,
    KeyNotFoundError,
    KeyObjectETag,
    KeyVersionId,
    LogVersionId,
    ReadOnlyError,
    S3Credentials,
    Sequence,
    TimestampMs,
)

logger = logging.getLogger(__name__)

# Type variables for generic key and value types
K = TypeVar("K", bound=str)
K2 = TypeVar("K2", bound=str)
V = TypeVar("V")
V2 = TypeVar("V2")
_T = TypeVar("_T")


class ImmuKVClient(Generic[K, V]):
    """Main client interface - Simple S3 versioning with auto-repair.

    Type parameters:
        K: Key type (must be subtype of str)
        V: Value type
    """

    # Instance field type annotations
    _config: Config
    _loop: asyncio.AbstractEventLoop
    _thread: threading.Thread
    _exit_stack: AsyncExitStack
    _s3: BrandedS3Client
    _owns_loop: bool
    _log_key: S3KeyPath[LogKey]
    _value_decoder: ValueDecoder[V]
    _value_encoder: ValueEncoder[V]
    _last_repair_check_ms: int
    _can_write: Optional[bool]
    _latest_orphan_status: Optional[OrphanStatus[K]]

    def __init__(
        self, config: Config, value_decoder: ValueDecoder[V], value_encoder: ValueEncoder[V]
    ) -> None:
        """Initialize client with configuration.

        Args:
            config: S3 bucket and prefix configuration
            value_decoder: Decoder to transform JSONValue to user's V type
            value_encoder: Encoder to transform user's V type to JSONValue
        """
        self._config = config
        self._value_decoder = value_decoder
        self._value_encoder = value_encoder

        # Build aiobotocore client parameters
        client_params: dict[str, object] = {
            "region_name": config.s3_region,
        }

        credential_provider: Optional[CredentialProvider] = None

        if config.overrides is not None:
            if config.overrides.endpoint_url is not None:
                client_params["endpoint_url"] = config.overrides.endpoint_url
            if config.overrides.credentials is not None:
                if callable(config.overrides.credentials):
                    credential_provider = config.overrides.credentials
                else:
                    creds: S3Credentials = config.overrides.credentials
                    client_params["aws_access_key_id"] = creds.aws_access_key_id
                    client_params["aws_secret_access_key"] = creds.aws_secret_access_key
                    if creds.aws_session_token is not None:
                        client_params["aws_session_token"] = creds.aws_session_token
            if config.overrides.force_path_style:
                from botocore.config import Config as BotocoreConfig

                client_params["config"] = BotocoreConfig(s3={"addressing_style": "path"})

        # Start background IO thread
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, name="immukv-io", daemon=True
        )
        self._thread.start()
        self._owns_loop = True

        # Create aiobotocore client on the background loop
        aio_client, self._exit_stack = self._run_on_loop(
            self._create_aio_client(client_params, credential_provider)
        )
        self._s3 = BrandedS3Client(aio_client, self._loop)
        self._log_key = cast(S3KeyPath[LogKey], S3KeyPaths.for_log(config.s3_prefix))
        self._last_repair_check_ms = 0  # In-memory timestamp tracking
        self._can_write: Optional[bool] = None  # Permission cache
        self._latest_orphan_status: Optional[OrphanStatus[K]] = None  # Orphan detection cache

    def _run_on_loop(self, coro: Coroutine[object, object, _T]) -> _T:
        """Submit coroutine to background loop, block for result.

        Used only for lifecycle operations (create client, close).
        All S3 operations go through BrandedS3Client._run.
        """
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()

    @staticmethod
    async def _create_aio_client(
        client_params: dict[str, object],
        credential_provider: Optional[CredentialProvider] = None,
    ) -> tuple["S3Client", AsyncExitStack]:
        import aiobotocore.session

        stack = AsyncExitStack()
        session = aiobotocore.session.get_session()

        if credential_provider is not None:
            from aiobotocore.credentials import AioDeferredRefreshableCredentials

            async def _refresh() -> dict[str, str]:
                from datetime import datetime, timedelta, timezone

                creds = await credential_provider()
                expiry = creds.expires_at or (datetime.now(timezone.utc) + timedelta(hours=1))
                return {
                    "access_key": creds.aws_access_key_id,
                    "secret_key": creds.aws_secret_access_key,
                    "token": creds.aws_session_token or "",
                    "expiry_time": expiry.isoformat(),
                }

            refreshable = AioDeferredRefreshableCredentials(
                refresh_using=_refresh, method="immukv-credential-provider"
            )
            session._credentials = refreshable  # type: ignore[attr-defined]

        client: "S3Client" = await stack.enter_async_context(
            session.create_client("s3", **client_params)  # type: ignore[call-overload,misc]
        )
        return client, stack

    def set(self, key: K, value: V) -> Entry[K, V]:
        """Write new entry (two-phase: pre-flight repair, log, key object).

        Pre-flight: Repair previous orphan (if any)
        Write Phase 1: Append to _log.json (creates new version) - ALWAYS succeeds or raises
        Write Phase 2: Update keys/{key}.json (creates new version) - MAY fail (orphaned)

        Returns: Entry object representing the committed log entry

        Note: Returns successfully even if phase 2 fails. Entry always exists in log.
              If phase 2 fails, orphan will be auto-repaired on next write.
        """
        # Check read-only mode at entry
        if self._config.read_only:
            raise ReadOnlyError("Cannot call set() in read-only mode")

        # Retry loop for optimistic locking on log writes
        max_retries = 10
        last_error: Optional[ClientError] = None

        for attempt in range(max_retries):
            # ===== Pre-Flight: Repair (with ETag) =====
            result = self._get_latest_and_repair()
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
                current_key = self._s3.head_object(bucket=self._config.s3_bucket, key=key_path)
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
                    response = self._s3.put_object(
                        bucket=self._config.s3_bucket,
                        key=self._log_key,
                        body=dumps_canonical(cast(JSONValue, log_entry_for_json)),
                        content_type="application/json",
                        if_match=log_etag,
                    )
                else:
                    # First write - use if_none_match='*'
                    response = self._s3.put_object(
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
                break  # ✅ Committed to log! Exit retry loop

            except ClientError as e:  # type: ignore[misc]
                if get_error_code(e) == "PreconditionFailed":
                    last_error = e
                    logger.debug(f"Log write conflict, retry {attempt + 1}/{max_retries}")
                    continue
                else:
                    raise

        else:
            diagnostic_info: Dict[str, object] = {}
            if last_error is not None:
                error_response = last_error.response  # type: ignore[misc]
                response_metadata = error_response.get("ResponseMetadata", {})  # type: ignore[misc]
                diagnostic_info = {
                    "httpStatus": response_metadata.get("HTTPStatusCode"),  # type: ignore[misc]
                    "errorCode": get_error_code(last_error),
                    "errorMessage": str(last_error),
                    "requestId": response_metadata.get("RequestId"),  # type: ignore[misc]
                }

            raise Exception(
                f"Failed to write log after {max_retries} retries: {json.dumps(diagnostic_info)}"
            ) from last_error

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
                response = self._s3.put_object(
                    bucket=self._config.s3_bucket,
                    key=key_path,
                    body=dumps_canonical(cast(JSONValue, key_data)),
                    content_type="application/json",
                    if_match=current_key_etag,
                )
                key_object_etag = PutObjectOutputs.key_object_etag(response)
            else:
                # CREATE new key object - use if_none_match='*'
                response = self._s3.put_object(
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

    def get(self, key: K) -> Entry[K, V]:
        """Get latest value for key (with conditional orphan check and fallback).

        Fast path: Single S3 read from key object (when repair check not needed)
        Slow path: Checks for orphans if repair_check_interval_ms has elapsed

        Raises KeyNotFoundError if key object doesn't exist and no orphan fallback available.
        """
        # Conditional orphan check based on time interval
        current_time_ms = int(time.time() * 1000)
        time_since_last_check = current_time_ms - self._last_repair_check_ms

        # Check if we need to perform orphan repair check
        if time_since_last_check >= self._config.repair_check_interval_ms:
            # Skip repair attempt if we know we're read-only
            if not (self._can_write is False or self._config.read_only):
                # Perform orphan check and repair
                result = self._get_latest_and_repair()
                if result["can_write"] is not None:
                    self._can_write = result["can_write"]
                if result["orphan_status"] is not None:
                    self._latest_orphan_status = result["orphan_status"]
                self._last_repair_check_ms = current_time_ms

        # Try to read from key object
        key_path = S3KeyPaths.for_key(self._config.s3_prefix, key)
        try:
            response = self._s3.get_object(bucket=self._config.s3_bucket, key=key_path)
            data = read_body_as_json(response["Body"])
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
                    # Return cached orphan entry (read-only mode) — decode on demand
                    raw = self._latest_orphan_status["orphan_entry"]
                    assert raw is not None
                    return Entry(
                        key=raw.key,
                        value=self._value_decoder(raw.value),
                        timestamp_ms=raw.timestamp_ms,
                        version_id=raw.version_id,
                        sequence=raw.sequence,
                        previous_version_id=raw.previous_version_id,
                        hash=raw.hash,
                        previous_hash=raw.previous_hash,
                        previous_key_object_etag=raw.previous_key_object_etag,
                    )

                raise KeyNotFoundError(f"Key '{key}' not found")
            else:
                raise

    def get_log_version(self, version_id: LogVersionId[K]) -> Entry[K, V]:
        """Get specific log version by S3 version ID."""
        try:
            response = self._s3.get_object(
                bucket=self._config.s3_bucket, key=self._log_key, version_id=version_id
            )
            data = read_body_as_json(response["Body"])
            return entry_from_log(data, version_id, self._value_decoder)
        except ClientError as e:  # type: ignore[misc]
            if get_error_code(e) in ["NoSuchKey", "NoSuchVersion", "404"]:
                raise KeyNotFoundError(f"Log version '{version_id}' not found")
            raise

    def history(
        self, key: K, before_version_id: Optional[KeyVersionId[K]], limit: Optional[int]
    ) -> Tuple[List[Entry[K, V]], Optional[KeyVersionId[K]]]:
        """Get all entries for a key (descending order - newest first).

        Orphan-aware: reads from key object versions and prepends orphan entry if present.

        Args:
            key: The key to retrieve history for
            before_version_id: Return entries before this key version ID (exclusive).
                             Pass None to start from newest (includes orphan if present).
            limit: Maximum number of entries to return. Pass None for unlimited (all entries).

        Returns:
            Tuple of (entries, oldest_key_version_id)
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
            raw = self._latest_orphan_status["orphan_entry"]
            assert raw is not None
            entries.append(
                Entry(
                    key=raw.key,
                    value=self._value_decoder(raw.value),
                    timestamp_ms=raw.timestamp_ms,
                    version_id=raw.version_id,
                    sequence=raw.sequence,
                    previous_version_id=raw.previous_version_id,
                    hash=raw.hash,
                    previous_hash=raw.previous_hash,
                    previous_key_object_etag=raw.previous_key_object_etag,
                )
            )

        # List versions of key object
        try:
            key_marker: Optional[str] = key_path if before_version_id is not None else None
            version_id_marker: Optional[str] = before_version_id
            last_key_version_id: Optional[KeyVersionId[K]] = None

            while True:
                list_params: Dict[str, Any] = {  # type: ignore[explicit-any]
                    "bucket": self._config.s3_bucket,
                    "prefix": key_path,
                }
                if key_marker is not None:
                    list_params["KeyMarker"] = key_marker  # type: ignore[misc]
                if version_id_marker is not None:
                    list_params["VersionIdMarker"] = version_id_marker  # type: ignore[misc]

                page = self._s3.list_object_versions(**list_params)  # type: ignore[misc]
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
                    response = self._s3.get_object(
                        bucket=self._config.s3_bucket,
                        key=key_path,
                        version_id=key_version_id,
                    )
                    data = read_body_as_json(response["Body"])
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

    def log_entries(
        self, before_version_id: Optional[LogVersionId[K]], limit: Optional[int]
    ) -> List[Entry[K, V]]:
        """Get entries from global log (descending order - newest first).

        Args:
            before_version_id: Return entries before this log version ID (exclusive).
            limit: Maximum number of entries to return. Pass None for unlimited.

        Returns:
            List of entries in descending order (newest first)
        """
        entries: List[Entry[K, V]] = []

        try:
            key_marker: Optional[str] = self._log_key if before_version_id is not None else None
            version_id_marker: Optional[str] = before_version_id

            while True:
                list_params: Dict[str, Any] = {  # type: ignore[explicit-any]
                    "bucket": self._config.s3_bucket,
                    "prefix": self._log_key,
                }
                if key_marker is not None:
                    list_params["KeyMarker"] = key_marker  # type: ignore[misc]
                if version_id_marker is not None:
                    list_params["VersionIdMarker"] = version_id_marker  # type: ignore[misc]

                page = self._s3.list_object_versions(**list_params)  # type: ignore[misc]
                versions_result = page.get("Versions")
                versions = versions_result if versions_result is not None else []
                for version in versions:
                    if version["Key"] != self._log_key:
                        continue

                    # Skip the before_version_id itself
                    if before_version_id is not None and version["VersionId"] == before_version_id:
                        continue

                    # Fetch version data
                    response = self._s3.get_object(
                        bucket=self._config.s3_bucket,
                        key=self._log_key,
                        version_id=version["VersionId"],
                    )
                    data = read_body_as_json(response["Body"])
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

    def list_keys(
        self,
        after_key: Optional[K],
        limit: Optional[int],
    ) -> List[K]:
        """List all keys in the system (lexicographic order).

        Args:
            after_key: Return keys after this key (exclusive, lexicographic order).
            limit: Maximum number of keys to return. Pass None for unlimited.

        Returns:
            List of key names in lexicographic order
        """
        return self._list_keys_internal(after_key, limit)

    def list_keys_with_prefix(
        self,
        prefix: str,
        after_key: Optional[K],
        limit: Optional[int],
    ) -> List[K]:
        """List keys matching the given prefix (lexicographic order).

        Args:
            prefix: Only return keys starting with this prefix. Filtering is
                done server-side.
            after_key: Return keys after this key (exclusive, lexicographic order).
            limit: Maximum number of keys to return. Pass None for unlimited.

        Returns:
            List of key names in lexicographic order
        """
        return self._list_keys_internal(after_key, limit, prefix=prefix)

    def _list_keys_internal(
        self,
        after_key: Optional[K],
        limit: Optional[int],
        prefix: Optional[str] = None,
    ) -> List[K]:
        """Internal implementation for listing keys.

        Args:
            after_key: Return keys after this key (exclusive, lexicographic order).
            limit: Maximum number of keys to return. Pass None for unlimited.
            prefix: Only return keys starting with this prefix.

        Returns:
            List of key names in lexicographic order
        """
        keys: List[K] = []
        base_prefix = f"{self._config.s3_prefix}keys/"
        s3_prefix = f"{base_prefix}{prefix}" if prefix is not None else base_prefix
        start_after = f"{base_prefix}{after_key}.json" if after_key is not None else s3_prefix

        try:
            continuation_token: Optional[str] = None
            while True:
                page = self._s3.list_objects_v2(
                    bucket=self._config.s3_bucket,
                    prefix=s3_prefix,
                    start_after=start_after if continuation_token is None else None,
                    continuation_token=continuation_token,
                )
                contents = page.get("Contents") or []
                for obj in contents:
                    key_name_str = obj["Key"][len(base_prefix) :]
                    if key_name_str.endswith(".json"):
                        keys.append(cast(K, key_name_str[:-5]))
                        if limit is not None and len(keys) >= limit:
                            return keys
                if not page["IsTruncated"]:
                    break
                continuation_token = page.get("NextContinuationToken")

        except ClientError:  # type: ignore[misc]
            return []

        return keys

    def verify(self, entry: Entry[K, V]) -> bool:
        """Verify single entry integrity."""
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

    def verify_log_chain(self, limit: Optional[int] = None) -> bool:
        """Verify hash chain in log.

        Args:
            limit: Only verify last N entries (None = all)

        Returns:
            True if chain is valid, False otherwise
        """
        entries = self._raw_log_entries(limit)

        if not entries:
            return True

        # Verify each entry's hash
        for entry in entries:
            if not self._verify_raw(entry):
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

    def _verify_raw(self, entry: RawEntry[K]) -> bool:
        """Verify single raw entry integrity (no decode/encode round-trip).

        Uses entry.value directly — the raw JSONValue from S3, which is the
        exact encoded value that was hashed at write time.
        """
        entry_for_hash: LogEntryForHash[K, JSONValue] = {
            "sequence": entry.sequence,
            "key": entry.key,
            "value": entry.value,
            "timestamp_ms": entry.timestamp_ms,
            "previous_hash": entry.previous_hash,
        }
        expected_hash = self._calculate_hash(entry_for_hash)
        return entry.hash == expected_hash

    def _raw_log_entries(self, limit: Optional[int] = None) -> List[RawEntry[K]]:
        """Get raw entries from global log for internal use (no value decoding).

        Mirrors log_entries() S3 iteration but uses raw_entry_from_log()
        instead of entry_from_log(), bypassing the value decoder entirely.

        Args:
            limit: Maximum number of entries to return. Pass None for unlimited.

        Returns:
            List of raw entries in descending order (newest first)
        """
        entries: List[RawEntry[K]] = []

        try:
            key_marker: Optional[str] = None
            version_id_marker: Optional[str] = None

            while True:
                list_params: Dict[str, Any] = {  # type: ignore[explicit-any]
                    "bucket": self._config.s3_bucket,
                    "prefix": self._log_key,
                }
                if key_marker is not None:
                    list_params["KeyMarker"] = key_marker  # type: ignore[misc]
                if version_id_marker is not None:
                    list_params["VersionIdMarker"] = version_id_marker  # type: ignore[misc]

                page = self._s3.list_object_versions(**list_params)  # type: ignore[misc]
                versions_result = page.get("Versions")
                versions = versions_result if versions_result is not None else []
                for version in versions:
                    if version["Key"] != self._log_key:
                        continue

                    # Fetch version data
                    response = self._s3.get_object(
                        bucket=self._config.s3_bucket,
                        key=self._log_key,
                        version_id=version["VersionId"],
                    )
                    data = read_body_as_json(response["Body"])
                    version_id_log: LogVersionId[K] = ObjectVersions.log_version_id(version)
                    entry: RawEntry[K] = raw_entry_from_log(data, version_id_log)
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

    def with_codec(
        self, value_decoder: ValueDecoder[V2], value_encoder: ValueEncoder[V2]
    ) -> "ImmuKVClient[K2, V2]":
        """Create a new client with different decoder/encoder, sharing the S3 connection.

        This allows working with different key/value types while reusing the connection pool.

        Note: The returned client shares the underlying S3 client. Closing either client
        (via close() or context manager) will close the shared connection, affecting both.
        """
        new_client: ImmuKVClient[K2, V2] = object.__new__(ImmuKVClient)
        # Share immutable fields
        new_client._config = self._config
        new_client._s3 = self._s3  # shared (holds loop ref internally)
        new_client._loop = self._loop  # shared for lifecycle
        new_client._thread = self._thread  # shared
        new_client._exit_stack = self._exit_stack  # shared
        new_client._owns_loop = False  # does NOT own cleanup
        new_client._log_key = self._log_key
        # Set new codec
        new_client._value_decoder = value_decoder
        new_client._value_encoder = value_encoder
        # Initialize mutable state
        new_client._last_repair_check_ms = 0
        new_client._can_write = None
        new_client._latest_orphan_status = None
        return new_client

    def close(self) -> None:
        """Close client and cleanup resources."""
        if not self._owns_loop:
            return  # with_codec() fork -- don't clean up shared resources

        self._run_on_loop(self._exit_stack.aclose())
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)
        self._loop.close()

    def __enter__(self) -> "ImmuKVClient[K, V]":
        """Context manager entry."""
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: object,
    ) -> None:
        """Context manager exit."""
        self.close()

    # ===== Private Helper Methods =====

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

    def _get_latest_and_repair(self) -> LatestLogState[K]:
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
            response = self._s3.get_object(bucket=self._config.s3_bucket, key=self._log_key)
            log_etag = cast(str, response["ETag"])
            current_version_id: LogVersionId[K] = LogVersionId(response["VersionId"])
            data = read_body_as_json(response["Body"])

            prev_version_id: LogVersionId[K] = current_version_id
            prev_hash: Hash[K] = hash_from_json(get_str(data, "hash"))
            sequence: Sequence[K] = sequence_from_json(get_int(data, "sequence"))

            # Create raw entry from latest log data (no value decoding)
            latest_entry: RawEntry[K] = raw_entry_from_log(data, current_version_id)

            # Try to repair orphan
            can_write, orphan_status = self._repair_orphan(latest_entry)

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

    def _repair_orphan(
        self, latest_log: RawEntry[K]
    ) -> Tuple[Optional[bool], Optional[OrphanStatus[K]]]:
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
                self._s3.head_object(bucket=self._config.s3_bucket, key=key_path)
                # Key object exists - not orphaned
                orphan_status: OrphanStatus[K] = {
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

        # Prepare repair data - use raw JSON value directly (no encode/decode round-trip)
        repair_data: KeyObjectDict = {
            "sequence": latest_log.sequence,
            "key": latest_log.key,
            "value": latest_log.value,
            "timestamp_ms": latest_log.timestamp_ms,
            "log_version_id": latest_log.version_id,
            "hash": latest_log.hash,
            "previous_hash": latest_log.previous_hash,
        }

        try:
            if latest_log.previous_key_object_etag is not None:
                # UPDATE with if_match=<previous_etag>
                self._s3.put_object(
                    bucket=self._config.s3_bucket,
                    key=key_path,
                    body=dumps_canonical(cast(JSONValue, repair_data)),
                    content_type="application/json",
                    if_match=latest_log.previous_key_object_etag,
                )
                logger.info(f"Propagated log entry to key object for {latest_log.key}")
            else:
                # CREATE with if_none_match='*'
                self._s3.put_object(
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

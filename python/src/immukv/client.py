"""ImmuKV client implementation."""

import hashlib
import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from immukv.types import (
    Config,
    Entry,
    KeyNotFoundError,
    LogEntryForHash,
    OrphanStatus,
    ReadOnlyError,
)

logger = logging.getLogger(__name__)


class ImmuKVClient:
    """Main client interface - Simple S3 versioning with auto-repair."""

    def __init__(self, config: Config) -> None:
        """Initialize client with configuration."""
        self.config = config
        self.s3 = boto3.client("s3", region_name=config.s3_region)
        self.log_key = f"{config.s3_prefix}_log.json"
        self._last_repair_check_ms = 0  # In-memory timestamp tracking
        self._can_write: Optional[bool] = None  # Permission cache
        self._latest_orphan_status: Optional[OrphanStatus] = None  # Orphan detection cache

    def set(self, key: str, value: Any) -> Entry[str, Any]:
        """Write new entry (two-phase: pre-flight repair, log, key object).

        Pre-flight: Repair previous orphan (if any)
        Write Phase 1: Append to _log.json (creates new version) - ALWAYS succeeds or raises
        Write Phase 2: Update keys/{key}.json (creates new version) - MAY fail (orphaned)

        Returns: Entry object representing the committed log entry

        Note: Returns successfully even if phase 2 fails. Entry always exists in log.
              If phase 2 fails, orphan will be auto-repaired on next write.
        """
        # Check read-only mode at entry
        if self.config.read_only:
            raise ReadOnlyError("Cannot call set() in read-only mode")

        # Retry loop for optimistic locking on log writes
        max_retries = 10
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
            key_path = f"{self.config.s3_prefix}keys/{key}.json"
            current_key_etag = None
            try:
                current_key = self.s3.head_object(Bucket=self.config.s3_bucket, Key=key_path)
                current_key_etag = current_key["ETag"]
            except ClientError as e:
                if e.response["Error"]["Code"] in ["NoSuchKey", "404"]:
                    current_key_etag = None
                else:
                    raise

            # Step 2: Create new log entry
            new_sequence = (sequence or 0) + 1
            timestamp_ms = int(time.time() * 1000)

            # Step 3: Calculate hash
            entry_for_hash: LogEntryForHash[str, Any] = {
                "sequence": new_sequence,
                "key": key,
                "value": value,
                "timestamp_ms": timestamp_ms,
                "previous_hash": prev_hash,
            }
            entry_hash = self._calculate_hash(entry_for_hash)

            # Step 4: Create complete log entry (with current key object ETag)
            log_entry = {
                "sequence": new_sequence,
                "key": key,
                "value": value,
                "timestamp_ms": timestamp_ms,
                "previous_version_id": prev_version_id,
                "previous_hash": prev_hash,
                "hash": entry_hash,
                "previous_key_object_etag": current_key_etag,
            }

            # Step 5: Write to log with optimistic locking
            try:
                if log_etag:
                    # Update existing log - use IfMatch
                    response = self.s3.put_object(
                        Bucket=self.config.s3_bucket,
                        Key=self.log_key,
                        Body=json.dumps(log_entry, separators=(",", ":")),
                        ContentType="application/json",
                        IfMatch=log_etag,
                    )
                else:
                    # First write - use IfNoneMatch='*'
                    response = self.s3.put_object(
                        Bucket=self.config.s3_bucket,
                        Key=self.log_key,
                        Body=json.dumps(log_entry, separators=(",", ":")),
                        ContentType="application/json",
                        IfNoneMatch="*",
                    )

                new_log_version_id = response["VersionId"]
                break  # ✅ Committed to log! Exit retry loop

            except ClientError as e:
                if e.response["Error"]["Code"] == "PreconditionFailed":
                    logger.debug(f"Log write conflict, retry {attempt + 1}/{max_retries}")
                    continue
                else:
                    raise

        else:
            raise Exception(f"Failed to write log after {max_retries} retries")

        # ===== Write Phase 2: Write Key Object (with conditional write) =====

        key_object_etag = None
        try:
            # Create key object data - INCLUDES ALL FIELDS FROM LOG ENTRY
            key_data = {
                "sequence": new_sequence,
                "key": key,
                "value": value,
                "timestamp_ms": timestamp_ms,
                "log_version_id": new_log_version_id,
                "hash": entry_hash,
                "previous_hash": prev_hash,
            }

            if current_key_etag:
                # UPDATE existing key object - use IfMatch
                response = self.s3.put_object(
                    Bucket=self.config.s3_bucket,
                    Key=key_path,
                    Body=json.dumps(key_data, separators=(",", ":")),
                    ContentType="application/json",
                    IfMatch=current_key_etag,
                )
                key_object_etag = response["ETag"]
            else:
                # CREATE new key object - use IfNoneMatch='*'
                response = self.s3.put_object(
                    Bucket=self.config.s3_bucket,
                    Key=key_path,
                    Body=json.dumps(key_data, separators=(",", ":")),
                    ContentType="application/json",
                    IfNoneMatch="*",
                )
                key_object_etag = response["ETag"]

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

    def get(self, key: str) -> Entry[str, Any]:
        """Get latest value for key (with conditional orphan check and fallback).

        Fast path: Single S3 read from key object (when repair check not needed)
        Slow path: Checks for orphans if repair_check_interval_ms has elapsed

        Raises KeyNotFoundError if key object doesn't exist and no orphan fallback available.
        """
        # Conditional orphan check based on time interval
        current_time_ms = int(time.time() * 1000)
        time_since_last_check = current_time_ms - self._last_repair_check_ms

        # Check if we need to perform orphan repair check
        if time_since_last_check >= self.config.repair_check_interval_ms:
            # Skip repair attempt if we know we're read-only
            if not (self._can_write is False or self.config.read_only):
                # Perform orphan check and repair
                result = self._get_latest_and_repair()
                if result["can_write"] is not None:
                    self._can_write = result["can_write"]
                if result["orphan_status"] is not None:
                    self._latest_orphan_status = result["orphan_status"]
                self._last_repair_check_ms = current_time_ms

        # Try to read from key object
        key_path = f"{self.config.s3_prefix}keys/{key}.json"
        try:
            response = self.s3.get_object(Bucket=self.config.s3_bucket, Key=key_path)
            data = json.loads(response["Body"].read())

            return Entry(
                key=data["key"],
                value=data["value"],
                timestamp_ms=data["timestamp_ms"],
                version_id=data["log_version_id"],
                sequence=data["sequence"],
                previous_version_id=None,  # Not stored in key object
                hash=data["hash"],
                previous_hash=data["previous_hash"],
                previous_key_object_etag=None,
            )

        except ClientError as e:
            if e.response["Error"]["Code"] in ["NoSuchKey", "404"]:
                # Key object doesn't exist - check for orphan fallback
                if (
                    self._latest_orphan_status
                    and self._latest_orphan_status.get("is_orphaned")
                    and self._latest_orphan_status.get("orphan_key") == key
                    and self._latest_orphan_status.get("orphan_entry")
                    and (self._can_write is False or self.config.read_only)
                ):
                    # Return cached orphan entry (read-only mode)
                    return self._latest_orphan_status["orphan_entry"]  # type: ignore

                raise KeyNotFoundError(f"Key '{key}' not found")
            else:
                raise

    def get_log_version(self, version_id: str) -> Entry[str, Any]:
        """Get specific log version by S3 version ID."""
        try:
            response = self.s3.get_object(
                Bucket=self.config.s3_bucket, Key=self.log_key, VersionId=version_id
            )
            data = json.loads(response["Body"].read())

            return Entry(
                key=data["key"],
                value=data["value"],
                timestamp_ms=data["timestamp_ms"],
                version_id=version_id,
                sequence=data["sequence"],
                previous_version_id=data.get("previous_version_id"),
                hash=data["hash"],
                previous_hash=data["previous_hash"],
                previous_key_object_etag=data.get("previous_key_object_etag"),
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ["NoSuchKey", "NoSuchVersion", "404"]:
                raise KeyNotFoundError(f"Log version '{version_id}' not found")
            raise

    def history(
        self, key: str, before_version_id: Optional[str], limit: Optional[int]
    ) -> Tuple[List[Entry[str, Any]], Optional[str]]:
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
        key_path = f"{self.config.s3_prefix}keys/{key}.json"
        entries: List[Entry[str, Any]] = []

        # Check if we should prepend orphan entry
        prepend_orphan = False
        if (
            before_version_id is None
            and self._latest_orphan_status
            and self._latest_orphan_status.get("is_orphaned")
            and self._latest_orphan_status.get("orphan_key") == key
            and self._latest_orphan_status.get("orphan_entry")
        ):
            prepend_orphan = True
            entries.append(self._latest_orphan_status["orphan_entry"])  # type: ignore

        # List versions of key object
        try:
            key_marker: Optional[str] = None
            version_id_marker: Optional[str] = before_version_id

            while True:
                list_params: Dict[str, Any] = {
                    "Bucket": self.config.s3_bucket,
                    "Prefix": key_path,
                }
                if key_marker:
                    list_params["KeyMarker"] = key_marker
                if version_id_marker:
                    list_params["VersionIdMarker"] = version_id_marker

                page = self.s3.list_object_versions(**list_params)
                versions = page.get("Versions", [])
                for version in versions:
                    if version["Key"] != key_path:
                        continue

                    # Skip the before_version_id itself
                    if before_version_id and version["VersionId"] == before_version_id:
                        continue

                    # Fetch version data
                    response = self.s3.get_object(
                        Bucket=self.config.s3_bucket,
                        Key=key_path,
                        VersionId=version["VersionId"],
                    )
                    data = json.loads(response["Body"].read())

                    entry = Entry(
                        key=data["key"],
                        value=data["value"],
                        timestamp_ms=data["timestamp_ms"],
                        version_id=data["log_version_id"],
                        sequence=data["sequence"],
                        previous_version_id=None,
                        hash=data["hash"],
                        previous_hash=data["previous_hash"],
                        previous_key_object_etag=None,
                    )
                    entries.append(entry)

                    # Check limit
                    if limit is not None and len(entries) >= limit:
                        oldest_version_id: Optional[str] = version["VersionId"]
                        return (entries, oldest_version_id)

                # Check if more pages
                if not page.get("IsTruncated", False):
                    break

                key_marker = page.get("NextKeyMarker")
                version_id_marker = page.get("NextVersionIdMarker")

        except ClientError as e:
            if e.response["Error"]["Code"] in ["NoSuchKey", "404"]:
                # No key object exists - return orphan if available
                if prepend_orphan:
                    return (entries, None)
                return ([], None)
            raise

        # Return all entries (or empty if none found)
        oldest_version_id = entries[-1].version_id if entries and not prepend_orphan else None
        return (entries, oldest_version_id)

    def log_entries(
        self, before_version_id: Optional[str], limit: Optional[int]
    ) -> List[Entry[str, Any]]:
        """Get entries from global log (descending order - newest first).

        Args:
            before_version_id: Return entries before this log version ID (exclusive).
            limit: Maximum number of entries to return. Pass None for unlimited.

        Returns:
            List of entries in descending order (newest first)
        """
        entries: List[Entry[str, Any]] = []

        try:
            key_marker: Optional[str] = None
            version_id_marker: Optional[str] = before_version_id

            while True:
                list_params: Dict[str, Any] = {
                    "Bucket": self.config.s3_bucket,
                    "Prefix": self.log_key,
                }
                if key_marker:
                    list_params["KeyMarker"] = key_marker
                if version_id_marker:
                    list_params["VersionIdMarker"] = version_id_marker

                page = self.s3.list_object_versions(**list_params)
                versions = page.get("Versions", [])
                for version in versions:
                    if version["Key"] != self.log_key:
                        continue

                    # Skip the before_version_id itself
                    if before_version_id and version["VersionId"] == before_version_id:
                        continue

                    # Fetch version data
                    response = self.s3.get_object(
                        Bucket=self.config.s3_bucket,
                        Key=self.log_key,
                        VersionId=version["VersionId"],
                    )
                    data = json.loads(response["Body"].read())

                    entry = Entry(
                        key=data["key"],
                        value=data["value"],
                        timestamp_ms=data["timestamp_ms"],
                        version_id=version["VersionId"],
                        sequence=data["sequence"],
                        previous_version_id=data.get("previous_version_id"),
                        hash=data["hash"],
                        previous_hash=data["previous_hash"],
                        previous_key_object_etag=data.get("previous_key_object_etag"),
                    )
                    entries.append(entry)

                    # Check limit
                    if limit is not None and len(entries) >= limit:
                        return entries

                # Check if more pages
                if not page.get("IsTruncated", False):
                    break

                key_marker = page.get("NextKeyMarker")
                version_id_marker = page.get("NextVersionIdMarker")

        except ClientError as e:
            if e.response["Error"]["Code"] in ["NoSuchKey", "404"]:
                return []
            raise

        return entries

    def list_keys(self, after_key: Optional[str], limit: Optional[int]) -> List[str]:
        """List all keys in the system (lexicographic order).

        Args:
            after_key: Return keys after this key (exclusive, lexicographic order).
            limit: Maximum number of keys to return. Pass None for unlimited.

        Returns:
            List of key names in lexicographic order
        """
        keys: List[str] = []
        prefix = f"{self.config.s3_prefix}keys/"

        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(
                Bucket=self.config.s3_bucket,
                Prefix=prefix,
                StartAfter=f"{prefix}{after_key}" if after_key else prefix,
            )

            for page in page_iterator:
                contents = page.get("Contents", [])
                for obj in contents:
                    # Extract key name from path
                    key_name = obj["Key"][len(prefix) :]
                    if key_name.endswith(".json"):
                        key_name = key_name[:-5]
                        keys.append(key_name)

                        # Check limit
                        if limit is not None and len(keys) >= limit:
                            return keys

        except ClientError:
            return []

        return keys

    def verify(self, entry: Entry[str, Any]) -> bool:
        """Verify single entry integrity."""
        entry_for_hash: LogEntryForHash[str, Any] = {
            "sequence": entry.sequence,
            "key": entry.key,
            "value": entry.value,
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
        entries = self.log_entries(None, limit)

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

    def close(self) -> None:
        """Close client and cleanup resources."""
        # boto3 clients don't need explicit cleanup
        pass

    def __enter__(self) -> "ImmuKVClient":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()

    # ===== Private Helper Methods =====

    def _calculate_hash(self, entry_for_hash: LogEntryForHash[str, Any]) -> str:
        """Calculate SHA-256 hash for a log entry.

        Hash Input Fields (in exact order):
        1. sequence - The entry number (integer)
        2. key - The key being written (string)
        3. value - The value being written (canonical JSON)
        4. timestamp_ms - The timestamp in epoch milliseconds (integer)
        5. previous_hash - The hash from the previous entry (string)

        Canonical String Format: <sequence>|<key>|<value_json>|<timestamp_ms>|<previous_hash>
        """
        # Canonicalize value (sorted keys, no whitespace)
        value_json = json.dumps(entry_for_hash["value"], sort_keys=True, separators=(",", ":"))

        # Construct canonical string
        canonical = (
            f"{entry_for_hash['sequence']}|"
            f"{entry_for_hash['key']}|"
            f"{value_json}|"
            f"{entry_for_hash['timestamp_ms']}|"
            f"{entry_for_hash['previous_hash']}"
        )

        # Encode to UTF-8 bytes
        data_bytes = canonical.encode("utf-8")

        # Compute SHA-256 hash
        hash_bytes = hashlib.sha256(data_bytes).digest()

        # Convert to hex and add prefix
        hash_hex = hash_bytes.hex()
        return f"sha256:{hash_hex}"

    def _get_latest_and_repair(self) -> Dict[str, Any]:
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
            response = self.s3.get_object(Bucket=self.config.s3_bucket, Key=self.log_key)
            log_etag = response["ETag"]
            current_version_id = response["VersionId"]
            data = json.loads(response["Body"].read())

            prev_version_id = current_version_id
            prev_hash = data["hash"]
            sequence = data["sequence"]

            # Create entry from latest log data
            latest_entry = Entry(
                key=data["key"],
                value=data["value"],
                timestamp_ms=data["timestamp_ms"],
                version_id=current_version_id,
                sequence=data["sequence"],
                previous_version_id=data.get("previous_version_id"),
                hash=data["hash"],
                previous_hash=data["previous_hash"],
                previous_key_object_etag=data.get("previous_key_object_etag"),
            )

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

        except ClientError as e:
            if e.response["Error"]["Code"] in ["NoSuchKey", "404"]:
                # First entry - use genesis hash
                return {
                    "log_etag": None,
                    "prev_version_id": None,
                    "prev_hash": "sha256:genesis",
                    "sequence": -1,  # Will become 0
                    "can_write": None,
                    "orphan_status": None,
                }
            raise

    def _repair_orphan(
        self, latest_log: Entry[str, Any]
    ) -> Tuple[Optional[bool], Optional[OrphanStatus]]:
        """Repair orphaned log entry by propagating to key object.

        Uses stored previous ETag from log entry for idempotent conditional write.

        Args:
            latest_log: The latest log entry to repair

        Returns:
            Tuple of (can_write, orphan_status)
        """
        # Skip if in read-only mode or we know we can't write
        if self.config.read_only or self._can_write is False:
            # Check if this key object exists
            key_path = f"{self.config.s3_prefix}keys/{latest_log.key}.json"
            try:
                self.s3.head_object(Bucket=self.config.s3_bucket, Key=key_path)
                # Key object exists - not orphaned
                orphan_status: OrphanStatus = {
                    "is_orphaned": False,
                    "orphan_key": None,
                    "orphan_entry": None,
                    "checked_at": int(time.time() * 1000),
                }
                return (self._can_write, orphan_status)
            except ClientError as e:
                if e.response["Error"]["Code"] in ["NoSuchKey", "404"]:
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
        key_path = f"{self.config.s3_prefix}keys/{latest_log.key}.json"

        # Prepare repair data
        repair_data = {
            "sequence": latest_log.sequence,
            "key": latest_log.key,
            "value": latest_log.value,
            "timestamp_ms": latest_log.timestamp_ms,
            "log_version_id": latest_log.version_id,
            "hash": latest_log.hash,
            "previous_hash": latest_log.previous_hash,
        }

        try:
            if latest_log.previous_key_object_etag:
                # UPDATE with IfMatch=<previous_etag>
                self.s3.put_object(
                    Bucket=self.config.s3_bucket,
                    Key=key_path,
                    Body=json.dumps(repair_data, separators=(",", ":")),
                    ContentType="application/json",
                    IfMatch=latest_log.previous_key_object_etag,
                )
                logger.info(f"Propagated log entry to key object for {latest_log.key}")
            else:
                # CREATE with IfNoneMatch='*'
                self.s3.put_object(
                    Bucket=self.config.s3_bucket,
                    Key=key_path,
                    Body=json.dumps(repair_data, separators=(",", ":")),
                    ContentType="application/json",
                    IfNoneMatch="*",
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

        except ClientError as e:
            error_code = e.response["Error"]["Code"]

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

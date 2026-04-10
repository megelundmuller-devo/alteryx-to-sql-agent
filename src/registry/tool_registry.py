"""Persistent registry for learned tool translations.

Unknown Alteryx tool types are translated by the LLM at most once.  After a
successful translation, the result is saved here as a JSON file.  On the next
run the registry is checked before any LLM call, so repeated encounters with
the same plugin incur zero API cost.

Registry location (in priority order):
  1. `TOOL_REGISTRY_PATH` environment variable (absolute path to JSON file)
  2. `~/.alteryx_to_sql/tool_registry.json`  (user-global default)

Usage:
    from registry.tool_registry import ToolRegistry, default_registry

    reg = default_registry()          # or ToolRegistry(path)
    entry = reg.lookup("Vendor.Plugin.Name")
    if entry is None:
        # ... translate with LLM ...
        reg.save(entry)
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path

from parsing.models import RegistryEntry

_DEFAULT_REGISTRY_PATH = Path.home() / ".alteryx_to_sql" / "tool_registry.json"

# Module-level lock table: one lock per registry file path.
# Ensures two ToolRegistry instances pointing to the same file coordinate writes.
_path_locks: dict[str, threading.Lock] = {}
_path_locks_lock = threading.Lock()


def _get_path_lock(path: Path) -> threading.Lock:
    key = str(path.resolve())
    with _path_locks_lock:
        if key not in _path_locks:
            _path_locks[key] = threading.Lock()
        return _path_locks[key]


def _config_hash(config: dict) -> str:
    """Return a short hash of a tool config dict for provenance tracking."""
    raw = json.dumps(config, sort_keys=True, default=str).encode()
    return hashlib.sha256(raw).hexdigest()[:12]


class ToolRegistry:
    """Thread-safe persistent JSON store for learned tool translations.

    The JSON file contains a top-level object keyed by plugin string.
    Writes use atomic rename (os.replace) to avoid corruption on crash.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = _get_path_lock(path)  # shared across all instances for this path
        self._cache: dict[str, RegistryEntry] | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def lookup(self, plugin: str) -> RegistryEntry | None:
        """Return the stored entry for `plugin`, or None if not yet learned."""
        entries = self._load()
        return entries.get(plugin)

    def save(self, entry: RegistryEntry) -> None:
        """Persist `entry` to the registry file (atomic write, thread-safe).

        Re-reads from disk under the lock so concurrent saves from different
        ToolRegistry instances pointing to the same file do not overwrite each other.
        """
        with self._lock:
            # Invalidate cache and re-read from disk to pick up any concurrent writes
            self._cache = None
            entries = self._load()
            entries[entry.plugin] = entry
            self._write(entries)

    def all_entries(self) -> list[RegistryEntry]:
        """Return all stored entries sorted by plugin string."""
        return sorted(self._load().values(), key=lambda e: e.plugin)

    def clear(self) -> None:
        """Remove all entries from the registry file."""
        with self._lock:
            self._write({})

    @property
    def path(self) -> Path:
        return self._path

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load(self) -> dict[str, RegistryEntry]:
        """Load the registry from disk, populating the in-process cache."""
        if self._cache is not None:
            return self._cache

        if not self._path.exists():
            self._cache = {}
            return self._cache

        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            self._cache = {k: RegistryEntry(**v) for k, v in raw.items()}
        except Exception:  # noqa: BLE001
            self._cache = {}

        return self._cache

    def _write(self, entries: dict[str, RegistryEntry]) -> None:
        """Atomically write `entries` to disk and update the in-process cache."""
        import tempfile

        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {k: v.model_dump() for k, v in entries.items()}
        content = json.dumps(payload, indent=2, ensure_ascii=False)

        # Write to a unique temp file in the same directory, then rename atomically.
        # Using a unique name (via NamedTemporaryFile) avoids collisions between
        # threads or processes writing concurrently.
        fd, tmp_str = tempfile.mkstemp(dir=self._path.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(content)
            os.replace(tmp_str, self._path)  # atomic on POSIX and Windows
        except Exception:
            with contextlib.suppress(OSError):
                os.unlink(tmp_str)
            raise

        self._cache = entries


# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------


def make_entry(
    plugin: str,
    tool_type: str,
    description: str,
    sql_body: str,
    config: dict,
) -> RegistryEntry:
    """Construct a RegistryEntry from the data available at save time."""
    return RegistryEntry(
        plugin=plugin,
        tool_type=tool_type,
        description=description,
        sql_body=sql_body,
        learned_at=datetime.now(timezone.utc).isoformat(),
        example_config_hash=_config_hash(config),
    )


def default_registry() -> ToolRegistry:
    """Return a ToolRegistry at the default location (respects TOOL_REGISTRY_PATH env var)."""
    path_str = os.environ.get("TOOL_REGISTRY_PATH", "")
    path = Path(path_str) if path_str else _DEFAULT_REGISTRY_PATH
    return ToolRegistry(path)

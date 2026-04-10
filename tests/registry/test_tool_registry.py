"""Tests for src/registry/tool_registry.py."""

import sys
import threading
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from parsing.models import RegistryEntry
from registry.tool_registry import ToolRegistry, make_entry


def _entry(plugin: str = "Vendor.Plugin.Name", tool_type: str = "custom") -> RegistryEntry:
    return make_entry(
        plugin=plugin,
        tool_type=tool_type,
        description="Test tool",
        sql_body="SELECT 1 AS _test",
        config={"Key": "value"},
    )


class TestRegistryLookup:
    def test_lookup_empty_registry_returns_none(self, tmp_path):
        reg = ToolRegistry(tmp_path / "reg.json")
        assert reg.lookup("Vendor.Plugin.Name") is None

    def test_lookup_after_save_returns_entry(self, tmp_path):
        reg = ToolRegistry(tmp_path / "reg.json")
        e = _entry()
        reg.save(e)
        result = reg.lookup("Vendor.Plugin.Name")
        assert result is not None
        assert result.plugin == "Vendor.Plugin.Name"
        assert result.sql_body == "SELECT 1 AS _test"

    def test_lookup_unknown_plugin_returns_none(self, tmp_path):
        reg = ToolRegistry(tmp_path / "reg.json")
        reg.save(_entry("Vendor.Plugin.A"))
        assert reg.lookup("Vendor.Plugin.B") is None


class TestRegistrySave:
    def test_save_creates_file(self, tmp_path):
        path = tmp_path / "reg.json"
        reg = ToolRegistry(path)
        reg.save(_entry())
        assert path.exists()

    def test_save_multiple_entries(self, tmp_path):
        reg = ToolRegistry(tmp_path / "reg.json")
        reg.save(_entry("Plugin.A", "type_a"))
        reg.save(_entry("Plugin.B", "type_b"))
        assert reg.lookup("Plugin.A") is not None
        assert reg.lookup("Plugin.B") is not None

    def test_save_overwrites_existing_entry(self, tmp_path):
        reg = ToolRegistry(tmp_path / "reg.json")
        reg.save(make_entry("P", "t", "old", "SELECT 0", {}))
        reg.save(make_entry("P", "t", "new", "SELECT 1", {}))
        assert reg.lookup("P").sql_body == "SELECT 1"

    def test_save_persists_across_instances(self, tmp_path):
        path = tmp_path / "reg.json"
        ToolRegistry(path).save(_entry())
        # New instance reads from disk
        result = ToolRegistry(path).lookup("Vendor.Plugin.Name")
        assert result is not None
        assert result.tool_type == "custom"


class TestRegistryAllEntries:
    def test_all_entries_empty(self, tmp_path):
        reg = ToolRegistry(tmp_path / "reg.json")
        assert reg.all_entries() == []

    def test_all_entries_sorted_by_plugin(self, tmp_path):
        reg = ToolRegistry(tmp_path / "reg.json")
        reg.save(_entry("Plugin.Z"))
        reg.save(_entry("Plugin.A"))
        reg.save(_entry("Plugin.M"))
        plugins = [e.plugin for e in reg.all_entries()]
        assert plugins == sorted(plugins)


class TestRegistryClear:
    def test_clear_removes_all_entries(self, tmp_path):
        reg = ToolRegistry(tmp_path / "reg.json")
        reg.save(_entry("Plugin.A"))
        reg.save(_entry("Plugin.B"))
        reg.clear()
        assert reg.all_entries() == []

    def test_clear_on_empty_registry(self, tmp_path):
        reg = ToolRegistry(tmp_path / "reg.json")
        reg.clear()  # Should not raise
        assert reg.all_entries() == []


class TestRegistryConcurrentWrites:
    def test_concurrent_saves_no_corruption(self, tmp_path):
        """Two threads saving different entries concurrently — both must survive."""
        path = tmp_path / "reg.json"
        errors: list[Exception] = []

        def save_entries(plugins: list[str]) -> None:
            reg = ToolRegistry(path)
            for p in plugins:
                try:
                    reg.save(_entry(p))
                except Exception as exc:  # noqa: BLE001
                    errors.append(exc)

        t1 = threading.Thread(target=save_entries, args=(["P.A", "P.B", "P.C"],))
        t2 = threading.Thread(target=save_entries, args=(["P.D", "P.E", "P.F"],))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert not errors
        # All 6 entries must be present
        final = ToolRegistry(path)
        saved = {e.plugin for e in final.all_entries()}
        assert {"P.A", "P.B", "P.C", "P.D", "P.E", "P.F"}.issubset(saved)


class TestMakeEntry:
    def test_make_entry_fields(self):
        e = make_entry("P", "t", "desc", "SELECT 1", {"k": "v"})
        assert e.plugin == "P"
        assert e.tool_type == "t"
        assert e.description == "desc"
        assert e.sql_body == "SELECT 1"
        assert len(e.example_config_hash) == 12
        assert "T" in e.learned_at  # ISO datetime contains "T"

    def test_different_configs_different_hashes(self):
        e1 = make_entry("P", "t", "d", "s", {"a": 1})
        e2 = make_entry("P", "t", "d", "s", {"a": 2})
        assert e1.example_config_hash != e2.example_config_hash

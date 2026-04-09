"""Smoke test for src/main.py entry point."""

from main import main


def test_main_runs(capsys):
    main()
    captured = capsys.readouterr()
    assert "Nodes in DAG:" in captured.out
    assert "Sources:" in captured.out
    assert "Chain:" in captured.out

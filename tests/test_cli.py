from __future__ import annotations

from click.testing import CliRunner

from ducktail.cli import cli


def test_version_output() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert "version" in result.output.lower()


def test_tail_help() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["tail", "--help"])
    assert result.exit_code == 0
    assert "Tail a DuckLake table" in result.output
    assert "--connection" in result.output


def test_tail_missing_connection_fails() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["tail", "my_table"])
    assert result.exit_code != 0
    assert "Missing option" in result.output or "--connection" in result.output

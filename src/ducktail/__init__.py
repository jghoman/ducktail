"""Ducktail — Tail DuckLake tables via CDC."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("ducktail")
except PackageNotFoundError:  # uninstalled source checkout
    __version__ = "0.0.0+unknown"

"""Shared path constants used across the package.

These are POSIX-style paths intended for use inside containers as well as
on the host when constructing mappings. The JOBROOT may be overridden via
the JOBROOT environment variable.
"""
import os
from pathlib import PurePosixPath

JOBROOT = PurePosixPath(os.environ.get("JOBROOT", "/cwl_job"))
INROOT = PurePosixPath(JOBROOT / "inputs")

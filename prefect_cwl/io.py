"""Command rendering and interpolation utilities for CWL v1.2.

This module provides helpers to render CWL-style expressions and build the
final argv list for a `CommandLineTool` step, including handling of inputs,
arguments, prefixes, positions, separators, and initial workdir listings.
"""

from __future__ import annotations

import datetime as dt
import os
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, unquote

from prefect_cwl.models import CommandLineToolNode

_INTERP = re.compile(r"\$\(([^)]+)\)")


def _cli_token_value(v: Any) -> str:
    """Convert a value into the CLI token form.

    - For CWL File/Directory objects, return their resolved path.
    - Otherwise use _to_string.
    """
    if isinstance(v, dict) and v.get("class") in {"File", "Directory"}:
        # Prefer the same resolution logic used by .path
        p = _cwl_path(v)  # uses v["path"] or derives from file:// location
        return _to_string(p)
    return _to_string(v)


def _to_string(value: Any) -> str:
    """Return a string representation suitable for CLI tokens.

    - ``None`` maps to an empty string.
    - ``bool`` maps to ``"true"``/``"false"``.
    - ``datetime``/``date``/``time`` use ISO-8601 via ``.isoformat()``.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    return str(value)


# Minimal expression grammar + optional attribute access
# Supports:
# - inputs.KEY
# - workflow.KEY
# - self
# - inputs.KEY[IDX]
# - workflow.KEY[IDX]
# - self[IDX]
# plus optional:
# - .path / .location / .basename / .nameroot / .nameext
_EXPR_RE = re.compile(
    r"""
    ^\s*
    (?:
        self
        (?:\[(?P<self_index>\d+)\])?
        (?:\.(?P<self_attr>path|location|basename|nameroot|nameext))?
      |
        (?P<root>inputs|workflow)
        \.
        (?P<key>[A-Za-z_]\w*)
        (?:\[(?P<index>\d+)\])?
        (?:\.(?P<attr>path|location|basename|nameroot|nameext))?
    )
    \s*$
    """,
    re.VERBOSE,
)


def _file_url_to_path(maybe_url: str) -> Optional[str]:
    """Convert file:// URL to a local-ish path string when possible."""
    try:
        p = urlparse(maybe_url)
    except Exception:
        return None
    if p.scheme != "file":
        return None
    # urlparse puts the path in .path; unquote for %20 etc.
    return unquote(p.path) if p.path else None


def _cwlobj_get(v: Any, key: str) -> Any:
    """Get attribute/key from dict-like or object-like CWL value."""
    if isinstance(v, dict):
        return v.get(key)
    return getattr(v, key, None)


def _cwl_class(v: Any) -> Optional[str]:
    """Return CWL class for File/Directory objects if available."""
    c = _cwlobj_get(v, "class")
    return c if isinstance(c, str) else None


def _cwl_location(v: Any) -> Optional[str]:
    loc = _cwlobj_get(v, "location")
    return loc if isinstance(loc, str) else None


def _cwl_path(v: Any) -> Optional[str]:
    """Resolve a usable filesystem path for File/Directory values.

    Preference order:
    1) v.path if present
    2) v.location if it's file://... -> converted
    3) v.location if it's already a plain path string (no scheme)
    """
    p = _cwlobj_get(v, "path")
    if isinstance(p, str) and p:
        return p

    loc = _cwl_location(v)
    if not loc:
        return None

    # If location is a file:// URL, convert
    as_path = _file_url_to_path(loc)
    if as_path:
        return as_path

    # If location has a scheme (http, s3, etc) we can't map it to a path here
    try:
        parsed = urlparse(loc)
        if parsed.scheme:
            return None
    except Exception:
        pass

    # Otherwise treat as a plain path
    return loc


def _basename_from_path(path: str) -> str:
    # strip trailing slashes to avoid basename("") cases
    path = path.rstrip("/\\")
    return os.path.basename(path)


def _split_ext(basename: str) -> Tuple[str, str]:
    # matches CWL-ish behavior: nameext includes the dot
    root, ext = os.path.splitext(basename)
    return root, ext


def _resolve_file_dir_attr(v: Any, attr: str) -> Any:
    """Resolve .path/.location/.basename/.nameroot/.nameext for File/Directory.

    Only meaningful for CWL File/Directory objects. If v isn't File/Directory,
    returns None.

    For .basename/.nameroot/.nameext:
    - use v.basename if provided
    - else derive from resolved path (or location filename)
    """
    cls = _cwl_class(v)
    if cls not in {"File", "Directory"}:
        return None

    if attr == "location":
        return _cwl_location(v)

    if attr == "path":
        return _cwl_path(v)

    # basename/nameroot/nameext
    if attr == "basename":
        b = _cwlobj_get(v, "basename")
        if isinstance(b, str) and b:
            return b
        p = _cwl_path(v)
        if isinstance(p, str) and p:
            return _basename_from_path(p)
        loc = _cwl_location(v)
        if isinstance(loc, str) and loc:
            # last resort: filename portion of location
            return _basename_from_path(loc)
        return None

    # derive from basename (explicit or computed)
    base = _resolve_file_dir_attr(v, "basename")
    if not isinstance(base, str) or not base:
        return None
    root, ext = _split_ext(base)

    if attr == "nameroot":
        return root
    if attr == "nameext":
        return ext

    return None


def _eval_expr(expr: str, ctx: Dict[str, Dict[str, Any]], self_value: Any) -> Any:
    """Evaluate a minimal CWL-style expression against a context."""
    m = _EXPR_RE.match(expr)
    if not m:
        raise ValueError(
            "Unsupported expression (expected inputs.X/workflow.X/self "
            "with optional [i] and optional .path/.location/.basename/.nameroot/.nameext): "
            f"{expr}"
        )

    # Handle self / self[index] / self.attr
    if m.group("root") is None and expr.strip().startswith("self"):
        v = self_value
        idx = m.group("self_index")
        if idx is not None:
            if not isinstance(v, (list, tuple)):
                return None
            i = int(idx)
            v = v[i] if 0 <= i < len(v) else None

        self_attr = m.group("self_attr")
        if self_attr:
            return _resolve_file_dir_attr(v, self_attr)

        return v

    # inputs.KEY / workflow.KEY (optionally [i], optionally .attr)
    root = m.group("root")
    key = m.group("key")
    idx = m.group("index")
    attr = m.group("attr")

    v = ctx.get(root, {}).get(key)

    if idx is not None:
        if not isinstance(v, (list, tuple)):
            return None
        i = int(idx)
        v = v[i] if 0 <= i < len(v) else None

    if attr:
        return _resolve_file_dir_attr(v, attr)

    return v


def interpolate(template: str, ctx: Dict[str, Dict[str, Any]], self_value: Any = None) -> str:
    """Render a template by interpolating ``$(...)`` expressions."""
    def repl(m: re.Match) -> str:
        v = _eval_expr(m.group(1), ctx, self_value)
        if isinstance(v, (list, tuple)):
            return " ".join(_to_string(x) for x in v)
        return _to_string(v)

    return _INTERP.sub(repl, str(template))


def _split_tokens(s: str) -> List[str]:
    """Split a string into tokens using simple whitespace rules."""
    return s.split()


def _get_value(values: Dict[str, Dict[str, Any]], key: str) -> Optional[Any]:
    """Read a key from ``values['inputs']`` or fallback to ``values['workflow']``."""
    if key in values.get("inputs", {}):
        return values["inputs"][key]
    if key in values.get("workflow", {}):
        return values["workflow"][key]
    return None


def _binding_position(binding: Any) -> Optional[int]:
    """Return the binding position, normalizing negative and missing values."""
    pos = getattr(binding, "position", None)
    if pos is None:
        return None
    if isinstance(pos, int) and pos < 0:
        return None
    return pos


def _binding_prefix(binding: Any) -> Optional[str]:
    """Return the binding prefix if present."""
    return getattr(binding, "prefix", None)


def _input_tokens(clt: CommandLineToolNode, key: str, values: Dict[str, Dict[str, Any]]) -> List[str]:
    """Build CLI tokens for a tool input based on its binding and value."""
    tool_input = clt.inputs.get(key)
    if tool_input is None or tool_input.inputBinding is None:
        return []

    raw = _get_value(values, key)
    if raw is None:
        return []

    prefix = _binding_prefix(tool_input.inputBinding)

    if isinstance(raw, (list, tuple)):
        items = [_cli_token_value(x) for x in raw if x is not None]
        items = [x for x in items if x != ""]

        sep = getattr(tool_input.inputBinding, "separator", None)
        if sep:
            joined = sep.join(items)
            if prefix:
                return [prefix, joined]
            return [joined]

        # default: repeat prefix per item (first item gets prefix)
        out: List[str] = []
        for i, item in enumerate(items):
            if prefix and i == 0:
                out.extend([prefix, item])
            else:
                out.append(item)
        return out

    val = _cli_token_value(raw)
    if val == "":
        return []
    if prefix:
        return [prefix, val]
    return [val]


def _arg_position(arg: Any) -> Optional[int]:
    """Return the argument position if provided."""
    if isinstance(arg, dict):
        pos = arg.get("position", None)
        return pos if isinstance(pos, int) else None
    return None


def _arg_prefix(arg: Any) -> Optional[str]:
    """Return the argument prefix if provided."""
    if isinstance(arg, dict):
        p = arg.get("prefix", None)
        return p if isinstance(p, str) else None
    return None


def _argument_tokens(arg: Any, ctx: Dict[str, Dict[str, Any]]) -> List[str]:
    """Build CLI tokens for an ``arguments`` entry."""
    if isinstance(arg, str):
        rendered = interpolate(arg, ctx)
        return [rendered] if rendered != "" else []

    if isinstance(arg, dict):
        prefix = _arg_prefix(arg)

        if "valueFrom" in arg and arg["valueFrom"] is not None:
            rendered = interpolate(str(arg["valueFrom"]), ctx, self_value=None)
            toks = [rendered] if rendered != "" else []
            return ([prefix] + toks) if prefix else toks

        if "value" in arg and arg["value"] is not None:
            v = arg["value"]
            if isinstance(v, str):
                rendered = interpolate(v, ctx)
                toks = [rendered] if rendered != "" else []
            elif isinstance(v, (list, tuple)):
                toks = [interpolate(str(x), ctx) for x in v]
                toks = [t for t in toks if t != ""]
            else:
                s = _to_string(v)
                toks = [s] if s != "" else []

            return ([prefix] + toks) if prefix else toks

        return [prefix] if prefix else []

    return []


def _base_command_tokens(clt: CommandLineToolNode, ctx: Dict[str, Dict[str, Any]]) -> List[str]:
    """Render the ``baseCommand`` into argv tokens."""
    base = clt.baseCommand
    if isinstance(base, str):
        rendered = interpolate(base, ctx)
        return [rendered] if rendered != "" else []
    return [interpolate(str(tok), ctx) for tok in base]


def _build_listing(clt: CommandLineToolNode, ctx: Dict[str, Dict[str, Any]]) -> List[Dict[str, str]]:
    """Render the InitialWorkDirRequirement listing entries."""
    req = getattr(clt, "requirements", None)
    if req is None:
        return []

    iwd = getattr(req, "initial_workdir_requirement", None)
    if iwd is None:
        return []

    out: List[Dict[str, str]] = []
    for item in getattr(iwd, "listing", []) or []:
        entryname = interpolate(item.entryname, ctx)
        entry = interpolate(item.entry, ctx)
        out.append({"entryname": entryname, "entry": entry})
    return out


def build_command_and_listing(
    clt: CommandLineToolNode,
    values: Dict[str, Dict[str, Any]],
) -> Tuple[List[str], List[Dict[str, str]]]:
    """Build the final argv for a tool and the rendered IWD listing."""
    ctx = {"inputs": values.get("inputs", {}), "workflow": values.get("workflow", {})}

    cmd: List[str] = _base_command_tokens(clt, ctx)

    entries: List[Tuple[Tuple[int, int], List[str]]] = []
    order = 0

    # arguments
    for arg in clt.arguments:
        pos = _arg_position(arg)
        sort_pos = pos if pos is not None else 10**9
        toks = _argument_tokens(arg, ctx)
        if toks:
            entries.append(((sort_pos, order), toks))
            order += 1

    # inputs
    for key, tool_input in clt.inputs.items():
        if tool_input.inputBinding is None:
            continue
        pos = _binding_position(tool_input.inputBinding)
        sort_pos = pos if pos is not None else 10**9
        toks = _input_tokens(clt, key, values)
        if toks:
            entries.append(((sort_pos, order), toks))
            order += 1

    entries.sort(key=lambda x: x[0])
    for _, toks in entries:
        cmd.extend(toks)

    cmd = [t for t in cmd if t != ""]
    listing = _build_listing(clt, ctx)
    return cmd, listing

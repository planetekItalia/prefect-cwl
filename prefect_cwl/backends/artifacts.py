"""Output artifact collection helpers owned by execution backends."""

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Union

from prefect_cwl.exceptions import ValidationError
from prefect_cwl.io import interpolate
from prefect_cwl.models import CommandLineToolNode

ArtifactPath = Union[Path, List[Path]]


@dataclass(frozen=True)
class OutputCollectionSpec:
    """Rendered output collection intent for a single output port."""

    outport: str
    rendered_glob: str
    base_type: str
    is_array: bool
    is_optional: bool


def _parse_output_type(output_type: str) -> tuple[str, bool, bool]:
    t = output_type.strip()
    is_optional = t.endswith("?")
    if is_optional:
        t = t[:-1].strip()
    is_array = t.endswith("[]")
    if is_array:
        t = t[:-2].strip()
    return t, is_array, is_optional


def _validate_rendered_glob(glob: str) -> None:
    if not glob:
        raise ValidationError("Rendered output glob must not be empty")
    if glob.startswith("/"):
        raise ValidationError(f"Rendered output glob must be relative: {glob!r}")
    if ".." in PurePosixPath(glob).parts:
        raise ValidationError(
            f"Rendered output glob must not contain '..' traversal: {glob!r}"
        )


def render_output_collection_specs(
    *, clt: CommandLineToolNode, values: Dict[str, Dict[str, Any]]
) -> List[OutputCollectionSpec]:
    """Render and validate output globs against runtime values."""
    ctx = {
        "inputs": values.get("inputs", {}),
        "workflow": values.get("workflow", {}),
    }

    specs: List[OutputCollectionSpec] = []
    for outport, outspec in clt.outputs.items():
        rendered_glob = interpolate(outspec.outputBinding.glob, ctx).strip()
        _validate_rendered_glob(rendered_glob)

        base_type, is_array, is_optional = _parse_output_type(outspec.type)
        specs.append(
            OutputCollectionSpec(
                outport=outport,
                rendered_glob=rendered_glob,
                base_type=base_type,
                is_array=is_array,
                is_optional=is_optional,
            )
        )

    return specs


def collect_out_artifacts_local(
    *,
    clt: CommandLineToolNode,
    host_outdir: Path,
    values: Dict[str, Dict[str, Any]],
) -> Dict[str, ArtifactPath]:
    """Collect step outputs from a locally reachable filesystem."""
    out_artifacts: Dict[str, ArtifactPath] = {}

    for spec in render_output_collection_specs(clt=clt, values=values):
        matches = sorted(host_outdir.glob(spec.rendered_glob), key=lambda p: str(p))

        if spec.base_type == "File":
            matches = [p for p in matches if p.is_file()]
        elif spec.base_type == "Directory":
            matches = [p for p in matches if p.is_dir()]

        if spec.is_array:
            out_artifacts[spec.outport] = matches
            continue

        if len(matches) == 0:
            if spec.is_optional:
                continue
            raise ValidationError(
                f"Output {spec.outport!r} expected one match for glob {spec.rendered_glob!r}, got none"
            )
        if len(matches) > 1:
            raise ValidationError(
                f"Output {spec.outport!r} expected one match for glob {spec.rendered_glob!r}, got {len(matches)}"
            )

        out_artifacts[spec.outport] = matches[0]

    return out_artifacts

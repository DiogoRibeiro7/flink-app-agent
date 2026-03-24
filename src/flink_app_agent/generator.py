"""Local template generator for the current single-template pipeline."""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path
import re

from .spec import FlinkJobSpec
from .utils import to_pascal_case


SAFE_TEXT_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".java",
        ".md",
        ".properties",
        ".txt",
        ".xml",
        ".yaml",
        ".yml",
    }
)
PLACEHOLDER_PATTERN = re.compile(r"\{\{[A-Z0-9_]+\}\}")


class TemplateRenderingError(ValueError):
    """Raised when rendered text files still contain unresolved placeholders."""


@dataclass(frozen=True)
class PlaceholderMapping:
    """Convert a validated spec into explicit template placeholders."""

    spec: FlinkJobSpec

    def as_dict(self) -> dict[str, str]:
        """Return the placeholder mapping used during template rendering."""
        return {f"{{{{{key}}}}}": value for key, value in self.spec.to_template_dict().items()}


@dataclass(frozen=True)
class TemplateRenderer:
    """Render supported text files with simple placeholder replacement."""

    text_extensions: frozenset[str] = SAFE_TEXT_EXTENSIONS

    def render_directory(self, root_dir: Path, placeholders: dict[str, str]) -> None:
        """Render all supported text files under the copied template tree."""
        for path in self.iter_text_files(root_dir):
            self.render_file(path, placeholders)

    def iter_text_files(self, root_dir: Path) -> list[Path]:
        """Return files that are safe to treat as text during rendering."""
        return sorted(
            path
            for path in root_dir.rglob("*")
            if path.is_file() and path.suffix in self.text_extensions
        )

    def render_file(self, path: Path, placeholders: dict[str, str]) -> None:
        """Render one text file and reject unresolved placeholders."""
        text = path.read_text(encoding="utf-8")
        rendered = self.render_text(text, placeholders)
        if rendered != text:
            path.write_text(rendered, encoding="utf-8")

    def render_text(self, text: str, placeholders: dict[str, str]) -> str:
        """Render text and fail if placeholders remain unresolved."""
        rendered = text
        for placeholder, value in placeholders.items():
            rendered = rendered.replace(placeholder, value)

        unresolved = sorted(set(PLACEHOLDER_PATTERN.findall(rendered)))
        if unresolved:
            unresolved_text = ", ".join(unresolved)
            raise TemplateRenderingError(
                f"Unresolved placeholders remain after rendering: {unresolved_text}"
            )
        return rendered


@dataclass(frozen=True)
class ProjectGenerator:
    """Generate a Flink project from the currently selected local template directory."""

    template_dir: Path
    renderer: TemplateRenderer = field(default_factory=TemplateRenderer)

    def generate(self, spec: FlinkJobSpec, output_dir: Path) -> list[Path]:
        """Copy the template, render placeholders, rename template classes, and list files."""
        self._validate_template_dir()
        self._validate_output_dir(output_dir)
        project_dir = self._copy_template(output_dir)
        placeholders = PlaceholderMapping(spec).as_dict()
        self.renderer.render_directory(project_dir, placeholders)
        self._rename_template_files(project_dir, spec)
        return self._list_generated_files(project_dir)

    def _validate_template_dir(self) -> None:
        """Ensure the configured template directory exists and is a directory."""
        if not self.template_dir.exists():
            raise FileNotFoundError(f"Template directory not found: {self.template_dir}")
        if not self.template_dir.is_dir():
            raise NotADirectoryError(f"Template path is not a directory: {self.template_dir}")

    def _validate_output_dir(self, output_dir: Path) -> None:
        """Reject invalid output paths before any files are copied."""
        if output_dir.exists():
            raise FileExistsError(f"Output path already exists: {output_dir}")
        if output_dir.parent.exists() and not output_dir.parent.is_dir():
            raise NotADirectoryError(f"Output parent is not a directory: {output_dir.parent}")

    def _copy_template(self, output_dir: Path) -> Path:
        """Copy the template directory into the requested output directory."""
        output_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(self.template_dir, output_dir)
        return output_dir

    def _rename_template_files(self, output_dir: Path, spec: FlinkJobSpec) -> None:
        """Rename the common Java template files to the resolved class names."""
        renames = {
            "JobTemplate.java": f"{build_main_class_name(spec.job_name)}.java",
            "InputEvent.java": f"{spec.input_event_name}.java",
            "OutputEvent.java": f"{spec.output_event_name}.java",
        }

        for path in sorted(output_dir.rglob("*"), key=lambda item: len(item.parts), reverse=True):
            if not path.is_file():
                continue
            new_name = renames.get(path.name)
            if new_name is None or new_name == path.name:
                continue
            target_path = path.with_name(new_name)
            if target_path.exists():
                raise FileExistsError(f"Cannot rename file because target already exists: {target_path}")
            path.rename(target_path)

    def _list_generated_files(self, output_dir: Path) -> list[Path]:
        """Return all generated files under the output directory."""
        return sorted(path for path in output_dir.rglob("*") if path.is_file())


def build_main_class_name(job_name: str) -> str:
    """Build the generated main Java class name from a normalized job name."""
    base_name = to_pascal_case(job_name)
    if base_name.endswith("Job"):
        return base_name
    return f"{base_name}Job"

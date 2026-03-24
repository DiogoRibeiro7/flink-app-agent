"""Local template-based project generation for Flink jobs."""

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
        ".json",
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
    """Raised when template rendering leaves unresolved placeholders behind."""


class TemplateSelectionError(ValueError):
    """Raised when no registered template matches the requested generation context."""


@dataclass(frozen=True)
class PlaceholderMapping:
    """Centralize conversion from a validated spec into template placeholders."""

    spec: FlinkJobSpec

    def as_dict(self) -> dict[str, str]:
        """Return the placeholder mapping used for template rendering."""
        return {
            f"{{{{{key}}}}}": value
            for key, value in self.spec.to_template_dict().items()
        }


@dataclass(frozen=True)
class TemplateRenderer:
    """Render text template files using a simple explicit placeholder mapping."""

    safe_text_extensions: frozenset[str] = SAFE_TEXT_EXTENSIONS

    def render_directory(self, root_dir: Path, placeholders: dict[str, str]) -> None:
        """Render supported text files in a directory tree."""
        for path in self.iter_renderable_files(root_dir):
            self.render_file(path, placeholders)

    def iter_renderable_files(self, root_dir: Path) -> list[Path]:
        """Return the text files that are safe to render."""
        return sorted(
            path
            for path in root_dir.rglob("*")
            if path.is_file() and path.suffix in self.safe_text_extensions
        )

    def render_file(self, path: Path, placeholders: dict[str, str]) -> None:
        """Render a single text file and reject unresolved placeholders."""
        text = path.read_text(encoding="utf-8")
        rendered = self.render_text(text, placeholders)
        if rendered != text:
            path.write_text(rendered, encoding="utf-8")

    def render_text(self, text: str, placeholders: dict[str, str]) -> str:
        """Render text and ensure all template placeholders are resolved."""
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
class TemplateMetadata:
    """Metadata for a registered local project template."""

    name: str
    template_path: Path
    supported_rule_types: frozenset[str]


@dataclass(frozen=True)
class TemplateCatalog:
    """Resolve registered templates and select one for a validated spec."""

    templates: tuple[TemplateMetadata, ...]

    @classmethod
    def from_root(cls, templates_root: Path) -> "TemplateCatalog":
        """Build the catalog for the currently supported local templates."""
        return cls(
            templates=(
                TemplateMetadata(
                    name="flink_kafka_rule_job",
                    template_path=templates_root / "flink_kafka_rule_job",
                    supported_rule_types=frozenset({"two_events_within_window"}),
                ),
            )
        )

    def get(self, template_name: str) -> TemplateMetadata:
        """Return a registered template by name."""
        for template in self.templates:
            if template.name == template_name:
                return template
        raise TemplateSelectionError(f"Unknown template: {template_name}")

    def select_for_spec(self, spec: FlinkJobSpec) -> TemplateMetadata:
        """Select the first registered template that supports the spec rule type."""
        for template in self.templates:
            if spec.rule_type in template.supported_rule_types:
                return template
        raise TemplateSelectionError(
            f"No registered template supports rule_type '{spec.rule_type}'."
        )


@dataclass(frozen=True)
class ProjectGenerator:
    """Generate a Flink project by copying and filling a local template directory."""

    template_dir: Path
    renderer: TemplateRenderer = field(default_factory=TemplateRenderer)
    safe_text_extensions: frozenset[str] = field(default=SAFE_TEXT_EXTENSIONS)

    def generate(self, spec: FlinkJobSpec, output_dir: Path) -> list[Path]:
        """Generate a project in ``output_dir`` and return all generated file paths."""
        self._validate_template_dir()
        self._validate_output_dir(output_dir)

        self._copy_template_directory(output_dir)
        placeholders = PlaceholderMapping(spec).as_dict()
        self.renderer.render_directory(output_dir, placeholders)
        self._rename_template_classes(output_dir, spec)
        return self._list_generated_files(output_dir)

    def _validate_template_dir(self) -> None:
        """Ensure the template directory exists and is usable."""
        if not self.template_dir.exists():
            raise FileNotFoundError(f"Template directory not found: {self.template_dir}")
        if not self.template_dir.is_dir():
            raise NotADirectoryError(f"Template path is not a directory: {self.template_dir}")

    def _validate_output_dir(self, output_dir: Path) -> None:
        """Reject invalid output paths before copying files."""
        if output_dir.exists():
            raise FileExistsError(f"Output path already exists: {output_dir}")

        parent = output_dir.parent
        if parent.exists() and not parent.is_dir():
            raise NotADirectoryError(f"Output parent is not a directory: {parent}")

    def _copy_template_directory(self, output_dir: Path) -> None:
        """Copy the template directory into the requested output directory."""
        output_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(self.template_dir, output_dir)

    def _rename_template_classes(self, root_dir: Path, spec: FlinkJobSpec) -> None:
        """Rename common template class files to match the resolved spec."""
        renames = {
            "InputEvent.java": f"{spec.input_event_name}.java",
            "OutputEvent.java": f"{spec.output_event_name}.java",
            "JobTemplate.java": f"{build_main_class_name(spec.job_name)}.java",
        }

        for path in sorted(root_dir.rglob("*"), key=lambda item: len(item.parts), reverse=True):
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


def select_template_for_spec(spec: FlinkJobSpec, templates_root: Path) -> TemplateMetadata:
    """Select template metadata explicitly for a validated spec."""
    catalog = TemplateCatalog.from_root(templates_root)
    return catalog.select_for_spec(spec)


def build_main_class_name(job_name: str) -> str:
    """Build the generated main job class name from a normalized job name."""
    base_name = to_pascal_case(job_name)
    if base_name.endswith("Job"):
        return base_name
    return f"{base_name}Job"

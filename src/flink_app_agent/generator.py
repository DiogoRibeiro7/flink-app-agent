"""Local template-based project generation for Flink jobs."""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path

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


@dataclass(frozen=True)
class ProjectGenerator:
    """Generate a Flink project by copying and filling a local template directory."""

    template_dir: Path
    safe_text_extensions: frozenset[str] = field(default=SAFE_TEXT_EXTENSIONS)

    def generate(self, spec: FlinkJobSpec, output_dir: Path) -> list[Path]:
        """Generate a project in ``output_dir`` and return all generated file paths."""
        self._validate_template_dir()
        self._validate_output_dir(output_dir)

        output_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(self.template_dir, output_dir)

        replacements = self._build_replacements(spec)
        self._replace_placeholders(output_dir, replacements)
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

    def _build_replacements(self, spec: FlinkJobSpec) -> dict[str, str]:
        """Build the supported placeholder map for text replacement."""
        return {
            "{{JOB_NAME}}": spec.job_name,
            "{{SOURCE_TOPIC}}": spec.source_topic,
            "{{SINK_TOPIC}}": spec.sink_topic,
            "{{KEY_BY}}": spec.key_by,
            "{{EVENT_TIME_FIELD}}": spec.event_time_field,
            "{{INPUT_EVENT_NAME}}": spec.input_event_name,
            "{{OUTPUT_EVENT_NAME}}": spec.output_event_name,
            "{{RULE_TYPE}}": spec.rule_type,
            "{{RULE_CONDITION}}": spec.rule_condition,
            "{{TIME_WINDOW_MINUTES}}": str(spec.time_window_minutes),
        }

    def _replace_placeholders(self, root_dir: Path, replacements: dict[str, str]) -> None:
        """Replace placeholders in text files only."""
        for path in root_dir.rglob("*"):
            if not path.is_file() or path.suffix not in self.safe_text_extensions:
                continue

            text = path.read_text(encoding="utf-8")
            rendered = self._replace_tokens(text, replacements)
            if rendered != text:
                path.write_text(rendered, encoding="utf-8")

    def _replace_tokens(self, text: str, replacements: dict[str, str]) -> str:
        """Apply plain string placeholder replacement."""
        rendered = text
        for placeholder, value in replacements.items():
            rendered = rendered.replace(placeholder, value)
        return rendered

    def _rename_template_classes(self, root_dir: Path, spec: FlinkJobSpec) -> None:
        """Rename common template class files to match the resolved spec."""
        renames = {
            "InputEvent.java": f"{spec.input_event_name}.java",
            "OutputEvent.java": f"{spec.output_event_name}.java",
            "JobTemplate.java": f"{self._main_class_name(spec)}.java",
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

    def _main_class_name(self, spec: FlinkJobSpec) -> str:
        """Build the generated main job class name from the job name."""
        base_name = to_pascal_case(spec.job_name)
        if base_name.endswith("Job"):
            return base_name
        return f"{base_name}Job"

    def _list_generated_files(self, output_dir: Path) -> list[Path]:
        """Return all generated files under the output directory."""
        return sorted(path for path in output_dir.rglob("*") if path.is_file())

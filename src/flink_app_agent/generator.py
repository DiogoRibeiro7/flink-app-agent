"""Template rendering logic for generated Flink projects."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

from .spec import FlinkJobSpec
from .utils import write_json_file


@dataclass
class ProjectGenerator:
    """Generate a Flink project from the single supported template."""

    template_root: Path

    def generate(self, spec: FlinkJobSpec, output_dir: Path) -> Path:
        """Copy the template and fill placeholders in text files."""
        template_dir = self.template_root / spec.template_id
        if not template_dir.exists():
            raise FileNotFoundError(f"Template not found: {template_dir}")

        project_dir = output_dir
        if project_dir.exists():
            raise FileExistsError(f"Output directory already exists: {project_dir}")

        shutil.copytree(template_dir, project_dir)
        replacements = self._build_replacements(spec)
        self._render_directory(project_dir, replacements)
        self._move_package_directories(project_dir, spec)
        self._rename_main_job_file(project_dir, spec)
        write_json_file(project_dir / "job_spec.json", spec.model_dump())
        return project_dir

    def _build_replacements(self, spec: FlinkJobSpec) -> dict[str, str]:
        """Build the placeholder map used across template files."""
        package_path = spec.package_name.replace(".", "/")
        return {
            "{{JOB_NAME}}": spec.job_name,
            "{{JOB_CLASS_NAME}}": spec.job_class_name,
            "{{PACKAGE_NAME}}": spec.package_name,
            "{{PACKAGE_PATH}}": package_path,
            "{{INPUT_TOPIC}}": spec.input_topic,
            "{{OUTPUT_TOPIC}}": spec.output_topic,
            "{{CONSUMER_GROUP}}": spec.consumer_group,
            "{{KEY_FIELD}}": spec.key_field,
            "{{RULE_EXPRESSION}}": spec.rule_expression,
            "{{BOOTSTRAP_SERVERS}}": spec.bootstrap_servers,
            "{{INPUT_SCHEMA_CLASS}}": spec.input_schema_class,
            "{{OUTPUT_SCHEMA_CLASS}}": spec.output_schema_class,
        }

    def _render_directory(self, root: Path, replacements: dict[str, str]) -> None:
        """Replace placeholders in all text files under the copied template."""
        for path in root.rglob("*"):
            if path.is_dir():
                continue
            text = path.read_text(encoding="utf-8")
            rendered_text = self._replace_tokens(text, replacements)
            if rendered_text != text:
                path.write_text(rendered_text, encoding="utf-8")

    def _replace_tokens(self, text: str, replacements: dict[str, str]) -> str:
        """Apply all placeholder replacements to a text fragment."""
        rendered = text
        for key, value in replacements.items():
            rendered = rendered.replace(key, value)
        return rendered

    def _move_package_directories(self, root: Path, spec: FlinkJobSpec) -> None:
        """Move the Java source tree from the template package to the target package."""
        package_parts = spec.package_name.split(".")
        for relative_root in ("src/main/java", "src/test/java"):
            source_dir = root / relative_root / "com" / "example"
            if not source_dir.exists():
                continue

            target_dir = root / relative_root
            for part in package_parts:
                target_dir = target_dir / part

            if source_dir == target_dir:
                continue

            target_dir.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source_dir), str(target_dir))

            # Remove the leftover empty template directories when possible.
            self._remove_empty_parents(root / relative_root / "com", stop_at=root / relative_root)

    def _rename_main_job_file(self, root: Path, spec: FlinkJobSpec) -> None:
        """Rename the generated main job file so it matches the public Java class."""
        package_dir = root / "src" / "main" / "java"
        for part in spec.package_name.split("."):
            package_dir = package_dir / part

        source_file = package_dir / "JobTemplate.java"
        if source_file.exists():
            source_file.rename(package_dir / f"{spec.job_class_name}.java")

    def _remove_empty_parents(self, path: Path, stop_at: Path) -> None:
        """Delete empty directories created by the template package layout."""
        current = path
        while current != stop_at and current.exists():
            try:
                current.rmdir()
            except OSError:
                break
            current = current.parent

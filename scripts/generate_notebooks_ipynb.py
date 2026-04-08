"""Generate .ipynb versions of Python modules under notebooks/ with proper Databricks formatting."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / "notebooks"
OUT_ROOT = ROOT / "notebooks_ipynb"


def _partition_code_into_sections(code_lines: list[str]) -> list[dict]:
    """
    Partition Python code into logical sections (imports, functions, main).
    Returns list of sections with type, description, and source lines.
    """
    sections = []
    current_section = []
    current_type = "imports"
    import_section_ended = False
    function_count = 0

    for i, line in enumerate(code_lines):
        # Skip empty lines at the start
        if not line.strip() and not current_section and not import_section_ended:
            continue

        # Detect transition from imports to function definitions
        if (
            not import_section_ended
            and line.strip()
            and not line.startswith("import ")
            and not line.startswith("from ")
            and not line.startswith("#")
            and not line.startswith("__pycache__")
        ):
            if current_section:
                # Save imports section
                sections.append({
                    "type": "imports",
                    "description": "Import required libraries and dependencies",
                    "content": current_section,
                })
                current_section = []
            import_section_ended = True

        # Detect function/class definitions
        if (line.strip().startswith("def ") or line.strip().startswith("class ")) and import_section_ended:
            if current_section and any(l.strip() for l in current_section):
                # Save current section before function
                desc = _infer_section_description(current_section)
                sections.append({
                    "type": "code",
                    "description": desc,
                    "content": current_section,
                })
                current_section = []
            current_section.append(line)
            function_count += 1
        else:
            current_section.append(line)

    # Save final section
    if current_section and any(line.strip() for line in current_section):
        if import_section_ended:
            desc = _infer_section_description(current_section)
        else:
            desc = "Remaining imports and module-level configuration"
        sections.append({
            "type": "imports" if not import_section_ended else "code",
            "description": desc,
            "content": current_section,
        })

    return sections


def _infer_section_description(code_lines: list[str]) -> str:
    """Infer a description of what this code section does."""
    text = "\n".join(code_lines)
    
    # Check for function/class definition
    for line in code_lines:
        if line.strip().startswith("def "):
            func_name = line.strip().split("(")[0].replace("def ", "").strip()
            # Check for docstring
            if any('"""' in l or "'''" in l for l in code_lines):
                return f"Define `{func_name}()` function with logic for processing"
            return f"Define `{func_name}()` helper function"
        if line.strip().startswith("class "):
            class_name = line.strip().split("(")[0].replace("class ", "").strip()
            return f"Define `{class_name}` class"
    
    # Infer from code patterns
    if "try:" in text:
        return "Error handling and validation logic"
    if "for " in text and "in " in text:
        return "Loop through and process items"
    if "if __name__" in text:
        return "Main entry point - execute when script is run directly"
    if "return " in text:
        return "Utility functions and helpers"
    
    return "Additional module code and configuration"


def _create_notebook_cells(py_file: Path, rel_path: Path, code_sections: list[dict]) -> list[dict]:
    """Create notebook cells with explanatory markdown before each code section."""
    cells = []

    # Title cell
    cells.append({
        "cell_type": "markdown",
        "metadata": {"language": "markdown"},
        "source": [
            f"# {py_file.stem}\n",
            f"\n",
            f"**Source:** `{rel_path.as_posix()}`  \n",
            f"**Purpose:** Databricks notebook auto-generated from framework Python module.\n",
        ],
    })

    # Add code cells with explanatory markdown
    for i, section in enumerate(code_sections, 1):
        source_lines = section["content"][:]
        
        # Clean up leading/trailing whitespace
        while source_lines and not source_lines[0].strip():
            source_lines.pop(0)
        while source_lines and not source_lines[-1].strip():
            source_lines.pop()

        if not source_lines:
            continue

        # Add markdown explanation before code
        cells.append({
            "cell_type": "markdown",
            "metadata": {"language": "markdown"},
            "source": [
                f"## Section {i}: {section['description']}\n",
                f"\n",
                f"This cell handles: *{section['description']}*\n",
            ],
        })

        # Add code cell
        cells.append({
            "cell_type": "code",
            "metadata": {"language": "python"},
            "source": [line if line.endswith("\n") else (line + "\n") for line in source_lines],
            "outputs": [],
            "execution_count": None,
        })

    return cells


def generate() -> int:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    generated = 0
    for py_file in SRC_ROOT.rglob("*.py"):
        rel = py_file.relative_to(SRC_ROOT)
        out_file = OUT_ROOT / rel.with_suffix(".ipynb")
        out_file.parent.mkdir(parents=True, exist_ok=True)

        code_lines = py_file.read_text(encoding="utf-8").splitlines()
        code_sections = _partition_code_into_sections(code_lines)
        notebook_cells = _create_notebook_cells(py_file, rel, code_sections)

        notebook = {
            "cells": notebook_cells,
            "metadata": {
                "language_info": {"name": "python", "version": "3.11.0"},
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3",
                },
            },
            "nbformat": 4,
            "nbformat_minor": 5,
        }

        out_file.write_text(json.dumps(notebook, indent=2), encoding="utf-8")
        generated += 1

    print(f"Generated {generated} notebook files in {OUT_ROOT}")
    return generated


if __name__ == "__main__":
    generate()

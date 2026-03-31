"""Generate .ipynb versions of Python modules under notebooks/."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / "notebooks"
OUT_ROOT = ROOT / "notebooks_ipynb"


def generate() -> int:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    generated = 0
    for py_file in SRC_ROOT.rglob("*.py"):
        rel = py_file.relative_to(SRC_ROOT)
        out_file = OUT_ROOT / rel.with_suffix(".ipynb")
        out_file.parent.mkdir(parents=True, exist_ok=True)

        code_lines = py_file.read_text(encoding="utf-8").splitlines()
        notebook = {
            "cells": [
                {
                    "cell_type": "markdown",
                    "metadata": {"language": "markdown"},
                    "source": [
                        f"# {py_file.stem}\\n",
                        f"Generated from {rel.as_posix()} for Databricks notebook import.\\n",
                    ],
                },
                {
                    "cell_type": "code",
                    "metadata": {"language": "python"},
                    "source": [line + "\\n" for line in code_lines],
                    "outputs": [],
                    "execution_count": None,
                },
            ],
            "metadata": {"language_info": {"name": "python"}},
            "nbformat": 4,
            "nbformat_minor": 5,
        }

        out_file.write_text(json.dumps(notebook, indent=2), encoding="utf-8")
        generated += 1

    print(f"Generated {generated} notebook files in {OUT_ROOT}")
    return generated


if __name__ == "__main__":
    generate()

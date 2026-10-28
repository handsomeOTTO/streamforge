# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Check repository language, source headers, metadata, and core import boundaries."""

import ast
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXCLUDED = {
    ".git",
    ".venv",
    ".superpowers",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    "build",
    "dist",
}
ALGORITHM_MODULES = {"cv2", "torch", "tensorflow", "onnxruntime", "numpy", "PIL"}
LOCALIZED_READMES = {"README.zh-CN.md"}
README_LANGUAGE_SWITCH = "**English** | [\u7b80\u4f53\u4e2d\u6587](README.zh-CN.md)"


def main():
    errors = []
    for path in ROOT.rglob("*"):
        relative = path.relative_to(ROOT)
        if not path.is_file() or any(
            part in EXCLUDED or part.endswith(".egg-info") for part in relative.parts
        ):
            continue
        if path.suffix not in {".py", ".md", ".toml", ".yml", ".yaml", ".jsonl"}:
            continue
        text = path.read_text(encoding="utf-8")
        language_text = (
            text.replace(README_LANGUAGE_SWITCH, "") if relative == Path("README.md") else text
        )
        if relative.as_posix() not in LOCALIZED_READMES and re.search(
            r"[\u3400-\u9fff]", language_text
        ):
            errors.append(f"{relative}: use English except in designated README translations")
        if path.suffix == ".py":
            if "# SPDX-License-Identifier: Apache-2.0" not in text.splitlines()[:3]:
                errors.append(f"{relative}: missing SPDX license header")
            if relative.parts[0] == "src":
                tree = ast.parse(text, filename=str(relative))
                imports = set()
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        imports.update(alias.name.split(".")[0] for alias in node.names)
                    elif isinstance(node, ast.ImportFrom) and node.module:
                        imports.add(node.module.split(".")[0])
                if imports & ALGORITHM_MODULES:
                    errors.append(f"{relative}: image/AI algorithm dependency in core")
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    if project.get("license") != "Apache-2.0":
        errors.append("Package license must be Apache-2.0")
    for name in (
        "LICENSE",
        "NOTICE",
        "CONTRIBUTING.md",
        "CODE_OF_CONDUCT.md",
        "SECURITY.md",
        "CHANGELOG.md",
    ):
        if not (ROOT / name).is_file():
            errors.append(f"Missing community file: {name}")
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("Repository policy checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

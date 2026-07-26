import ast
from pathlib import Path


def test_source_tree_has_no_stock_project_import() -> None:
    source_root = Path(__file__).resolve().parents[1] / "src"
    violations: list[str] = []
    for path in sorted(source_root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            modules: tuple[str, ...] = ()
            if isinstance(node, ast.Import):
                modules = tuple(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules = (node.module,)
            for module in modules:
                if module.startswith("us_stocks_swing_model"):
                    violations.append(f"{path.relative_to(source_root)}:{node.lineno}:{module}")
    assert violations == []

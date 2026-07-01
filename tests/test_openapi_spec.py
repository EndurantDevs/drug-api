import ast
import re
from pathlib import Path

import yaml

HTTP_METHODS = {"get", "post", "put", "delete", "patch", "options", "head"}
ENDPOINT_DIR = Path("api/endpoint")
OPENAPI_PATH = Path("openapi.yaml")


def _const_str(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _collect_code_routes() -> set[tuple[str, str]]:
    routes = set()
    for path in ENDPOINT_DIR.glob("*.py"):
        tree = ast.parse(path.read_text())
        prefixes_by_blueprint_dict = {}
        for node in tree.body:
            if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
                continue
            func = node.value.func
            if not (isinstance(func, ast.Name) and func.id == "Blueprint"):
                continue
            prefix = ""
            version = None
            for kwarg in node.value.keywords:
                if kwarg.arg == "url_prefix":
                    prefix = _const_str(kwarg.value) or ""
                if kwarg.arg == "version" and isinstance(kwarg.value, ast.Constant):
                    version = kwarg.value.value
            for target_node in node.targets:
                if isinstance(target_node, ast.Name):
                    prefixes_by_blueprint_dict[target_node.id] = (prefix, version)

        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for decorator in node.decorator_list:
                if not isinstance(decorator, ast.Call) or not isinstance(decorator.func, ast.Attribute):
                    continue
                method = decorator.func.attr.lower()
                if method not in HTTP_METHODS:
                    continue
                if not isinstance(decorator.func.value, ast.Name):
                    continue
                blueprint_name = decorator.func.value.id
                if blueprint_name not in prefixes_by_blueprint_dict:
                    continue
                route = _const_str(decorator.args[0]) if decorator.args else ""
                prefix, version = prefixes_by_blueprint_dict[blueprint_name]
                full_path = f"/api/v{version}{prefix}{route}"
                full_path = re.sub(r"/+", "/", full_path)
                full_path = re.sub(r"<([^>:]+)(?::[^>]+)?>", r"{\1}", full_path)
                if len(full_path) > 1 and full_path.endswith("/"):
                    full_path = full_path[:-1]
                routes.add((method, full_path))
    return routes


def _collect_spec_routes() -> tuple[set[tuple[str, str]], list[str]]:
    spec = yaml.safe_load(OPENAPI_PATH.read_text())
    routes = set()
    operation_ids = []
    for path, methods in (spec.get("paths") or {}).items():
        for method, payload in (methods or {}).items():
            if method not in HTTP_METHODS:
                continue
            normalized_path = path if path == "/" else path.rstrip("/")
            routes.add((method, normalized_path))
            operation_id = (payload or {}).get("operationId")
            if operation_id:
                operation_ids.append(operation_id)
    return routes, operation_ids


def test_openapi_routes_match_code():
    spec_routes, _operation_ids = _collect_spec_routes()
    code_routes = _collect_code_routes()

    assert sorted(code_routes - spec_routes) == []
    assert sorted(spec_routes - code_routes) == []


def test_openapi_operation_ids_are_present_and_unique():
    spec_routes, operation_ids = _collect_spec_routes()

    assert len(operation_ids) == len(spec_routes)
    assert len(operation_ids) == len(set(operation_ids))

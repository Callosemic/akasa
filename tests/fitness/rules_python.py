import os
import ast
from pathlib import Path
from .core import (
    PROJECT_ROOT,
    KERNEL_DIR,
    VFS_WRITE_WHITELIST,
    SQLITE_WHITELIST,
    FLASK_TRANSPORT_WHITELIST,
    KERNEL_RING_MAP,
    report_violation,
    collect_unique_files,
)
# Client-identity forwarding headers (HTTP and WSGI spellings). Only akasa/auth/peer.py may read them,
# so every trust decision about where a request came from flows through classify_peer().
FORWARDED_CLIENT_HEADERS = frozenset({
    "x-forwarded-for", "x-real-ip", "forwarded",
    "http_x_forwarded_for", "http_x_real_ip", "http_forwarded",
})


def _is_peer_classifier(filepath) -> bool:
    fp = Path(filepath)
    return fp.name == "peer.py" and fp.parent.name == "auth"


def _resolve_module_key(filepath: Path) -> str:
    try:
        rel = filepath.resolve().relative_to(KERNEL_DIR.resolve())
        parts = rel.parts
        if len(parts) > 1:
            return parts[0]
    except ValueError:
        pass
    return filepath.stem


class KernelFitnessVisitor(ast.NodeVisitor):
    def __init__(self, filepath, filename, is_test=False):
        self.filepath = filepath
        self.filename = filename
        self.is_test = is_test or "tests" in Path(filepath).parts or filename.startswith("test_")
        self.module_key = _resolve_module_key(Path(filepath)) if not self.is_test else ""
        self.my_ring = KERNEL_RING_MAP.get(self.module_key, 4)
        self.top_level_imports = set()
    def visit_Module(self, node):
        top_level_funcs = set()
        for stmt in node.body:
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if stmt.name in top_level_funcs:
                    report_violation(
                        "DUPLICATE_TOP_LEVEL_DEFINITION",
                        self.filepath,
                        stmt.lineno,
                        f"Duplicate top-level function '{stmt.name}' detected in '{self.filename}'."
                    )
                top_level_funcs.add(stmt.name)
            elif isinstance(stmt, ast.Import):
                for alias in stmt.names:
                    self.top_level_imports.add(alias.asname or alias.name)
            elif isinstance(stmt, ast.ImportFrom):
                module = stmt.module or ""
                for alias in stmt.names:
                    self.top_level_imports.add(f"{module}.{alias.name}" if module else alias.name)
                    self.top_level_imports.add(alias.asname or alias.name)
        self.generic_visit(node)
    def visit_FunctionDef(self, node):
        for child in ast.walk(node):
            if isinstance(child, (ast.Import, ast.ImportFrom)):
                if not self.is_test:
                    report_violation(
                        "STRICT_TOP_LEVEL_IMPORTS_MANDATE",
                        self.filepath,
                        child.lineno,
                        f"Inline import detected inside '{node.name}'. The Akasa Ring DAG prohibits inline imports in kernel modules; hoist all imports to the top of the file."
                    )
                elif isinstance(child, ast.Import):
                    for alias in child.names:
                        name = alias.asname or alias.name
                        if name in self.top_level_imports or alias.name in self.top_level_imports:
                            report_violation("REDUNDANT_INLINE_IMPORT", self.filepath, child.lineno, f"Redundant inline import '{alias.name}' inside '{node.name}' (already imported at module level).")
                        elif not (alias.asname and alias.asname.startswith('_')):
                            report_violation("UNALIASED_INLINE_IMPORT", self.filepath, child.lineno, f"Inline import '{alias.name}' inside '{node.name}' must be placed at the module top-level or aliased with a leading underscore.")
                elif isinstance(child, ast.ImportFrom):
                    module = child.module or ""
                    for alias in child.names:
                        full_name = f"{module}.{alias.name}" if module else alias.name
                        name = alias.asname or alias.name
                        if full_name in self.top_level_imports or name in self.top_level_imports:
                            report_violation("REDUNDANT_INLINE_IMPORT", self.filepath, child.lineno, f"Redundant inline import 'from {module} import {alias.name}' inside '{node.name}' (already imported at module level).")
                        elif not (alias.asname and alias.asname.startswith('_')):
                            report_violation("UNALIASED_INLINE_IMPORT", self.filepath, child.lineno, f"Inline import 'from {module} import {alias.name}' inside '{node.name}' must be placed at the module top-level or aliased with a leading underscore.")
        self.generic_visit(node)

    def visit_Import(self, node):
        for alias in node.names:
            if alias.name.startswith('insetu'):
                report_violation(
                    "ZERO_DOMAIN_LEAKAGE",
                    self.filepath,
                    node.lineno,
                    f"Tier 1 kernel cannot import from host application '{alias.name}'."
                )
            if self.filename not in SQLITE_WHITELIST and alias.name == 'sqlite3':
                report_violation(
                    "THREAD_LOCAL_POOLING_MANDATE",
                    self.filepath,
                    node.lineno,
                    "Direct sqlite3 import banned outside db.py. Use get_connection()."
                )
            if not self.is_test:
                if alias.name == 'flask' or alias.name.startswith('flask.'):
                    if self.my_ring < 4 and self.filename not in FLASK_TRANSPORT_WHITELIST:
                        report_violation(
                            "KERNEL_TRANSPORT_ISOLATION",
                            self.filepath,
                            node.lineno,
                            f"Ring {self.my_ring} module '{self.module_key}' cannot import from transport framework '{alias.name}'."
                        )
                if alias.name.startswith('akasa.'):
                    target_mod = alias.name.split('.')[1]
                    target_ring = KERNEL_RING_MAP.get(target_mod)
                    if target_ring is not None and target_ring > self.my_ring:
                        report_violation(
                            "KERNEL_RING_DAG_MANDATE",
                            self.filepath,
                            node.lineno,
                            f"Ring {self.my_ring} module '{self.module_key}' cannot import upward from Ring {target_ring} module 'akasa.{target_mod}'."
                        )
        self.generic_visit(node)
    def visit_ImportFrom(self, node):
        if node.module and node.module.startswith('insetu'):
            report_violation(
                "ZERO_DOMAIN_LEAKAGE",
                self.filepath,
                node.lineno,
                f"Tier 1 kernel cannot import from host application '{node.module}'."
            )
        if not self.is_test and node.module:
            if node.module == 'flask' or node.module.startswith('flask.'):
                if self.my_ring < 4 and self.filename not in FLASK_TRANSPORT_WHITELIST:
                    report_violation(
                        "KERNEL_TRANSPORT_ISOLATION",
                        self.filepath,
                        node.lineno,
                        f"Ring {self.my_ring} module '{self.module_key}' cannot import from transport framework '{node.module}'."
                    )
            if node.module.startswith('akasa.'):
                target_mod = node.module.split('.')[1]
                target_ring = KERNEL_RING_MAP.get(target_mod)
                if target_ring is not None and target_ring > self.my_ring:
                    report_violation(
                        "KERNEL_RING_DAG_MANDATE",
                        self.filepath,
                        node.lineno,
                        f"Ring {self.my_ring} module '{self.module_key}' cannot import upward from Ring {target_ring} module 'akasa.{target_mod}'."
                    )
        self.generic_visit(node)
    def visit_Constant(self, node):
        if isinstance(node.value, str):
            val = node.value.lower()
            if not self.is_test and val.strip() in FORWARDED_CLIENT_HEADERS and not _is_peer_classifier(self.filepath):
                report_violation(
                    "FORWARDED_HEADER_MEMBRANE",
                    self.filepath,
                    node.lineno,
                    f"Forwarding header '{node.value}' read outside akasa/auth/peer.py. Use classify_peer() so proxy trust stays fail-closed and rightmost-hop."
                )
            if ".insetu" in val or "insetu_" in val:
                report_violation(
                    "DYNAMIC_SPATIAL_PHYSICS", 
                    self.filepath, 
                    node.lineno, 
                    f"Hardcoded host token '{node.value}' detected. Use AKASA_CONTROL_DIR environments."
                )
            # Substrate Purity Check: Detect domain-specific leakage
            banned_domain_tokens = [
                "tailscale", ".ts.net", "_get_base_step_and_diffs", "resolve_macro_includes",
                "request_manifest", "request_vfs_manifest", "request_manifest_chunks",
                "save_manifest", "insetu", "yomama"
            ]
            for token in banned_domain_tokens:
                if token in val:
                    report_violation(
                        "SUBSTRATE_PURITY_VIOLATION",
                        self.filepath,
                        node.lineno,
                        f"Domain-specific token '{node.value}' detected in Tier 1 kernel."
                    )

            if self.filename != "utils.py" and ("_mutated_config_cache" in val or "_mutated_config_mtime" in val):
                report_violation(
                    "DEPRECATED_CACHE_MUTATION",
                    self.filepath,
                    node.lineno,
                    "Direct config cache dictionary mutation is deprecated. Emit 'config_mutated' event instead."
                )
        self.generic_visit(node)

    def visit_Assign(self, node):
        # Physics Signature Guard: Enforce 2-tuple unpacking for get_workspace_physics
        if isinstance(node.value, ast.Call):
            func_name = None
            if isinstance(node.value.func, ast.Name):
                func_name = node.value.func.id
            elif isinstance(node.value.func, ast.Attribute):
                func_name = node.value.func.attr

            if func_name == 'get_workspace_physics':
                if len(node.targets) == 1 and isinstance(node.targets[0], (ast.Tuple, ast.List)):
                    target_len = len(node.targets[0].elts)
                    if target_len != 2:
                        report_violation(
                            "PHYSICS_SIGNATURE_MISMATCH",
                            self.filepath,
                            node.lineno,
                            f"get_workspace_physics() must be unpacked into exactly 2 targets (got {target_len})."
                        )
        self.generic_visit(node)
    def visit_Call(self, node):
        if self.is_test:
            self.generic_visit(node)
            return

        if isinstance(node.func, ast.Name):
            if node.func.id == 'open' and self.filename not in VFS_WRITE_WHITELIST:
                if len(node.args) >= 2 and isinstance(node.args[1], ast.Constant) and any(m in node.args[1].value for m in ['w', 'a', 'x', '+']):
                    report_violation(
                        "ASYNC_VFS_PIPELINE_COMPLIANCE", 
                        self.filepath, 
                        node.lineno, 
                        "Native file write detected outside vfs.py. Route through VFS async queue."
                    )
                for kw in node.keywords:
                    if kw.arg == 'mode' and isinstance(kw.value, ast.Constant) and any(m in kw.value.value for m in ['w', 'a', 'x', '+']):
                        report_violation(
                            "ASYNC_VFS_PIPELINE_COMPLIANCE", 
                            self.filepath, 
                            node.lineno, 
                            "Native file write detected outside vfs.py. Route through VFS async queue."
                        )
        if isinstance(node.func, ast.Attribute):
            if node.func.attr == 'startswith' and self.filename not in ('rules_python.py', 'hooks.py'):
                is_scheme_check = (
                    len(node.args) > 0
                    and isinstance(node.args[0], ast.Constant)
                    and isinstance(node.args[0].value, str)
                    and "://" in node.args[0].value
                )
                # Flag string startswith on path-like variables in kernel modules
                if not is_scheme_check and isinstance(node.func.value, (ast.Call, ast.Name)):
                    val_repr = ast.unparse(node.func.value).lower()
                    if any(term in val_repr for term in ('path', 'resolved', 'dir', 'root', 'target')):
                        report_violation(
                            "SUBPATH_SAFETY_MANDATE",
                            self.filepath,
                            node.lineno,
                            f"Path prefix comparison '{val_repr}.startswith()' detected. Use is_subpath() to prevent traversal collisions."
                        )
            if node.func.attr in ('unlink', 'write_text', 'write_bytes') and self.filename not in VFS_WRITE_WHITELIST:
                report_violation(
                    "ASYNC_VFS_PIPELINE_COMPLIANCE", 
                    self.filepath, 
                    node.lineno, 
                    f"Synchronous mutation '{node.func.attr}()' detected outside vfs.py."
                )
            if (
                node.func.attr in ('remove', 'rmdir')
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == 'os'
                and self.filename not in VFS_WRITE_WHITELIST
            ):
                report_violation(
                    "ASYNC_VFS_PIPELINE_COMPLIANCE", 
                    self.filepath, 
                    node.lineno, 
                    f"Synchronous mutation 'os.{node.func.attr}()' detected outside vfs.py."
                )
            
            # AkasaURI parsing enforcement
            if node.func.attr == 'split' and self.filename != 'uri.py':
                if len(node.args) > 0 and isinstance(node.args[0], ast.Constant) and node.args[0].value == '/':
                    report_violation(
                        "ZERO_KNOWLEDGE_URI_ROUTING", 
                        self.filepath, 
                        node.lineno, 
                        "String math .split('/') detected. Cast to AkasaURI and use .basename or .volume instead."
                    )

        self.generic_visit(node)

    def visit_Subscript(self, node):
        if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute) and node.value.func.attr == 'split':
            if len(node.value.args) > 0 and isinstance(node.value.args[0], ast.Constant) and node.value.args[0].value == '/':
                report_violation(
                    "ZERO_KNOWLEDGE_URI_ROUTING", 
                    self.filepath, 
                    node.lineno, 
                    "Path slicing logic detected. Use AkasaURI primitives instead."
                )
        self.generic_visit(node)
def check_python_files():
    print("🔍 Sweeping Akasa Kernel (AST Analysis)...")
    for filepath in collect_unique_files([KERNEL_DIR], [".py"]):
        with open(filepath, "r", encoding="utf-8") as f:
            try:
                tree = ast.parse(f.read(), filename=str(filepath))
                visitor = KernelFitnessVisitor(filepath, filepath.name)
                visitor.visit(tree)
            except SyntaxError:
                print(f"⚠️  Skipping {filepath.name} due to SyntaxError.")

    unit_tests_dir = PROJECT_ROOT / "tests" / "unit"
    if unit_tests_dir.exists():
        for test_fp in sorted(unit_tests_dir.glob("test_*.py")):
            try:
                t_tree = ast.parse(test_fp.read_text(encoding="utf-8"), filename=str(test_fp))
                KernelFitnessVisitor(test_fp, test_fp.name, is_test=True).visit(t_tree)
                has_testcase_class = False
                for stmt in t_tree.body:
                    if isinstance(stmt, ast.FunctionDef) and stmt.name.startswith("test_"):
                        report_violation(
                            "UNITTEST_DISCOVERY_MANDATE",
                            test_fp,
                            stmt.lineno,
                            f"Top-level function '{stmt.name}()' is ignored by unittest.TestLoader().discover(). Place all test methods inside a unittest.TestCase subclass."
                        )
                    elif isinstance(stmt, ast.ClassDef):
                        has_testcase_class = True
                if not has_testcase_class:
                    report_violation(
                        "UNITTEST_DISCOVERY_MANDATE",
                        test_fp,
                        1,
                        f"Test module '{test_fp.name}' contains no unittest.TestCase classes."
                    )
            except SyntaxError as e:
                report_violation("UNITTEST_SYNTAX_ERROR", test_fp, e.lineno or 1, f"Syntax error in unit test file: {e}")
    code_index_path = PROJECT_ROOT / "CODE_INDEX.md"
    if code_index_path.exists():
        for idx, line in enumerate(code_index_path.read_text(encoding="utf-8").splitlines(), start=1):
            if "[comment required]" in line:
                tree_entry = line.split("#", 1)[0].lstrip(" │├└─\t").strip()
                if tree_entry.startswith(("def ", "async def ", "class ", "function ")):
                    continue
                report_violation(
                    "CARTOGRAPHER_PLACEHOLDER_BAN",
                    code_index_path,
                    idx,
                    f"Unresolved file/directory '# [comment required]' placeholder in CODE_INDEX.md: {line.strip()}"
                )
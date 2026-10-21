import os
import ast
from pathlib import Path
from .core import KERNEL_DIR, VFS_WRITE_WHITELIST, SQLITE_WHITELIST, report_violation, collect_unique_files

class KernelFitnessVisitor(ast.NodeVisitor):
    def __init__(self, filepath, filename):
        self.filepath = filepath
        self.filename = filename

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
        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        if node.module and node.module.startswith('insetu'):
            report_violation(
                "ZERO_DOMAIN_LEAKAGE", 
                self.filepath, 
                node.lineno, 
                f"Tier 1 kernel cannot import from host application '{node.module}'."
            )
        self.generic_visit(node)
    def visit_Constant(self, node):
        if isinstance(node.value, str):
            val = node.value.lower()
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

            if node.func.attr in ('remove', 'rmdir', 'unlink', 'write_text', 'write_bytes') and self.filename not in VFS_WRITE_WHITELIST:
                report_violation(
                    "ASYNC_VFS_PIPELINE_COMPLIANCE", 
                    self.filepath, 
                    node.lineno, 
                    f"Synchronous mutation '{node.func.attr}()' detected outside vfs.py."
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
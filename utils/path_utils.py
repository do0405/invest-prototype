import os
import sys
from typing import Optional

def add_project_root(subdir: Optional[str] = None) -> str:
    """Add project root or a subdirectory to ``sys.path`` if not present."""
    project_root = os.path.dirname(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    target_path = os.path.join(project_root, subdir) if subdir else project_root
    if target_path not in sys.path:
        sys.path.insert(0, target_path)
    return target_path

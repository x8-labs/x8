"""
Local Object Store on File System.
"""

__all__ = ["Local"]

from .file_system import FileSystem


class Local(FileSystem):
    pass

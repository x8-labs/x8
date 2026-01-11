import os

from x8.core import Loader
from x8.core.manifest import MANIFEST_FILE


def create_requirements_file(
    handle: str,
    tag: str | None,
    filename: str = "requirements.txt",
    path: str = ".",
    manifest: str = MANIFEST_FILE,
):
    """Create requirements file.

    Args:
        handle: Component handle.
        tag: Provider tag.
        filename: Requirements file name.
        path: Path to save requirements file.
        manifest: Manifest file name.
    """
    requirements_file = os.path.join(path, filename)
    loader = Loader(path=path, manifest=manifest)
    loader.generate_requirements(
        handle=handle,
        tag=tag,
        out=requirements_file,
    )

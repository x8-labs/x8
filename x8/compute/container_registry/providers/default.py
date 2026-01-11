"""
Local provider for container registry.
"""

__all__ = ["Default"]


from .docker_local import DockerLocal


class Default(DockerLocal):
    pass

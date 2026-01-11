"""
Local provider for container registry.
"""

__all__ = ["Local"]


from .docker_local import DockerLocal


class Local(DockerLocal):
    pass

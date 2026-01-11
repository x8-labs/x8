"""
Local container deployment.
"""

__all__ = ["Local"]


from .docker_local import DockerLocal


class Local(DockerLocal):
    pass

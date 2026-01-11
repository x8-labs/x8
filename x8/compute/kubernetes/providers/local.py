"""
Local kubernetes deployment.
"""

__all__ = ["Local"]


from .minikube import MinikubeKubernetes


class Local(MinikubeKubernetes):
    pass

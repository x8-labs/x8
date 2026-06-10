from x8.core.spec import SpecBuilder


def test_build_component_spec_without_providers_directory(tmp_path):
    package_dir = tmp_path / "demo" / "no_provider_component"
    package_dir.mkdir(parents=True)

    (tmp_path / "demo" / "__init__.py").write_text("")
    (package_dir / "__init__.py").write_text("")
    (package_dir / "component.py").write_text("""
from x8.core._component import Component


class InlineComponent(Component):
    \"\"\"Inline component with no providers package.\"\"\"

    def ping(self) -> str:
        return \"pong\"
""".strip())

    spec = SpecBuilder(path=str(tmp_path)).build_component_spec(
        "demo.no_provider_component"
    )

    assert spec.type == "demo.no_provider_component"
    assert spec.providers == []

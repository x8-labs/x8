import time
import uuid

from x8.core._loader import Loader


def test_resolve_func_time_unix_timestamp(tmp_path):
    manifest_path = tmp_path / "x8.yaml"
    manifest_path.write_text(
        """
metadata: {}
variables:
  now: "${func.time}"
components: {}
bindings: {}
requirements: []
""".strip()
    )

    loader = Loader(path=str(tmp_path))

    direct_value = loader._resolve_param("${func.time}", None)
    variable_value = loader._resolve_param("${variables.now}", None)

    assert isinstance(direct_value, int)
    assert isinstance(variable_value, int)
    assert direct_value == variable_value

    now = int(time.time())
    assert now - 2 <= direct_value <= now + 2


def test_resolve_func_uuid_guid_string(tmp_path):
    manifest_path = tmp_path / "x8.yaml"
    manifest_path.write_text(
        """
metadata: {}
variables:
  id: "${func.uuid}"
components: {}
bindings: {}
requirements: []
""".strip()
    )

    loader = Loader(path=str(tmp_path))

    direct_value = loader._resolve_param("${func.uuid}", None)
    variable_value = loader._resolve_param("${variables.id}", None)

    assert isinstance(direct_value, str)
    assert isinstance(variable_value, str)
    assert direct_value != variable_value

    assert str(uuid.UUID(direct_value)) == direct_value
    assert str(uuid.UUID(variable_value)) == variable_value

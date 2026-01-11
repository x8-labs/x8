import inspect
import json
from typing import Any, get_args, get_origin, get_type_hints


class TypeConverter:
    @staticmethod
    def convert_value(value, expected_type):
        origin = get_origin(expected_type)

        # Handle Optional[T] (e.g., DataModel | None)
        if origin is not None and type(None) in get_args(expected_type):
            expected_type = next(
                t for t in get_args(expected_type) if t is not type(None)
            )
            origin = get_origin(expected_type)

        # Convert lists recursively
        if isinstance(value, list) and origin in (list, tuple):
            elem_type = (
                get_args(expected_type)[0] if get_args(expected_type) else Any
            )
            return [TypeConverter.convert_value(v, elem_type) for v in value]

        # Convert dictionaries recursively
        if isinstance(value, dict) and origin is dict:
            key_type, val_type = (
                get_args(expected_type)
                if get_args(expected_type)
                else (Any, Any)
            )
            return {
                TypeConverter.convert_value(
                    k, key_type
                ): TypeConverter.convert_value(v, val_type)
                for k, v in value.items()
            }

        # Convert dictionary to DataModel
        if (
            isinstance(value, dict)
            and hasattr(expected_type, "from_dict")
            and callable(getattr(expected_type, "from_dict"))
        ):
            return expected_type.from_dict(value)

        if (
            isinstance(value, str)
            and hasattr(expected_type, "from_dict")
            and callable(getattr(expected_type, "from_dict"))
        ):
            return expected_type.from_dict(json.loads(value))

        # Primitive type conversions
        try:
            if expected_type is int and isinstance(value, (str, float)):
                return int(value)
            if expected_type is float and isinstance(value, (str, int)):
                return float(value)
            if expected_type is str and isinstance(value, (int, float, bytes)):
                return str(value)
            if expected_type is bytes and isinstance(value, str):
                return value.encode()
            if expected_type is bool and isinstance(value, (str, int)):
                return (
                    bool(int(value))
                    if isinstance(value, str) and value.isdigit()
                    else bool(value)
                )
            if expected_type is list and not isinstance(value, list):
                return list(value)
            if expected_type is dict and isinstance(value, str):
                return json.loads(value)
            if expected_type is dict and isinstance(value, bytes):
                return json.loads(value.decode())
        except (ValueError, TypeError):
            pass  # If conversion fails, fallback to returning value as is.

        return value

    @staticmethod
    def convert_args(method, args: dict) -> dict:
        sig = inspect.signature(method)
        converted_args: dict = {}

        hints = get_type_hints(method)
        for param_name, _ in sig.parameters.items():
            param_type = hints.get(param_name, None)
            if param_name in args:
                converted_args[param_name] = TypeConverter.convert_value(
                    args[param_name], param_type
                )

        return args | converted_args

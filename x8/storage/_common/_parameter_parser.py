from typing import Any


class ParameterParser:
    @staticmethod
    def get_collection_parameter(
        parameter: Any,
        collection: str | None,
        is_parameter_dict: bool = False,
    ) -> Any:
        return (
            parameter[collection]
            if isinstance(parameter, dict)
            and collection in parameter
            and (
                not is_parameter_dict
                or all(
                    isinstance(v, dict) for v in parameter[collection].values()
                )
            )
            else parameter
        )

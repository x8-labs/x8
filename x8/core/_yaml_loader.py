import yaml


class YamlLoader:
    @staticmethod
    def load(path: str) -> dict:
        with open(path, "r") as file:
            return yaml.load(file, Loader=yaml.FullLoader)

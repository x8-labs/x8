import json


class Comparator:
    @staticmethod
    def contains(item1: dict, item2: dict) -> bool:
        for key, value in item1.items():
            if key not in item2:
                return False
            if isinstance(value, dict):
                if not Comparator.contains(value, item2[key]):
                    return False
            elif isinstance(value, list):
                if not isinstance(item2[key], list):
                    return False
                if len(value) != len(item2[key]):
                    return False
                for i1, i2 in zip(value, item2[key]):
                    if isinstance(i1, dict) and isinstance(i2, dict):
                        if not Comparator.contains(i1, i2):
                            return False
                    elif i1 != i2:
                        return False
            else:
                if value != item2[key]:
                    return False
        return True

    @staticmethod
    def equals(item1: dict, item2: dict) -> bool:
        str1 = json.dumps(item1, sort_keys=True)
        str2 = json.dumps(item2, sort_keys=True)
        return str1 == str2

from typing import Any

from x8.ql._models import Field, Undefined, UpdateOp, Value

from .data_model import DataModel
from .exceptions import BadRequestError


class DataAccessor:
    @staticmethod
    def get_field(
        item: dict | DataModel | None, field: str
    ) -> Value | Undefined:
        if item is None:
            return Undefined()
        npath = (
            field.replace("[", "/")
            .replace("]", "")
            .replace(".", "/")
            .rstrip("/")
        )
        splits = npath.split("/")
        current_item = item
        for split in splits:
            if split.isnumeric() and isinstance(current_item, list):
                index = int(split)
                if index < len(current_item):
                    current_item = current_item[index]
                else:
                    return Undefined()
            elif split == "-":
                if isinstance(current_item, list):
                    if len(current_item) > 0:
                        current_item = current_item[-1]
                    else:
                        return Undefined()
                else:
                    return Undefined()
            else:
                if isinstance(current_item, DataModel):
                    if hasattr(current_item, split):
                        current_item = getattr(current_item, split)
                    else:
                        return Undefined()
                else:
                    if not isinstance(current_item, dict):
                        return Undefined()
                    if split in current_item:
                        current_item = current_item[split]
                    else:
                        return Undefined()
        return current_item

    @staticmethod
    def update_field(item: dict, field: str, op: UpdateOp, value: Any):
        npath = (
            field.replace("[", "/")
            .replace("]", "")
            .replace(".", "/")
            .rstrip("/")
        )
        splits = npath.split("/")
        current_item = item
        last_split = False
        len_splits = len(splits)
        for i in range(len_splits):
            split = splits[i]
            last_split = i == len_splits - 1
            if split.isnumeric() and isinstance(current_item, list):
                index = int(split)
                if not last_split:
                    if index < len(current_item):
                        current_item = current_item[index]
                    else:
                        raise BadRequestError(
                            f"Index {index} more than length of the array"
                        )
                else:
                    if op == UpdateOp.PUT:
                        if index >= 0 and index < len(current_item):
                            current_item[index] = value
                        else:
                            raise BadRequestError(
                                f"Index {index} is out of range"
                            )
                    elif op == UpdateOp.INSERT:
                        if index >= 0 and index <= len(current_item):
                            current_item.insert(index, value)
                        else:
                            raise BadRequestError(
                                f"Index {index} is out of range"
                            )
                    elif op == UpdateOp.DELETE:
                        if index >= 0 and index < len(current_item):
                            current_item.pop(index)
                        else:
                            raise BadRequestError(
                                f"Index {index} is out of range"
                            )
                    elif op == UpdateOp.INCREMENT:
                        if index >= 0 and index < len(current_item):
                            if isinstance(current_item[index], (int, float)):
                                current_item[index] += value
                            else:
                                raise BadRequestError(
                                    "Increment field should be a number"
                                )
                        else:
                            raise BadRequestError(
                                f"Index {index} is out of range"
                            )
                    else:
                        raise BadRequestError(
                            "Operation not supported on arrays"
                        )
            elif split == "-":
                if isinstance(current_item, list):
                    if not last_split:
                        raise BadRequestError(
                            "- can be used only as a suffix in field path"
                        )
                    else:
                        if op == UpdateOp.PUT:
                            if len(current_item) > 0:
                                current_item[-1] = value
                            else:
                                raise BadRequestError(
                                    "Array is empty for SET operation"
                                )
                        elif op == UpdateOp.INSERT:
                            current_item.append(value)
                        elif op == UpdateOp.DELETE:
                            if len(current_item) > 0:
                                current_item.pop(-1)
                            else:
                                raise BadRequestError(
                                    "Array is empty for DELETE operation"
                                )
                        else:
                            raise BadRequestError(
                                "Operation not supported on arrays"
                            )
            else:
                if not last_split:
                    if split not in current_item:
                        if isinstance(current_item, DataModel):
                            setattr(current_item, split, dict())
                        else:
                            current_item[split] = dict()
                    if isinstance(current_item, DataModel):
                        current_item = getattr(current_item, split)
                    else:
                        current_item = current_item[split]
                else:
                    if op == UpdateOp.PUT:
                        if isinstance(current_item, DataModel):
                            setattr(current_item, split, value)
                        else:
                            current_item[split] = value
                    elif op == UpdateOp.INSERT:
                        if isinstance(current_item, DataModel):
                            setattr(current_item, split, value)
                        else:
                            current_item[split] = value
                    elif op == UpdateOp.DELETE:
                        if isinstance(current_item, DataModel):
                            delattr(current_item, split)
                        else:
                            if split in current_item:
                                current_item.pop(split)
                    elif op == UpdateOp.INCREMENT:
                        if isinstance(current_item, DataModel):
                            setattr(
                                current_item,
                                split,
                                getattr(current_item, split) + value,
                            )
                        else:
                            if isinstance(current_item[split], (int, float)):
                                current_item[split] += value
                            else:
                                raise BadRequestError(
                                    "Increment field should be a number"
                                )
                    elif op == UpdateOp.ARRAY_UNION:
                        if isinstance(current_item, DataModel):
                            lst = getattr(current_item, split)
                            if isinstance(lst, list):
                                for arg in value:
                                    if arg not in lst:
                                        lst.append(arg)
                            else:
                                raise BadRequestError(
                                    f"Field {split} must be an array"
                                )
                        else:
                            if isinstance(current_item[split], list):
                                for arg in value:
                                    if arg not in current_item[split]:
                                        current_item[split].append(arg)
                            else:
                                raise BadRequestError(
                                    f"Field {split} must be an array"
                                )
                    elif op == UpdateOp.ARRAY_REMOVE:
                        if isinstance(current_item, DataModel):
                            lst = getattr(current_item, split)
                            if isinstance(lst, list):
                                for arg in value:
                                    if arg in lst:
                                        lst.remove(arg)
                            else:
                                raise BadRequestError(
                                    f"Field {split} must be an array"
                                )
                        else:
                            if isinstance(current_item[split], list):
                                for arg in value:
                                    if arg in current_item[split]:
                                        current_item[split].remove(arg)
                            else:
                                raise BadRequestError(
                                    f"Field {split} must be an array"
                                )
                    elif op == UpdateOp.MOVE:
                        present = False
                        if isinstance(current_item, DataModel):
                            present = hasattr(current_item, split)
                        else:
                            present = split in current_item
                        if present:
                            DataAccessor.update_field(
                                item,
                                (
                                    value.path
                                    if isinstance(value, Field)
                                    else value
                                ),
                                UpdateOp.PUT,
                                current_item[split],
                            )
                            if isinstance(current_item, DataModel):
                                delattr(current_item, split)
                            else:
                                current_item.pop(split)
                        else:
                            BadRequestError(f"Field {split} not found")

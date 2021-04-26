from typing import TypeVar, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from .avrojson import AvroJsonConverter

TC = TypeVar("TC", bound="DictWrapper")


class DictWrapper:
    __slots__ = ["_inner_dict"]
    _inner_dict: dict

    def __init__(self):
        self._inner_dict = {}

    @classmethod
    def construct(cls: Type[TC], inner_dict: dict) -> TC:
        """
        Construct an object without any validations or type annotation checks.
        You should not be using this under normal circumstances.
        """
        obj = cls.__new__(cls)
        obj._inner_dict = {}
        obj._restore_defaults()
        for key, value in inner_dict.items():
            # The call to _restore_defaults() populates the dict with the full set of keys.
            if key not in obj._inner_dict:
                raise ValueError(f"{cls.__name__} does not support field {key}")
            if value is not None:
                obj._inner_dict[key] = value
        return obj

    @classmethod
    def _get_json_converter(cls) -> "AvroJsonConverter":
        # This attribute will be set by the AvroJsonConverter's init method.
        return cls._json_converter

    @classmethod
    def from_obj(cls: Type[TC], obj: dict, tuples: bool = False) -> TC:
        conv = cls._get_json_converter().with_tuple_union(tuples)
        return conv.from_json_object(obj, cls.RECORD_SCHEMA)

    def to_obj(self, tuples: bool = False) -> dict:
        conv = self._get_json_converter().with_tuple_union(tuples)
        return conv.to_json_object(self, self.RECORD_SCHEMA)

    def to_avro_writable(self, fastavro: bool = False) -> dict:
        return self.to_obj(tuples=fastavro)

    def _restore_defaults(self) -> None:
        pass

    def validate(self) -> bool:
        """
        Checks the current object against its pre-defined schema. This does
        not ensure that an object is completely valid (e.g. we don't check that
        the URNs are formatted corrected and point to valid objects), but only
        checks it against the Avro schema. Returns True if valid.
        """
        conv = self._get_json_converter()
        return conv.validate(self.RECORD_SCHEMA, self)

    def get(self, item, default=None):
        return self._inner_dict.get(item, default)

    def items(self):
        return self._inner_dict.items()

    def keys(self):
        return self._inner_dict.keys()

    def __str__(self):
        return f"{self.__class__.__name__}({self._inner_dict.__str__()})"

    def __repr__(self):
        return f"{self.__class__.__name__}({self._inner_dict.__repr__()})"

    def __eq__(self, other):
        if isinstance(other, DictWrapper):
            return self._inner_dict.__eq__(other._inner_dict)
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

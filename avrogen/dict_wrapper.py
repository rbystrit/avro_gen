from typing import NoReturn, TypeVar, Type
import six

from .avrojson import AvroJsonConverter

TC = TypeVar('TC', bound='DictWrapper')


class DictWrapper(dict):
    __slots__ = ['_inner_dict']

    def __init__(self, inner_dict=None):
        super(DictWrapper, self).__init__()
        self._inner_dict = {} if inner_dict is None else inner_dict  # type: dict
    
    @classmethod
    def _get_json_converter(cls) -> AvroJsonConverter:
        # This attribute will be set by the AvroJsonConverter's init method.
        return cls._json_converter

    @classmethod
    def from_obj(cls: Type[TC], obj, tuples=False) -> TC:
        conv = cls._get_json_converter().with_tuple_union(tuples)
        return conv.from_json_object(obj, cls.RECORD_SCHEMA)

    def to_obj(self, tuples=False) -> dict:
        conv = self._get_json_converter().with_tuple_union(tuples)
        return conv.to_json_object(self, self.RECORD_SCHEMA)
    
    def validate(self) -> bool:
        """
        Checks the current object against its pre-defined schema. This does
        not ensure that an object is completely valid (e.g. we don't check that
        the URNs are formatted corrected and point to valid objects), but only
        checks it against the Avro schema. Returns True if valid.
        """
        conv = self._get_json_converter()
        return conv.validate(self.RECORD_SCHEMA, self)

    def __getitem__(self, item):
        return self._inner_dict.__getitem__(item)

    def __iter__(self):
        return self._inner_dict.__iter__()

    def __len__(self):
        return self._inner_dict.__len__()

    def __setitem__(self, key, value) -> NoReturn:
        raise NotImplementedError()

    def items(self):
        return self._inner_dict.items()

    def keys(self):
        return self._inner_dict.keys()

    def values(self):
        return self._inner_dict.values()

    def fromkeys(self, v=None) -> NoReturn:
        raise NotImplementedError

    def clear(self) -> NoReturn:
        raise NotImplementedError

    def copy(self):
        return DictWrapper(self._inner_dict.copy())

    def get(self, k, d=None):
        return self._inner_dict.get(k, d)

    def __contains__(self, item):
        return self._inner_dict.__contains__(item)

    def __str__(self):
        return self._inner_dict.__str__()

    def __repr__(self):
        return self._inner_dict.__repr__()

    def __sizeof__(self):
        return self._inner_dict.__sizeof__()

    def pop(self, k, d=None) -> NoReturn:
        raise NotImplementedError()

    def popitem(self) -> NoReturn:
        raise NotImplementedError()

    def update(self, E=None, **F) -> NoReturn:
        raise NotImplementedError()

    def setdefault(self, k, d=None) -> NoReturn:
        raise NotImplementedError()

    def __eq__(self, other):
        return self._inner_dict.__eq__(other)

    def __ne__(self, other):
        return self._inner_dict.__ne__(other)

    def __le__(self, other):
        return self._inner_dict.__le__(other)

    def __ge__(self, other):
        return self._inner_dict.__ge__(other)

    def __lt__(self, other):
        return self._inner_dict.__lt__(other)

    def __gt__(self, other):
        return self._inner_dict.__gt__(other)

    def __hash__(self):
        return self._inner_dict.__hash__()
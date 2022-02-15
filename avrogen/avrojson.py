from avrogen.core_writer import find_type_of_default
import collections
import six

from . import logical
from .dict_wrapper import DictWrapper
from avro import schema
from avro import io

io_validate = io.validate

try:
    from avro.io import SchemaResolutionException, AvroTypeException
except ImportError:
    from avro.errors import SchemaResolutionException, AvroTypeException

_PRIMITIVE_TYPES = set(schema.PRIMITIVE_TYPES)


class AvroJsonConverter(object):
    def __init__(self, use_logical_types=False, logical_types=logical.DEFAULT_LOGICAL_TYPES, schema_types=None):
        self.use_logical_types = use_logical_types
        self.logical_types = logical_types or {}
        self.schema_types = schema_types or {}
        self.fastavro = False
        
        # Register self with all the schema objects.
        for klass in self.schema_types.values():
            klass._json_converter = self
    
    def with_tuple_union(self, enable=True) -> 'AvroJsonConverter':
        ret = AvroJsonConverter(self.use_logical_types, self.logical_types, self.schema_types)
        ret.fastavro = enable
        return ret

    def validate(self, expected_schema, datum, skip_logical_types=False):
        if self.use_logical_types and expected_schema.props.get('logicalType') and not skip_logical_types \
                and expected_schema.props.get('logicalType') in self.logical_types:
            return self.logical_types[expected_schema.props.get('logicalType')].can_convert(expected_schema) \
                   and self.logical_types[expected_schema.props.get('logicalType')].validate(expected_schema, datum)
        schema_type = expected_schema.type
        if schema_type == 'array':
            return (isinstance(datum, list) and
                    False not in [self.validate(expected_schema.items, d, skip_logical_types) for d in datum])
        elif schema_type == 'map':
            return (isinstance(datum, dict) and
                    False not in [isinstance(k, six.string_types) for k in datum.keys()] and
                    False not in
                    [self.validate(expected_schema.values, v, skip_logical_types) for v in datum.values()])
        elif schema_type in ['union', 'error_union']:
            if isinstance(datum, DictWrapper):
                # Match the type based on the declared schema.
                data_schema = self._get_record_schema_if_available(datum)
                for i, candidate_schema in enumerate(expected_schema.schemas):
                    if candidate_schema.fullname == data_schema.fullname:
                        return self.validate(candidate_schema, datum)

            # If the union type is using a "name" to distinguish the type, we
            # must handle this specially during validation.
            value_type = None
            if not self.fastavro and isinstance(datum, dict):
                if len(datum) == 1:
                    items = list(six.iteritems(datum))
                    if not items:
                        return None
                    value_type = items[0][0]
                    value = items[0][1]
            elif self.fastavro and (isinstance(datum, list) or isinstance(datum, tuple)):
                if len(datum) == 2:
                    value_type = datum[0]
                    value = datum[1]
            if value_type is not None:
                for s in expected_schema.schemas:
                    name = self._fullname(s)
                    if name == value_type:
                        if self.validate(s, value, skip_logical_types):
                            return True
                        # If the specialized validation fails, we still attempt normal validation.

            return True in [self.validate(s, datum, skip_logical_types) for s in expected_schema.schemas]
        elif schema_type in ['record', 'error', 'request']:
            return ((isinstance(datum, dict) or isinstance(datum, DictWrapper)) and
                    False not in
                    [self.validate(f.type, datum.get(f.name), skip_logical_types) for f in expected_schema.fields])
        elif not self.fastavro and schema_type == 'bytes':
            # Specialization for bytes, which are encoded as strings in JSON.
            if isinstance(datum, str):
                return True

        return io_validate(expected_schema, datum)

    def from_json_object(self, json_obj, writers_schema=None, readers_schema=None):
        if readers_schema is None:
            readers_schema = writers_schema
        if writers_schema is None:
            writers_schema = readers_schema

        if writers_schema is None:
            raise Exception('At least one schema must be specified')

        if not writers_schema.match(readers_schema):
            raise SchemaResolutionException('Could not match schemas', writers_schema, readers_schema)

        return self._generic_from_json(json_obj, writers_schema, readers_schema)

    def to_json_object(self, data_obj, writers_schema=None):
        if writers_schema is None:
            writers_schema = self._get_record_schema_if_available(data_obj)

        if writers_schema is None:
            raise Exception("Could not determine writer's schema from the object type and schema was not passed")
        assert isinstance(writers_schema, schema.Schema)

        if not self.validate(writers_schema, data_obj):
            raise AvroTypeException(writers_schema, data_obj)

        return self._generic_to_json(data_obj, writers_schema)

    def _fullname(self, schema_):
        if isinstance(schema_, schema.NamedSchema):
            return schema_.fullname.lstrip('.')
        return schema_.type

    def _get_record_schema_if_available(self, data_obj):
        if hasattr(type(data_obj), 'RECORD_SCHEMA'):
            return type(data_obj).RECORD_SCHEMA
        return None

    def _generic_to_json(self, data_obj, writers_schema, was_within_array=False):
        if self.use_logical_types and writers_schema.props.get('logicalType'):
            lt = self.logical_types.get(writers_schema.props.get('logicalType'))  # type: logical.LogicalTypeProcessor
            if lt.can_convert(writers_schema):
                if lt.validate(writers_schema, data_obj):
                    data_obj = lt.convert(writers_schema, data_obj)
                else:
                    raise schema.AvroException(
                        'Wrong object for %s logical type' % writers_schema.props.get('logicalType'))

        if writers_schema.type in _PRIMITIVE_TYPES:
            result = self._primitive_to_json(data_obj, writers_schema)
        elif writers_schema.type == 'fixed':
            result = self._fixed_to_json(data_obj, writers_schema)
        elif writers_schema.type == 'enum':
            result = self._enum_to_json(data_obj, writers_schema)
        elif writers_schema.type == 'array':
            result = self._array_to_json(data_obj, writers_schema)
        elif writers_schema.type == 'map':
            result = self._map_to_json(data_obj, writers_schema)
        elif writers_schema.type in ['record', 'error', 'request']:
            result = self._record_to_json(data_obj, writers_schema)
        elif writers_schema.type in ['union', 'error_union']:
            result = self._union_to_json(data_obj, writers_schema, was_within_array)
        else:
            raise schema.AvroException('Invalid schema type: %s' % writers_schema.type)

        return result

    def _primitive_to_json(self, data_obj, writers_schema):
        if not self.fastavro and isinstance(data_obj, bytes):
            return data_obj.decode()
        return data_obj

    def _fixed_to_json(self, data_obj, writers_schema):
        return data_obj

    def _enum_to_json(self, data_obj, writers_schema):
        return data_obj

    def _array_to_json(self, data_obj, writers_schema):
        return [self._generic_to_json(x, writers_schema.items, True) for x in data_obj]

    def _map_to_json(self, data_obj, writers_schema):
        return {name: self._generic_to_json(x, writers_schema.values) for name, x in six.iteritems(data_obj)}

    def _record_to_json(self, data_obj, writers_schema):
        result = collections.OrderedDict()

        for field in writers_schema.fields:
            result[field.name] = self._generic_to_json(
                data_obj.get(field.name,
                             self.from_json_object(field.default, field.type) if field.has_default else None),
                field.type)
        return result
    
    def _is_unambiguous_union(self, writers_schema) -> bool:
        if any(isinstance(candidate_schema, schema.EnumSchema) for candidate_schema in writers_schema.schemas):
            if len(writers_schema.schemas) == 2 and any(candidate_schema.type == 'null' for candidate_schema in writers_schema.schemas):
                # Enums and null do not conflict, so this is fine.
                return True
            else:
                # Enum and string conflict, so this case is ambiguous.
                return False

        advanced_count = 0
        for candidate_schema in writers_schema.schemas:
            if candidate_schema.type != 'null':
                advanced_count += 1
        if advanced_count <= 1:
            return True
        return False

    def _union_to_json(self, data_obj, writers_schema, was_within_array=False):
        index_of_schema = -1
        data_schema = self._get_record_schema_if_available(data_obj)
        for i, candidate_schema in enumerate(writers_schema.schemas):
            # Check for exact matches first.
            if data_schema and candidate_schema.fullname == data_schema.fullname:
                index_of_schema = i
                break

            # Fallback to schema guessing based on validation.
            if self.validate(candidate_schema, data_obj):
                index_of_schema = i
                if candidate_schema.type == 'boolean':
                    break
        if index_of_schema < 0:
            raise AvroTypeException(writers_schema, data_obj)
        candidate_schema = writers_schema.schemas[index_of_schema]
        if candidate_schema.type == 'null':
            return None
        
        output_obj = self._generic_to_json(data_obj, candidate_schema)
        if not self.fastavro and not was_within_array and self._is_unambiguous_union(writers_schema):
            # If the union is unambiguous, we can avoid wrapping it in
            # an extra layer of tuples or dicts. Fastavro doesn't like this though.
            # Arrays with unions inside must specify the type.
            return output_obj
        if self.fastavro:
            # Fastavro likes tuples instead of dicts for union types.
            return (self._fullname(candidate_schema), output_obj)
        return {self._fullname(candidate_schema): output_obj}

    def _generic_from_json(self, json_obj, writers_schema, readers_schema):
        if (writers_schema.type not in ['union', 'error_union']
            and readers_schema.type in ['union', 'error_union']):
            for s in readers_schema.schemas:
                if writers_schema.match(s):
                    return self._generic_from_json(json_obj, writers_schema, s)
            raise SchemaResolutionException('Schemas do not match', writers_schema, readers_schema)

        result = None
        if writers_schema.type == 'null':
            result = None
        elif writers_schema.type in _PRIMITIVE_TYPES:
            result = self._primitive_from_json(json_obj, writers_schema, readers_schema)
        elif writers_schema.type == 'fixed':
            result = self._fixed_from_json(json_obj, writers_schema, readers_schema)
        elif writers_schema.type == 'enum':
            result = self._enum_from_json(json_obj, writers_schema, readers_schema)
        elif writers_schema.type == 'array':
            result = self._array_from_json(json_obj, writers_schema, readers_schema)
        elif writers_schema.type == 'map':
            result = self._map_from_json(json_obj, writers_schema, readers_schema)
        elif writers_schema.type in ('union', 'error_union'):
            result = self._union_from_json(json_obj, writers_schema, readers_schema)
        elif writers_schema.type in ('record', 'error', 'request'):
            result = self._record_from_json(json_obj, writers_schema, readers_schema)

        result = self._logical_type_from_json(writers_schema, readers_schema, result)
        return result

    def _logical_type_from_json(self, writers_schema, readers_schema, result):
        if self.use_logical_types and readers_schema.props.get('logicalType'):
            lt = self.logical_types.get(readers_schema.props.get('logicalType'))  # type: logical.LogicalTypeProcessor
            if lt and lt.does_match(writers_schema, readers_schema):
                result = lt.convert_back(writers_schema, readers_schema, result)
        return result

    def _primitive_from_json(self, json_obj, writers_schema, readers_schema):
        if not self.fastavro and writers_schema.type == 'bytes':
            if isinstance(json_obj, str):
                return json_obj.encode()
        return json_obj

    def _fixed_from_json(self, json_obj, writers_schema, readers_schema):
        return json_obj

    def _enum_from_json(self, json_obj, writers_schema, readers_schema):
        return json_obj

    def _array_from_json(self, json_obj, writers_schema, readers_schema):
        return [self._generic_from_json(x, writers_schema.items, readers_schema.items)
                for x in json_obj]

    def _map_from_json(self, json_obj, writers_schema, readers_schema):
        return {name: self._generic_from_json(value, writers_schema.values, readers_schema.values)
                for name, value in six.iteritems(json_obj)}

    def _union_from_json(self, json_obj, writers_schema, readers_schema):
        if json_obj is None:
            return None
        value_type = None
        value = None
        if not self.fastavro and isinstance(json_obj, dict):
            items = list(six.iteritems(json_obj))
            if not items:
                return None
            value_type = items[0][0]
            value = items[0][1]
        if self.fastavro and (isinstance(json_obj, list) or isinstance(json_obj, tuple)):
            if len(json_obj) == 2:
                value_type = json_obj[0]
                value = json_obj[1]

        if value_type is not None:
            for s in writers_schema.schemas:
                name = self._fullname(s)
                if name == value_type:
                    return self._generic_from_json(value, s, readers_schema)

        for s in writers_schema.schemas:
            if self.validate(s, json_obj, skip_logical_types=True):
                return self._generic_from_json(json_obj, s, readers_schema)
        raise schema.AvroException('Datum union type not in schema: %s', value_type)
    
    def _make_type(self, tp, record):
        if issubclass(tp, DictWrapper):
            return tp.construct(record)
        return tp(record)

    def _instantiate_record(self, decoded_record, writers_schema, readers_schema):
        # First try the fullname, which includes namespaces.
        readers_name = self._fullname(readers_schema)
        if readers_name in self.schema_types:
            return self._make_type(self.schema_types[readers_name], decoded_record)
        # Fallback to the bare name, without namespace.
        readers_name = readers_schema.name
        if readers_name in self.schema_types:
            return self._make_type(self.schema_types[readers_name], decoded_record)
        return decoded_record

    def _record_from_json(self, json_obj, writers_schema, readers_schema):
        writer_fields = writers_schema.fields_dict

        input_keys = set(json_obj.keys())

        result = {}
        for field in readers_schema.fields:
            writers_field = writer_fields.get(field.name)
            if writers_field is None:
                field_value = self._generic_from_json(field.default, field.type, field.type) \
                    if field.has_default else None
            else:
                if field.name in json_obj:
                    field_value = self._generic_from_json(json_obj[field.name], writers_field.type, field.type)
                    input_keys.remove(field.name)
                else:
                    _, nullable = find_type_of_default(field.type)
                    if writers_field.has_default:
                        field_value = self._generic_from_json(writers_field.default,
                                                            writers_field.type, field.type)
                    elif nullable:
                        field_value = None
                    else:
                        raise ValueError(f'{readers_schema.fullname} is missing required field: {field.name}')
            result[field.name] = field_value
        if input_keys:
            raise ValueError(f'{readers_schema.fullname} contains extra fields: {input_keys}')
        return self._instantiate_record(result, writers_schema, readers_schema)

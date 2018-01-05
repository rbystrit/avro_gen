import collections
import six

from . import logical
from avro import schema
from avro import io

_PRIMITIVE_TYPES = set(schema.PRIMITIVE_TYPES)


class AvroJsonConverter(object):
    def __init__(self, use_logical_types=False, logical_types=logical.DEFAULT_LOGICAL_TYPES, schema_types=None):
        self.use_logical_types = use_logical_types
        self.logical_types = logical_types or {}
        self.schema_types = schema_types or {}

    def validate(self, expected_schema, datum):
        return io.validate(expected_schema, datum)

    def from_json_dict(self, json_obj, writers_schema=None, readers_schema=None):
        if readers_schema is None:
            readers_schema = writers_schema
        if writers_schema is None:
            writers_schema = readers_schema

        if writers_schema is None:
            raise Exception('At least one schema must be specified')

        return self._generic_from_json(json_obj, writers_schema, readers_schema)

    def to_json_dict(self, data_obj, writers_schema=None):
        if hasattr(type(data_obj), 'RECORD_SCHEMA'):
            writers_schema = type(data_obj).writers_schema

        if writers_schema is None:
            raise Exception("Could not determine writer's schema from the object type and schema was not passed")
        assert isinstance(writers_schema, schema.Schema)

        if not io.validate(writers_schema, data_obj):
            raise io.AvroTypeException(schema, data_obj)

    def _fullname(self, schema_):
        if isinstance(schema_, schema.NamedSchema):
            return schema_.fullname
        return schema_.type

    def _generic_to_json(self, data_obj, writers_schema):
        result = None
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
            result = self._union_to_json(data_obj, writers_schema)
        else:
            raise schema.AvroException('Invalid schema type: %s' % writers_schema.type)

        if self.use_logical_types and writers_schema.get_prop('logicalType'):
            lt = self.logical_types.get(writers_schema.get_prop('logicalType')) #type: logical.LogicalTypeProcessor
            if lt.can_convert(writers_schema):
                result = lt.convert(writers_schema)
        return result

    def _primitive_to_json(self, data_obj, writers_schema):
        return data_obj

    def _fixed_to_json(self, data_obj, writers_schema):
        return data_obj

    def _enum_to_json(self, data_obj, writers_schema):
        return data_obj

    def _array_to_json(self, data_obj, writers_schema):
        return [self._generic_to_json(x, writers_schema.items) for x in data_obj]

    def _map_to_json(self, data_obj, writers_schema):
        return {name: self._generic_to_json(x, writers_schema.values) for name, x in six.iteritems(data_obj)}

    def _record_to_json(self, data_obj, writers_schema):
        result = collections.OrderedDict()

        for field in writers_schema.fields:
            result[field.name] = self._generic_to_json(
                data_obj.get(field.name,
                             self.from_json_dict(field.default, field.type) if field.has_default else None),
                field.type)
        return result

    def _union_to_json(self, data_obj, writers_schema):
        index_of_schema = -1
        for i, candidate_schema in enumerate(writers_schema.schemas):
            if io.validate(candidate_schema, data_obj):
                index_of_schema = i
                if candidate_schema.type == 'boolean':
                    break
        if index_of_schema < 0:
            raise io.AvroTypeException(writers_schema, data_obj)
        candidate_schema = writers_schema.schemas[index_of_schema]
        if candidate_schema.type == 'null':
            return None
        return {self._fullname(candidate_schema): self._generic_to_json(data_obj, candidate_schema)}

    def _generic_from_json(self, json_obj, writers_schema, readers_schema):
        if (writers_schema.type not in ['union', 'error_union']
                and readers_schema.type in ['union', 'error_union']):
            for s in readers_schema.schemas:
                if io.DatumReader.match_schemas(writers_schema, s):
                    return self._generic_from_json(json_obj, writers_schema, s)
            raise io.SchemaResolutionException('Schemas do not match', writers_schema, readers_schema)

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
        if self.use_logical_types and readers_schema.get_prop('logicalType'):
            lt = self.logical_types.get(readers_schema.get_prop('logicalType'))  # type: logical.LogicalTypeProcessor
            if lt and lt.does_match(writers_schema, readers_schema):
                result = lt.convert_back(writers_schema, readers_schema, result)
        return result

    def _primitive_from_json(self, json_obj, writers_schema, readers_schema):
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
        items = list(six.iteritems(json_obj))
        if not items:
            return None

        value_type = items[0][0]
        value = items[0][1]
        for s in writers_schema.schemas:
            name = self._fullname(s)
            if s == value_type:
                return self._generic_from_json(value, s, readers_schema)
        raise schema.AvroException('Datum union type not in schema: %s', value_type)

    def _instantiate_record(self, decoded_record, writers_schema, readers_schema):
        readers_name = self._fullname(readers_schema)
        if readers_name in self.schema_types:
            return self.schema_types[readers_name](decoded_record)
        return decoded_record

    def _record_from_json(self, json_obj, writers_schema, readers_schema):
        writer_fields = writers_schema.field_dict

        result = {}
        for field in readers_schema.fields:
            writers_field = writer_fields.get(field.name)
            if writers_field is None:
                field_value = self._generic_from_json(field.default, field.type, field.type) \
                    if field.has_default else None
            else:
                if field.name in json_obj:
                    field_value = self._generic_from_json(json_obj[field.name], writers_field.type, field.type) \
                        if field.has_default else None
                else:
                    field_value = self._generic_from_json(writers_field.default,
                                                          writers_field.type, field.type) \
                        if writers_field.has_default else None
            result[field.name] = field_value
        return self._instantiate_record(result, writers_schema, readers_schema)
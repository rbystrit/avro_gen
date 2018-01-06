from avrogen import avrojson
from avrogen import logical
from avro import schema
import unittest
import six
import datetime


class AvroJsonTest(unittest.TestCase):
    converter = avrojson.AvroJsonConverter()
    converter_lt = avrojson.AvroJsonConverter(use_logical_types=True)

    def test_primitive_types(self):
        primitives = dict(int=1, float=2.0,
                          string='3.0', bytes=bytes('4.0'), boolean=True, long=5L, double=6.0, null=None)

        for t, v in six.iteritems(primitives):
            test_schema = schema.PrimitiveSchema(t)
            self.assertEquals(self.converter.to_json_object(v, test_schema), v)
            self.assertEquals(self.converter.from_json_object(v, test_schema), v)

    def test_enum(self):
        test_schema = schema.EnumSchema('test_enum', None, ['A', 'B'], schema.Names())
        self.assertEquals(self.converter.to_json_object('A', test_schema), 'A')
        self.assertEquals(self.converter.from_json_object('B', test_schema), 'B')

    def test_fixed(self):
        test_schema = schema.FixedSchema('test_enum', None, 5, schema.Names())
        self.assertEquals(self.converter.to_json_object('A' * 5, test_schema), 'A' * 5)
        self.assertEquals(self.converter.from_json_object('B' * 5, test_schema), 'B' * 5)

    def test_array(self):
        test_schema = schema.make_avsc_object({'type': 'array', 'items': 'int'})
        self.assertEquals(self.converter.to_json_object([1, 2, 3], test_schema), [1, 2, 3])
        self.assertEquals(self.converter.from_json_object([1, 2, 3], test_schema), [1, 2, 3])

    def test_map(self):
        test_schema = schema.make_avsc_object({'type': 'map', 'values': 'int'})
        d = dict(a=1, b=2, c=3)
        self.assertDictEqual(self.converter.to_json_object(d, test_schema), d)
        self.assertDictEqual(self.converter.from_json_object(d, test_schema), d)

    def test_union(self):
        test_schema = schema.make_avsc_object(['int', 'string', 'null'])
        result1 = dict(string='a')
        result2 = dict(int=2)

        self.assertIsNone(self.converter.to_json_object(None, test_schema))
        self.assertIsNone(self.converter.from_json_object(None, test_schema))

        self.assertDictEqual(self.converter.to_json_object('a', test_schema), result1)
        self.assertDictEqual(self.converter.to_json_object(2, test_schema), result2)

        self.assertEquals(self.converter.from_json_object(result1, test_schema), 'a')
        self.assertEquals(self.converter.from_json_object(result2, test_schema), 2)

    def test_record(self):
        test_schema = schema.make_avsc_object({'type': 'record', 'name': 'test_record', 'fields': [
            {'name': 'field1', 'type': 'int'},
            {'name': 'field2', 'type': 'string'}
        ]})

        d = dict(field1=2, field2='3')
        self.assertDictEqual(self.converter.to_json_object(d, test_schema), d)
        self.assertDictEqual(self.converter.from_json_object(d, test_schema), d)

    def test_schema_evolution(self):
        writers_schema = schema.make_avsc_object({'type': 'record', 'name': 'test_record', 'fields': [
            {'name': 'field1', 'type': 'int'},
            {'name': 'field2', 'type': 'string'}
        ]})

        readers_schema = schema.make_avsc_object({'type': 'record', 'name': 'test_record', 'fields': [
            {'name': 'field1', 'type': 'int'},
            {'name': 'field2', 'type': 'string'},
            {'name': 'field3', 'type': 'double', 'default': 3.0}
        ]})

        input = dict(field1=2, field2='3')
        output = dict(field1=2, field2='3', field3=3.0)

        self.assertDictEqual(self.converter.from_json_object(self.converter.to_json_object(input, writers_schema),
                                                             writers_schema, readers_schema),
                             output)

    def test_logical_type(self):
        writers_schema = schema.make_avsc_object({'type': 'record', 'name': 'test_record', 'fields': [
            {'name': 'field1', 'type': 'int'},
            {'name': 'field2', 'type': 'string'},
            {'name': 'date1', 'type': {'type': 'int', 'logicalType': 'date'}}
        ]})

        readers_schema = schema.make_avsc_object({'type': 'record', 'name': 'test_record', 'fields': [
            {'name': 'field1', 'type': 'int'},
            {'name': 'field2', 'type': 'string'},
            {'name': 'field3', 'type': 'double', 'default': 3.0},
            {'name': 'date1', 'type': {'type': 'int', 'logicalType': 'date'}},
            {'name': 'date2', 'type': {'type': 'int', 'logicalType': 'date'}, 'default': 42}
        ]})

        input = dict(field1=2, field2='3', date1=datetime.date(2012, 3, 4))
        output1 = dict(field1=2, field2='3', date1=datetime.date(2012, 3, 4))
        output2 = dict(field1=2, field2='3', field3=3.0, date1=datetime.date(2012, 3, 4),
                       date2=datetime.date(1970, 2, 12))

        self.assertDictEqual(self.converter_lt.from_json_object(self.converter_lt.to_json_object(input, writers_schema),
                                                             writers_schema, writers_schema),
                             output1)
        self.assertDictEqual(self.converter_lt.from_json_object(self.converter_lt.to_json_object(input, writers_schema),
                                                             writers_schema, readers_schema),
                             output2)

    @unittest.expectedFailure
    def test_schema_mismatch(self):
        self.converter.from_json_object(42, schema.make_avsc_object('int'), schema.make_avsc_object('string'))

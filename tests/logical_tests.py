from __future__ import division
from avrogen.logical import DecimalLogicalTypeProcessor, DateLogicalTypeProcessor
from avrogen.logical import TimestampMicrosLogicalTypeProcessor, TimestampMillisLogicalTypeProcessor
from avrogen.logical import TimeMicrosLogicalTypeProcessor, TimeMillisLogicalTypeProcessor
from avro import schema
import unittest
import decimal
import contextlib
import datetime
import pytz
import tzlocal
import time


class LogicalTypeTest(unittest.TestCase):
    @contextlib.contextmanager
    def _exception(self):
        try:
            yield
            self.fail('Expected exception was not raised')
        except:
            pass

    def test_decimal(self):
        p = DecimalLogicalTypeProcessor()

        test_schema1 = schema.make_avsc_object('string')
        test_schema2 = schema.make_avsc_object('int')
        test_schema3 = schema.make_avsc_object(
            {'type': 'record', 'name': 'test2', 'fields': [{'name': 'f1', 'type': 'int'}]})

        self.assertTrue(p.can_convert(test_schema1))
        self.assertFalse(p.can_convert(test_schema2))
        self.assertFalse(p.can_convert(test_schema3))
        self.assertTrue(p.does_match(test_schema1, test_schema2))
        self.assertFalse(p.does_match(test_schema2, test_schema2))
        self.assertFalse(p.does_match(test_schema3, test_schema3))

        self.assertEquals(p.convert(test_schema1, 1234), '1234')
        self.assertEquals(p.convert(test_schema1, 1234.5), '1234.5')
        self.assertEquals(p.convert(test_schema1, 12345L), '12345')
        self.assertEquals(p.convert(test_schema1, decimal.Decimal('12345.678')), '12345.678')

        with self._exception():
            p.convert('123456')

        self.assertEquals(p.convert_back(test_schema1, test_schema1, '123456.789'), decimal.Decimal('123456.789'))

    def test_date(self):
        p = DateLogicalTypeProcessor()

        test_schema1 = schema.make_avsc_object('string')
        test_schema2 = schema.make_avsc_object('int')
        test_schema3 = schema.make_avsc_object(
            {'type': 'record', 'name': 'test2', 'fields': [{'name': 'f1', 'type': 'int'}]})

        self.assertFalse(p.can_convert(test_schema1))
        self.assertTrue(p.can_convert(test_schema2))
        self.assertFalse(p.can_convert(test_schema3))

        self.assertFalse(p.does_match(test_schema1, test_schema2))
        self.assertTrue(p.does_match(test_schema2, test_schema2))
        self.assertFalse(p.does_match(test_schema3, test_schema3))

        self.assertEquals(p.convert(test_schema2, datetime.date(2015, 3, 4)), 16498)
        with self._exception():
            p.convert('123456')

        self.assertEquals(p.convert_back(test_schema2, test_schema2, 16498), datetime.date(2015, 3, 4))

    def test_time_micros(self):
        p = TimeMicrosLogicalTypeProcessor()

        test_schema1 = schema.make_avsc_object('string')
        test_schema2 = schema.make_avsc_object('long')
        test_schema3 = schema.make_avsc_object(
            {'type': 'record', 'name': 'test2', 'fields': [{'name': 'f1', 'type': 'int'}]})

        self.assertFalse(p.can_convert(test_schema1))
        self.assertTrue(p.can_convert(test_schema2))
        self.assertFalse(p.can_convert(test_schema3))

        self.assertFalse(p.does_match(test_schema1, test_schema2))
        self.assertTrue(p.does_match(test_schema2, test_schema2))
        self.assertFalse(p.does_match(test_schema3, test_schema3))

        self.assertEquals(p.convert(test_schema2, datetime.time(23, 24, 25, 123456)), 84265123456L)
        with self._exception():
            p.convert('123456')

        self.assertEquals(p.convert_back(test_schema2, test_schema2, 84265123456L), datetime.time(23, 24, 25, 123456))

    def test_time_millis(self):
        p = TimeMillisLogicalTypeProcessor()

        test_schema1 = schema.make_avsc_object('string')
        test_schema2 = schema.make_avsc_object('int')
        test_schema3 = schema.make_avsc_object(
            {'type': 'record', 'name': 'test2', 'fields': [{'name': 'f1', 'type': 'int'}]})

        self.assertFalse(p.can_convert(test_schema1))
        self.assertTrue(p.can_convert(test_schema2))
        self.assertFalse(p.can_convert(test_schema3))

        self.assertFalse(p.does_match(test_schema1, test_schema2))
        self.assertTrue(p.does_match(test_schema2, test_schema2))
        self.assertFalse(p.does_match(test_schema3, test_schema3))

        self.assertEquals(p.convert(test_schema2, datetime.time(23, 24, 25, 123456)), 84265123L)
        with self._exception():
            p.convert('123456')

        self.assertEquals(p.convert_back(test_schema2, test_schema2, 84265123L), datetime.time(23, 24, 25, 123000))

    def test_timestamp_micros(self):
        p = TimestampMicrosLogicalTypeProcessor()

        test_schema1 = schema.make_avsc_object('string')
        test_schema2 = schema.make_avsc_object('long')
        test_schema3 = schema.make_avsc_object(
            {'type': 'record', 'name': 'test2', 'fields': [{'name': 'f1', 'type': 'int'}]})

        self.assertFalse(p.can_convert(test_schema1))
        self.assertTrue(p.can_convert(test_schema2))
        self.assertFalse(p.can_convert(test_schema3))

        self.assertFalse(p.does_match(test_schema1, test_schema2))
        self.assertTrue(p.does_match(test_schema2, test_schema2))
        self.assertFalse(p.does_match(test_schema3, test_schema3))

        dt1 = datetime.datetime(2015, 5, 1)
        self.assertEquals(p.convert(test_schema2, datetime.date(2015, 5, 1)), p.convert(test_schema2, dt1))
        self.assertEquals(p.convert(test_schema2, pytz.UTC.localize(datetime.datetime(2015, 5, 1, microsecond=123456))),
                          1430438400123456)

        offset_res = 1430452800123456L
        self.assertEquals(p.convert(test_schema2,
                                    pytz.timezone('America/New_York').localize(
                                        datetime.datetime(2015, 5, 1, 0, 0, 0, microsecond=123456))),
                          offset_res)
        with self._exception():
            p.convert('123456')

        self.assertEquals(
            p.convert_back(test_schema2, test_schema2, p.convert(test_schema2, datetime.datetime(2016, 1, 1))),
            datetime.datetime(2016, 1, 1))

    def test_timestamp_millis(self):
        p = TimestampMillisLogicalTypeProcessor()

        test_schema1 = schema.make_avsc_object('string')
        test_schema2 = schema.make_avsc_object('long')
        test_schema3 = schema.make_avsc_object(
            {'type': 'record', 'name': 'test2', 'fields': [{'name': 'f1', 'type': 'int'}]})

        self.assertFalse(p.can_convert(test_schema1))
        self.assertTrue(p.can_convert(test_schema2))
        self.assertFalse(p.can_convert(test_schema3))

        self.assertFalse(p.does_match(test_schema1, test_schema2))
        self.assertTrue(p.does_match(test_schema2, test_schema2))
        self.assertFalse(p.does_match(test_schema3, test_schema3))

        dt1 = datetime.datetime(2015, 5, 1)
        self.assertEquals(p.convert(test_schema2, datetime.date(2015, 5, 1)), p.convert(test_schema2, dt1))
        self.assertEquals(p.convert(test_schema2, datetime.datetime(2015, 5, 1, microsecond=123456, tzinfo=pytz.UTC)),
                          1430438400123)

        offset_res = 1430452800123L
        self.assertEquals(
            p.convert(test_schema2,
                      pytz.timezone('America/New_York').localize(datetime.datetime(2015, 5, 1, microsecond=123456))),
            offset_res)
        with self._exception():
            p.convert('123456')

        self.assertEquals(
            p.convert_back(test_schema2, test_schema2, p.convert(test_schema2, datetime.datetime(2016, 1, 1))),
            datetime.datetime(2016, 1, 1))


if __name__ == "__main__":
    unittest.main()

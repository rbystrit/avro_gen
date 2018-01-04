from __future__ import division
from avro import schema, io
import functools
import abc
import six
import collections
import frozendict
import datetime
import decimal
import struct
import time
import pytz
import tzlocal

EPOCH_DATE = datetime.date(1970, 1, 1)
SECONDS_IN_DAY = 24 * 60 * 60
EPOCH_TT = time.mktime((1970, 1, 1, 0, 0, 0, 0, 0, 0))


class LogicalTypeProcessor(object, six.with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def does_match(self, writers_schema, readers_schema):
        return False

    @abc.abstractmethod
    def convert_back(self, writers_schema, readers_schema, value):
        return value

    @abc.abstractmethod
    def convert(self, value):
        pass

    @abc.abstractmethod
    def can_convert(self, writers_schema):
        pass


class DecimalLogicalTypeProcessor(LogicalTypeProcessor):
    def can_convert(self, writers_schema):
        return isinstance(writers_schema, schema.PrimitiveSchema) and writers_schema.type == 'string'

    def convert(self, value):
        if not isinstance(value, (int, float, long, decimal.Decimal)):
            raise Exception('Wrong type for decimal conversion')
        return str(value)

    def convert_back(self, writers_schema, readers_schema, value):
        return decimal.Decimal(value)

    def does_match(self, writers_schema, readers_schema):
        if isinstance(writers_schema, schema.PrimitiveSchema):
            if writers_schema.type == 'string':
                return True
        return False


class DateLogicalTypeProcessor(LogicalTypeProcessor):
    _matching_types = {'int', 'long', 'float', 'double'}

    def can_convert(self, writers_schema):
        return isinstance(writers_schema, schema.PrimitiveSchema) and writers_schema.type == 'int'

    def convert(self, value):
        if not isinstance(value, datetime.date):
            raise Exception("Wrong type for date conversion")
        return (value - EPOCH_DATE).total_seconds() // SECONDS_IN_DAY

    def convert_back(self, writers_schema, readers_schema, value):
        return EPOCH_DATE + datetime.timedelta(days=int(value))

    def does_match(self, writers_schema, readers_schema):
        if isinstance(writers_schema, schema.PrimitiveSchema):
            if writers_schema.type in DateLogicalTypeProcessor._matching_types:
                return True
        return False


class TimeMicrosLogicalTypeProcessor(LogicalTypeProcessor):
    _matching_types = {'int', 'long', 'float', 'double'}

    def can_convert(self, writers_schema):
        return isinstance(writers_schema, schema.PrimitiveSchema) and writers_schema.type == 'long'

    def convert(self, value):
        if not isinstance(value, datetime.time):
            raise Exception('Wrong type for time conversion')
        return ((value.hour * 60 + value.minute) * 60 + value.second) * 1000000 + value.microsecond

    def convert_back(self, writers_schema, readers_schema, value):
        _, hours, minutes, seconds, microseconds = TimeMicrosLogicalTypeProcessor.extract_time_parts(value)
        return datetime.time(hours, minutes, seconds, microseconds)

    @staticmethod
    def extract_time_parts(value):
        value = long(value)
        days = value // 86400000000
        hours = value % 86400000000
        minutes = hours % 3600000000
        hours = hours // 3600000000
        seconds = minutes % 60000000
        minutes = minutes // 60000000
        microseconds = seconds % 1000000
        seconds = seconds // 1000000
        return days, hours, minutes, seconds, microseconds

    def does_match(self, writers_schema, readers_schema):
        if isinstance(writers_schema, schema.PrimitiveSchema):
            if writers_schema.type in TimeMicrosLogicalTypeProcessor._matching_types:
                return True
        return False


class TimeMillisLogicalTypeProcessor(TimeMicrosLogicalTypeProcessor):
    def can_convert(self, writers_schema):
        return isinstance(writers_schema, schema.PrimitiveSchema) and writers_schema.type == 'int'

    def convert(self, value):
        if not isinstance(value, datetime.time):
            raise Exception('Wrong type for time conversion')
        return int(super(TimeMillisLogicalTypeProcessor, self).convert(value) // 1000)

    def convert_back(self, writers_schema, readers_schema, value):
        return super(TimeMillisLogicalTypeProcessor, self).convert_back(writers_schema, readers_schema, value * 1000)


class TimestampMicrosLogicalTypeProcessor(LogicalTypeProcessor):
    _matching_types = {'int', 'long', 'float', 'double'}

    def can_convert(self, writers_schema):
        return isinstance(writers_schema, schema.PrimitiveSchema) and writers_schema.type == 'long'

    def convert(self, value):
        if not isinstance(value, datetime.datetime):
            if isinstance(value, datetime.date):
                value = tzlocal.get_localzone().localize(
                    datetime.datetime(value.year, value.month, value.day, 0, 0, 0, 0))

        if value.tzinfo is None:
            value = tzlocal.get_localzone().localize(value)
        value = (time.mktime(value.utctimetuple()) - EPOCH_TT) + value.microsecond / 1000000.0
        return long(value * 1000000)

    def convert_back(self, writers_schema, readers_schema, value):
        value = long(value)/1000000.0
        utc = datetime.datetime.utcfromtimestamp(value).replace(tzinfo=pytz.UTC)
        return utc.astimezone(tzlocal.get_localzone()).replace(tzinfo=None)

    def does_match(self, writers_schema, readers_schema):
        if isinstance(writers_schema, schema.PrimitiveSchema):
            if writers_schema.type in TimestampMicrosLogicalTypeProcessor._matching_types:
                return True
        return False


class TimestampMillisLogicalTypeProcessor(TimestampMicrosLogicalTypeProcessor):
    def convert(self, value):
        return super(TimestampMillisLogicalTypeProcessor, self).convert(value) // 1000

    def convert_back(self, writers_schema, readers_schema, value):
        return super(TimestampMillisLogicalTypeProcessor, self).convert_back(writers_schema, readers_schema,
                                                                             value * 1000)


DEFAULT_LOGICAL_TYPES = frozendict.frozendict(**{
    'decimal': DecimalLogicalTypeProcessor(),
    'date': DateLogicalTypeProcessor(),
    'time-millis': TimeMicrosLogicalTypeProcessor(),
    'time-micros': TimeMicrosLogicalTypeProcessor(),
    'timestamp-millis': TimestampMillisLogicalTypeProcessor(),
    'timestamp-micros': TimestampMicrosLogicalTypeProcessor(),
})


class LogicalDatumReader(io.DatumReader):
    def __init__(self, writers_schema=None, readers_schema=None, logical_types=DEFAULT_LOGICAL_TYPES):
        """
        Initializes DatumReader with logical type support

        :param schema.Schema writers_schema: Writer's schema
        :param schema.Schema readers_schema: Optional reader's schema
        :param dict[str, LogicalTypeProcessor] logical_types: Optional logical types dict
        """
        super(LogicalDatumReader, self).__init__(writers_schema=writers_schema, readers_schema=readers_schema)
        self.logical_types = logical_types or {}

    def read_data(self, writers_schema, readers_schema, decoder):
        """
        Reads data from the decoded stream and applies logical type conversion

        :param schema.Schema writers_schema:
        :param schema.Schema readers_schema:
        :param io.BinaryDecoder decoder:
        :return:
        """
        result = super(LogicalDatumReader, self).read_data(writers_schema, readers_schema, decoder)
        logical_type = readers_schema.get_prop('logicalType')
        if logical_type:
            logical_type_handler = self.logical_types.get(logical_type)
            if logical_type_handler and logical_type_handler.does_match(writers_schema, readers_schema):
                result = logical_type_handler.convert_back(writers_schema, readers_schema, result)
        return result


class LogicalDatumWriter(io.DatumWriter):
    """
       Initializes DatumWriter with logical type support

       :param schema.Schema writers_schema: Writer's schema
       :param dict[str, LogicalTypeProcessor] logical_types: Optional logical types dict
       """

    def __init__(self, writers_schema=None, logical_types=DEFAULT_LOGICAL_TYPES):
        super(LogicalDatumWriter, self).__init__(writers_schema=writers_schema)
        self.logical_types = logical_types

    def write_data(self, writers_schema, datum, encoder):
        logical_type = writers_schema.get_prop('logicalType')
        if logical_type:
            logical_type_handler = self.logical_types.get(logical_type)
            if logical_type_handler and logical_type_handler.can_convert(writers_schema):
                return super(LogicalDatumWriter, self).write_data(writers_schema, logical_type_handler.convert(datum),
                                                                  encoder)
        return super(LogicalDatumWriter, self).write_data(writers_schema, datum, encoder)


def patch_logical_types():
    io.DatumWriter = LogicalDatumWriter
    io.DatumReader = LogicalDatumReader

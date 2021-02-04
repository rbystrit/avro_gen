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

if six.PY3:
    long = int

EPOCH_DATE = datetime.date(1970, 1, 1)
SECONDS_IN_DAY = 24 * 60 * 60

# below doesn't work for time zones which are ahead of UTC,
# so will need to switch to datetime arithmetic
#EPOCH_TT = time.mktime((1970, 1, 1, 0, 0, 0, 0, 0, 0))

EPOCH_TT = -datetime.datetime(1970, 1, 1).astimezone(tzlocal.get_localzone()).utcoffset().total_seconds()


class LogicalTypeProcessor(object, six.with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def validate(self, expected_schema, datum):
        pass

    @abc.abstractmethod
    def does_match(self, writers_schema, readers_schema):
        return False

    @abc.abstractmethod
    def convert_back(self, writers_schema, readers_schema, value):
        return value

    @abc.abstractmethod
    def convert(self, writers_schema, value):
        pass

    @abc.abstractmethod
    def can_convert(self, writers_schema):
        pass

    @abc.abstractmethod
    def typename(self):
        pass

    @abc.abstractmethod
    def initializer(self, value=None):
        pass


class DecimalLogicalTypeProcessor(LogicalTypeProcessor):
    def can_convert(self, writers_schema):
        return isinstance(writers_schema, schema.PrimitiveSchema) and writers_schema.type == 'string'

    def validate(self, expected_schema, datum):
        return isinstance(datum, (int, float, long, decimal.Decimal))

    def convert(self, writers_schema, value):
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

    def typename(self):
        return 'decimal.Decimal'

    def initializer(self, value=None):
        return 'decimal.Decimal(%s)' % (0 if value is None else value)


class DateLogicalTypeProcessor(LogicalTypeProcessor):
    _matching_types = {'int', 'long', 'float', 'double'}

    def can_convert(self, writers_schema):
        return isinstance(writers_schema, schema.PrimitiveSchema) and writers_schema.type == 'int'

    def validate(self, expected_schema, datum):
        return isinstance(datum, datetime.date)

    def convert(self, writers_schema, value):
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

    def typename(self):
        return 'datetime.date'

    def initializer(self, value=None):
        return ((
                    'logical.DateLogicalTypeProcessor().convert_back(None, None, %s)' % value) if value is not None
                else 'datetime.datetime.today().date()')


class TimeMicrosLogicalTypeProcessor(LogicalTypeProcessor):
    _matching_types = {'int', 'long', 'float', 'double'}

    def can_convert(self, writers_schema):
        return isinstance(writers_schema, schema.PrimitiveSchema) and writers_schema.type == 'long'

    def validate(self, expected_schema, datum):
        return isinstance(datum, datetime.time)

    def convert(self, writers_schema, value):
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

    def typename(self):
        return 'datetime.time'

    def initializer(self, value=None):
        return ((
                    'logical.TimeMicrosLogicalTypeProcessor().convert_back(None, None, %s)' % value) if value is not None
                else 'datetime.datetime.today().time()')


class TimeMillisLogicalTypeProcessor(TimeMicrosLogicalTypeProcessor):
    def can_convert(self, writers_schema):
        return isinstance(writers_schema, schema.PrimitiveSchema) and writers_schema.type == 'int'

    def convert(self, writers_schema, value):
        if not isinstance(value, datetime.time):
            raise Exception('Wrong type for time conversion')
        return int(super(TimeMillisLogicalTypeProcessor, self).convert(writers_schema, value) // 1000)

    def convert_back(self, writers_schema, readers_schema, value):
        return super(TimeMillisLogicalTypeProcessor, self).convert_back(writers_schema, readers_schema, value * 1000)

    def initializer(self, value=None):
        return ((
                    'logical.TimeMillisLogicalTypeProcessor().convert_back(None, None, %s)' % value) if value is not None
                else 'datetime.datetime.today().time()')


class TimestampMicrosLogicalTypeProcessor(LogicalTypeProcessor):
    _matching_types = {'int', 'long', 'float', 'double'}

    def can_convert(self, writers_schema):
        return isinstance(writers_schema, schema.PrimitiveSchema) and writers_schema.type == 'long'

    def validate(self, expected_schema, datum):
        return isinstance(datum, datetime.datetime)

    def convert(self, writers_schema, value):
        if not isinstance(value, datetime.datetime):
            if isinstance(value, datetime.date):
                value = tzlocal.get_localzone().localize(
                    datetime.datetime(value.year, value.month, value.day, 0, 0, 0, 0))

        if value.tzinfo is None:
            value = tzlocal.get_localzone().localize(value)
        value = (time.mktime(value.utctimetuple()) - EPOCH_TT) + value.microsecond / 1000000.0
        return long(value * 1000000)

    def convert_back(self, writers_schema, readers_schema, value):
        value = long(value) / 1000000.0
        utc = datetime.datetime.utcfromtimestamp(value).replace(tzinfo=pytz.UTC)
        return utc.astimezone(tzlocal.get_localzone()).replace(tzinfo=None)

    def does_match(self, writers_schema, readers_schema):
        if isinstance(writers_schema, schema.PrimitiveSchema):
            if writers_schema.type in TimestampMicrosLogicalTypeProcessor._matching_types:
                return True
        return False

    def typename(self):
        return 'datetime.datetime'

    def initializer(self, value=None):
        return ((
                    'logical.TimestampMicrosLogicalTypeProcessor().convert_back(None, None, %s)' % value) if value is not None
                else 'datetime.datetime.now()')


class TimestampMillisLogicalTypeProcessor(TimestampMicrosLogicalTypeProcessor):
    def convert(self, writers_schema, value):
        return super(TimestampMillisLogicalTypeProcessor, self).convert(writers_schema, value) // 1000

    def convert_back(self, writers_schema, readers_schema, value):
        return super(TimestampMillisLogicalTypeProcessor, self).convert_back(writers_schema, readers_schema,
                                                                             value * 1000)

    def initializer(self, value=None):
        return ((
                    'logical.TimestampMillisLogicalTypeProcessor().convert_back(None, None, %s)' % value) if value is not None
                else 'datetime.datetime.now()')


DEFAULT_LOGICAL_TYPES = frozendict.frozendict(**{
    'decimal': DecimalLogicalTypeProcessor(),
    'date': DateLogicalTypeProcessor(),
    'time-millis': TimeMillisLogicalTypeProcessor(),
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
        logical_type = readers_schema.props.get('logicalType')
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
        logical_type = writers_schema.props.get('logicalType')
        if logical_type:
            logical_type_handler = self.logical_types.get(logical_type)
            if logical_type_handler and logical_type_handler.can_convert(writers_schema):
                return super(LogicalDatumWriter, self).write_data(writers_schema,
                                                                  logical_type_handler.convert(writers_schema, datum),
                                                                  encoder)
        return super(LogicalDatumWriter, self).write_data(writers_schema, datum, encoder)

    def __validate(self, writers_schema, datum):
        logical_type = writers_schema.props.get('logicalType')
        if logical_type:
            lt = self.logical_types.get(logical_type)
            if lt:
                if lt.can_convert(writers_schema):
                    if lt.validate(writers_schema, datum):
                        return True
                    return False

        schema_type = writers_schema.type
        if schema_type == 'array':
            return (isinstance(datum, list) and
                    False not in [self.__validate(writers_schema.items, d) for d in datum])
        elif schema_type == 'map':
            return (isinstance(datum, dict) and
                    False not in [isinstance(k, basestring) for k in datum.keys()] and
                    False not in
                    [self.__validate(writers_schema.values, v) for v in datum.values()])
        elif schema_type in ['union', 'error_union']:
            return True in [self.__validate(s, datum) for s in writers_schema.schemas]
        elif schema_type in ['record', 'error', 'request']:
            return (isinstance(datum, dict) and
                    False not in
                    [self.__validate(f.type, datum.get(f.name)) for f in writers_schema.fields])

        return io.validate(writers_schema, datum)

    def write(self, datum, encoder):
        # validate datum
        if not self.__validate(self.writers_schema, datum):
            raise io.AvroTypeException(self.writers_schema, datum)

        self.write_data(self.writers_schema, datum, encoder)


def patch_logical_types():
    io.DatumWriter = LogicalDatumWriter
    io.DatumReader = LogicalDatumReader

from avro import schema
import json

PRIMITIVE_TYPES = {
    'null',
    'boolean',
    'int',
    'long',
    'float',
    'double',
    'bytes',
    'string'
}

__PRIMITIVE_TYPE_MAPPING = {
    'null': '',
    'boolean': bool,
    'int': int,
    'long': long,
    'float': float,
    'double': float,
    'bytes': str,
    'string': unicode,
}


class TabbedWriter(object):
    class Indent(object):
        def __init__(self, writer):
            self.writer = writer

        def __enter__(self):
            self.writer.tab()

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.writer.untab()

    def __init__(self, inner_writer, tab_symbol='    '):
        self.__inner_writer = inner_writer
        self.__tabs = 0
        self.__tab_symbol = tab_symbol
        self.__current_tab = ''

    def write(self, text):
        assert isinstance(text, (str, unicode))

        start_pos = 0
        last_pos = text.find('\n')

        while last_pos >= 0:
            self.__inner_writer.write(text[start_pos:last_pos + 1])
            self.__inner_writer.write(self.__current_tab)
            start_pos = last_pos + 1
            last_pos = text.find('\n', start_pos)

        self.__inner_writer.write(text[start_pos:])

    def tab(self):
        self.set_tab(self.__tabs + 1)

    def untab(self):
        self.set_tab(self.__tabs - 1)

    def set_tab(self, tabs):
        self.__tabs = max(0, tabs)
        self.__current_tab = self.__tab_symbol * self.__tabs

    def indent(self):
        return TabbedWriter.Indent(self)

def get_primitive_field_initializer(field_schema):
    """

    :param schema.PrimitiveSchema field_schema:
    :return:
    """

    if field_schema.fullname == 'null':
        return 'None'
    return get_field_type_name(field_schema) + "()"


def get_field_type_name(field_schema):
    """
    :param schema.Schema field_schema:
    :return:
    """

    if isinstance(field_schema, schema.PrimitiveSchema):
        if field_schema.fullname == 'null':
            return ''
        return __PRIMITIVE_TYPE_MAPPING[field_schema.fullname].__name__
    elif isinstance(field_schema, schema.FixedSchema):
        return 'bytes'
    elif isinstance(field_schema, schema.NamedSchema):
        return 'SchemaClasses.' + field_schema.fullname + 'Class'
    elif isinstance(field_schema, schema.ArraySchema):
        return 'list[' + get_field_type_name(field_schema.items) + ']'
    elif isinstance(field_schema, schema.MapSchema):
        return 'dict[str, ' + get_field_type_name(field_schema.values) + ']'
    elif isinstance(field_schema, schema.UnionSchema):
        type_names = [get_field_type_name(x) for x in field_schema.schemas if get_field_type_name(x)]
        if len(type_names) > 1:
            return  ' | '.join(type_names)
        elif len(type_names) == 1:
            return type_names[0]
        return ''


def find_type_of_default(field_type):
    """

    :param schema.Schema field_type:
    :return:
    """

    if isinstance(field_type, schema.UnionSchema):
        type_, nullable =  find_type_of_default(field_type.schemas)
        nullable = nullable or any(f for f in field_type.schemas if isinstance(f, schema.PrimitiveSchema) and f.fullname == 'null')
        return type_, nullable
    elif isinstance(field_type, schema.PrimitiveSchema):
        return field_type, field_type.fullname == 'null'
    else:
        return field_type, False

def write_record(record, writer):
    """

    :param avro.schema.RecordSchema record:
    :param TabbedWriter writer:
    :return:
    """

    type_name = record.name.split('.')[-1]
    writer.write('''\nclass {name}Class(DictWrapper):'''.format(name=type_name))

    with writer.indent():
        writer.write("\n\n")
        writer.write('"""\n')
        writer.write(record.doc or '')
        writer.write('\n')
        writer.write('"""\n\n')

        writer.write('RECORD_SCHEMA = NAMES.get_name("%s", "%s")' % (record.name, record.namespace))

        writer.write('\n\n\ndef __init__(self, inner_dict=None):')
        with writer.indent():
            writer.write('\n')
            writer.write('super(SchemaClasses.{name}Class, self).__init__(inner_dict)'.format(name=record.fullname))

            writer.write('\nif inner_dict is None:')
            with writer.indent():
                something_written = False
                i = 0
                for field in record.fields:
                    default_type, nullable = find_type_of_default(field.type)
                    default_written = False
                    if field.has_default:
                        if isinstance(default_type, schema.RecordSchema):
                            writer.write('\nself.{name} = SchemaClasses.{full_name}(SchemaClasses.{full_name}Class.RECORD_SCHEMA.fields[{idx}].default)'
                                         .format(name = field.name, full_name=default_type.fullname, idx=i))
                            default_written = True
                        elif isinstance(default_type, (schema.PrimitiveSchema, schema.EnumSchema, schema.FixedSchema)):
                            writer.write('\nself.{name} = SchemaClasses.{full_name}Class.RECORD_SCHEMA.fields[{idx}].default'
                                         .format(name = field.name, full_name=default_type.fullname, idx=i))
                            default_written = True

                    if not default_written:
                        default_written = True
                        if nullable:
                            writer.write('\nself.{name} = None'.format(name = field.name))
                        elif isinstance(default_type, schema.PrimitiveSchema):
                            writer.write('\nself.{name} = {default}'.format(name = field.name,
                                                                    default=get_primitive_field_initializer(field.type)))
                        elif isinstance(default_type, schema.EnumSchema):
                            writer.write('\nself.{name} = SchemaClasses.{full_name}Class.{sym}'
                                         .format(name = field.name, full_name=default_type.fullname,
                                                 sym = default_type.symbols[0]))
                        elif isinstance(default_type, schema.MapSchema):
                            writer.write('\nself.{name} = dict()'.format(name = field.name))
                        elif isinstance(default_type, schema.ArraySchema):
                            writer.write('\nself.{name} = list()'.format(name = field.name))
                        elif isinstance(default_type, schema.FixedSchema):
                            writer.write('\nself.{name} = str()'.format(name = field.name))
                        elif isinstance(default_type, schema.RecordSchema):
                            writer.write('\nself.{name} = SchemaClasses.{full_name}Class()'
                                         .format(name = field.name, full_name=default_type.fullname))
                        else:
                            default_written = False
                    something_written = something_written or default_written
                    i += 1

                if not something_written:
                    writer.write('\npass')
        write_fields(record, writer)


def write_fields(record, writer):
    writer.write('\n\n')
    for field in record.fields:  # type: schema.Field
        write_field(field, writer)


def write_field(field, writer):
    writer.write('''
@property
def {name}(self):
    """
    :rtype: {ret_type_name}
    """
    return self._inner_dict.get('{name}')

@{name}.setter
def {name}(self, value):
    #"""
    #:param {ret_type_name} value:
    #"""
    self._inner_dict['{name}'] = value

'''.format(name=field.name, ret_type_name=get_field_type_name(field.type)))


def write_enum(enum, writer):
    """

    :param schema.EnumSchema enum:
    :param TabbedWriter writer:
    :return:
    """
    writer.write('''\nclass {name}Class(DictWrapper):'''.format(name=enum.name))

    with writer.indent():
        writer.write('\n\n')
        writer.write('"""\n')
        writer.write(enum.doc or '')
        writer.write('\n')
        writer.write('"""\n\n')

        for field in enum.symbols:
            writer.write('{name} = "{name}"\n'.format(name=field))


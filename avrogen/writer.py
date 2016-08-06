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


def get_field_default(field):
    """
    
    :param schema.Field field: 
    :return: 
    """
    
    if not field.has_default:
        return 'None'
    field_type = field.type
    while isinstance(field_type, schema.UnionSchema):
        field_type = field_type.schemas[0]

    if isinstance(field_type, schema.PrimitiveSchema):
        if field_type == 'null':
            return 'None'
        elif field_type in ('bytes', 'string'):
            return "'%s'".format(field.default)
        return str(field.default)
    elif isinstance(field_type, schema.FixedSchema):
        return "'%s'".format(field.default)
    elif isinstance(field_type, schema.RecordSchema):
        return field_type.fullname + "(json.loads('''" + json.dumps(field.default) + "'''))"
    elif isinstance(field_type, schema.EnumSchema):
        return field_type.fullname + "." + field.default
    elif isinstance(field_type, (schema.ArraySchema, schema.MapSchema)):
        if isinstance(field_type.items, schema.PrimitiveSchema):
            return "json.loads('''" + json.dumps(field.default) + "''')"
        return 'None'


def get_field_type_name(field_schema):
    """
    :param schema.Schema field:
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

        writer.write('def __init__(self, inner_dict=None):')
        with writer.indent():
            writer.write('\n')
            writer.write('super(SchemaClasses.{name}Class, self).__init__(inner_dict)\n'.format(name=record.fullname))
        writer.write('\n')
        for field in record.fields:  # type: schema.Field
            writer.write('''@property
def {name}(self):
    """
    :rtype: {ret_type_name}
    """
    return self._inner_dict.get('{name}', {default})

@{name}.setter
def {name}(self, value):
    #"""
    #:param {ret_type_name} value:
    #"""
    self._inner_dict['{name}'] = value

'''.format(name=field.name, default=get_field_default(field), ret_type_name=get_field_type_name(field.type)))


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


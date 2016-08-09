import os

from avro import schema
from . import namespace as ns_


def write_defaults(record, writer, my_full_name=None):
    i = 0
    my_full_name = my_full_name or record.fullname

    something_written = False
    for field in record.fields:
        default_type, nullable = find_type_of_default(field.type)
        default_written = False
        if field.has_default:
            if isinstance(default_type, schema.RecordSchema):
                writer.write(
                    '\nself.{name} = SchemaClasses.{full_name}(SchemaClasses.{my_full_name}Class.RECORD_SCHEMA.fields[{idx}].default)'
                        .format(name=field.name, full_name=default_type.fullname, idx=i, my_full_name=my_full_name))
                default_written = True
            elif isinstance(default_type, (schema.PrimitiveSchema, schema.EnumSchema, schema.FixedSchema)):
                writer.write('\nself.{name} = SchemaClasses.{full_name}Class.RECORD_SCHEMA.fields[{idx}].default'
                             .format(name=field.name, full_name=my_full_name, idx=i))
                default_written = True

        if not default_written:
            default_written = True
            if nullable:
                writer.write('\nself.{name} = None'.format(name=field.name))
            elif isinstance(default_type, schema.PrimitiveSchema):
                writer.write('\nself.{name} = {default}'.format(name=field.name,
                                                                default=get_primitive_field_initializer(field.type)))
            elif isinstance(default_type, schema.EnumSchema):
                writer.write('\nself.{name} = SchemaClasses.{full_name}Class.{sym}'
                             .format(name=field.name, full_name=default_type.fullname,
                                     sym=default_type.symbols[0]))
            elif isinstance(default_type, schema.MapSchema):
                writer.write('\nself.{name} = dict()'.format(name=field.name))
            elif isinstance(default_type, schema.ArraySchema):
                writer.write('\nself.{name} = list()'.format(name=field.name))
            elif isinstance(default_type, schema.FixedSchema):
                writer.write('\nself.{name} = str()'.format(name=field.name))
            elif isinstance(default_type, schema.RecordSchema):
                writer.write('\nself.{name} = SchemaClasses.{full_name}Class()'
                             .format(name=field.name, full_name=default_type.fullname))
            else:
                default_written = False
        something_written = something_written or default_written
        i += 1
    if not something_written:
        writer.write('\npass')


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
            return ' | '.join(type_names)
        elif len(type_names) == 1:
            return type_names[0]
        return ''


def find_type_of_default(field_type):
    """

    :param schema.Schema field_type:
    :return:
    """

    if isinstance(field_type, schema.UnionSchema):
        type_, nullable = find_type_of_default(field_type.schemas)
        nullable = nullable or any(
            f for f in field_type.schemas if isinstance(f, schema.PrimitiveSchema) and f.fullname == 'null')
        return type_, nullable
    elif isinstance(field_type, schema.PrimitiveSchema):
        return field_type, field_type.fullname == 'null'
    else:
        return field_type, False


def start_namespace(current, target, writer):
    """

    :param tuple[str] current:
    :param tuple[str] target:
    :param TabbedWriter writer:
    :return:
    """

    i = 0
    while i < min(len(current), len(target)) and current[i] == target[i]:
        i += 1

    writer.write('\npass')
    writer.set_tab(i + 1)
    writer.write('\n')
    for component in target[i:]:
        writer.write('class {name}(object):'.format(name=component))
        writer.tab()
        writer.write('\n')


def write_preamble(writer):
    writer.write('import json\n')
    writer.write('import os.path\n')
    writer.write('from avrogen.dict_wrapper import DictWrapper\n')
    writer.write('from avro import schema as avro_schema\n')


def write_read_file(writer):
    writer.write('\n\ndef __read_file(file_name):')
    with writer.indent():
        writer.write('\nwith open(file_name, "r") as f:')
        with writer.indent():
            writer.write('\nreturn f.read()')


def write_get_schema(writer):
    """
    Write get_schema_type
    :param writer:
    :return:
    """
    writer.write('\n__SCHEMAS = {}')
    writer.write('\ndef get_schema_type(fullname):')
    with writer.indent():
        writer.write('\nreturn __SCHEMAS.get(fullname)')


def write_reader_impl(record_types, writer):
    writer.write('\n\n\nclass SpecificDatumReader(DatumReader):')
    with writer.indent():
        writer.write('\nSCHEMA_TYPES = {')
        with writer.indent():
            for type in record_types:
                writer.write('\n"{type}": SchemaClasses.{type}Class,'.format(type=type))

        writer.write('\n}')
        writer.write('\ndef __init__(self, readers_schema=None, **kwargs):')
        with writer.indent():
            writer.write('\nsuper(SpecificDatumReader, self).__init__(readers_schema=readers_schema,**kwargs)')

        writer.write('\ndef read_record(self, writers_schema, readers_schema, decoder):')
        with writer.indent():
            writer.write(
                '\n\nresult = super(SpecificDatumReader, self).read_record(writers_schema, readers_schema, decoder)')
            writer.write('\n\nif readers_schema.fullname in SpecificDatumReader.SCHEMA_TYPES:')
            with writer.indent():
                writer.write('\nresult = SpecificDatumReader.SCHEMA_TYPES[readers_schema.fullname](result)')
            writer.write('\n\nreturn result')


def generate_namespace_modules(names, output_folder):
    ns_dict = {}
    for name in names:
        name_parts = name.split('.')
        full_path = output_folder
        for part in name_parts[:-1]:
            full_path = os.path.join(full_path, part)
            if not os.path.isdir(full_path):
                os.mkdir(full_path)
                # make sure __init__.py is created for every namespace level
                with open(os.path.join(full_path, "__init__.py"), "w+"): pass

        ns = '.'.join(name_parts[:-1])
        if not ns in ns_dict:
            ns_dict[ns] = []
        ns_dict[ns].append(name_parts[-1])
    return ns_dict


def write_schema_record(record, writer):
    """
    Writes class representing Avro record schema
    :param avro.schema.RecordSchema record:
    :param TabbedWriter writer:
    :return:
    """

    namespace, type_name = ns_.split_fullname(record.fullname)
    writer.write('''\nclass {name}Class(DictWrapper):'''.format(name=type_name))

    with writer.indent():
        writer.write("\n\n")
        writer.write('"""\n')
        writer.write(record.doc or '')
        writer.write('\n')
        writer.write('"""\n\n')

        writer.write('\nRECORD_SCHEMA = get_schema_type("%s")' % record.fullname)

        writer.write('\n\n\ndef __init__(self, inner_dict=None):')
        with writer.indent():
            writer.write('\n')
            writer.write('super(SchemaClasses.{name}Class, self).__init__(inner_dict)'.format(name=record.fullname))

            writer.write('\nif inner_dict is None:')
            with writer.indent():
                write_defaults(record, writer)
        write_fields(record, writer)


def write_enum(enum, writer):
    """
    Write class representing Avro enum schema
    :param schema.EnumSchema enum:
    :param TabbedWriter writer:
    :return:
    """
    writer.write('''\nclass {name}Class(object):'''.format(name=enum.name))

    with writer.indent():
        writer.write('\n\n')
        writer.write('"""\n')
        writer.write(enum.doc or '')
        writer.write('\n')
        writer.write('"""\n\n')

        for field in enum.symbols:
            writer.write('{name} = "{name}"\n'.format(name=field))

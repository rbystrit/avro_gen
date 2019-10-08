import os

from avro import schema
from . import namespace as ns_
from . import logical
import six
import keyword

if six.PY3:
    long = int


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
    'bytes': bytes,
    'string': str,
}

def clean_fullname(fullname):
    if six.PY3:
        return fullname.lstrip('.')
    return fullname


def convert_default(full_name, idx, do_json=True):
    if do_json:
        return ('_json_converter.from_json_object(SchemaClasses.{full_name}Class.RECORD_SCHEMA.fields[{idx}].default,'
               + ' writers_schema=SchemaClasses.{full_name}Class.RECORD_SCHEMA.fields[{idx}].type)').format(
            full_name=full_name, idx=idx
        )
    else:
        return ('SchemaClasses.{full_name}Class.RECORD_SCHEMA.fields[{idx}].default').format(
            full_name=full_name, idx=idx
        )


def write_defaults(record, writer, my_full_name=None, use_logical_types=False):
    """
    Write concrete record class's constructor part which initializes fields with default values
    :param schema.RecordSchema record: Avro RecordSchema whose class we are generating
    :param TabbedWriter writer: Writer to write to
    :param str my_full_name: Full name of the RecordSchema we are writing. Should only be provided for protocol requests.
    :return:
    """
    i = 0
    my_full_name = my_full_name or clean_fullname(record.fullname)

    something_written = False
    for field in record.fields:
        f_name = field.name
        if keyword.iskeyword(field.name):
            f_name =  field.name + get_field_type_name(field.type, use_logical_types)
        default_type, nullable = find_type_of_default(field.type)
        default_written = False
        if field.has_default:
            if use_logical_types and default_type.props.get('logicalType') \
                    and default_type.props.get('logicalType') in logical.DEFAULT_LOGICAL_TYPES:
                lt = logical.DEFAULT_LOGICAL_TYPES[default_type.props.get('logicalType')]

                writer.write(
                    '\nself.{name} = {value}'
                        .format(name=f_name,
                                value=lt.initializer(convert_default(my_full_name, idx=i,
                                                                     do_json=isinstance(default_type,
                                                                                        schema.RecordSchema)))))
                default_written = True
            elif isinstance(default_type, schema.RecordSchema):
                writer.write(
                    '\nself.{name} = SchemaClasses.{full_name}Class({default})'
                        .format(name=f_name, full_name=clean_fullname(default_type.fullname),
                                default=convert_default(idx=i, full_name=my_full_name, do_json=True)))
                default_written = True
            elif isinstance(default_type, (schema.PrimitiveSchema, schema.EnumSchema, schema.FixedSchema)):
                writer.write('\nself.{name} = {default}'
                             .format(name=f_name, default=convert_default(full_name=my_full_name, idx=i,
                                                                              do_json=False)))
                default_written = True

        if not default_written:
            default_written = True
            if nullable:
                writer.write('\nself.{name} = None'.format(name=f_name))
            elif use_logical_types and default_type.props.get('logicalType') \
                    and default_type.props.get('logicalType') in logical.DEFAULT_LOGICAL_TYPES:
                lt = logical.DEFAULT_LOGICAL_TYPES[default_type.props.get('logicalType')]
                writer.write('\nself.{name} = {default}'.format(name=f_name,
                                                                default=lt.initializer()))
            elif isinstance(default_type, schema.PrimitiveSchema) and not default_type.props.get('logicalType'):
                writer.write('\nself.{name} = {default}'.format(name=f_name,
                                                                default=get_primitive_field_initializer(default_type)))
            elif isinstance(default_type, schema.EnumSchema):
                writer.write('\nself.{name} = SchemaClasses.{full_name}Class.{sym}'
                             .format(name=f_name, full_name=clean_fullname(default_type.fullname),
                                     sym=default_type.symbols[0]))
            elif isinstance(default_type, schema.MapSchema):
                writer.write('\nself.{name} = dict()'.format(name=f_name))
            elif isinstance(default_type, schema.ArraySchema):
                writer.write('\nself.{name} = list()'.format(name=f_name))
            elif isinstance(default_type, schema.FixedSchema):
                writer.write('\nself.{name} = str()'.format(name=f_name))
            elif isinstance(default_type, schema.RecordSchema):
                writer.write('\nself.{name} = SchemaClasses.{full_name}Class()'
                             .format(name=f_name, full_name=default_type.fullname))
            else:
                default_written = False
        something_written = something_written or default_written
        i += 1
    if not something_written:
        writer.write('\npass')


def write_fields(record, writer, use_logical_types):
    """
    Write field definitions for a given RecordSchema
    :param schema.RecordSchema record: Avro RecordSchema we are generating
    :param TabbedWriter writer: Writer to write to
    :return:
    """
    writer.write('\n\n')
    for field in record.fields:  # type: schema.Field
        write_field(field, writer, use_logical_types)


def write_field(field, writer, use_logical_types):
    """
    Write a single field definition
    :param field:
    :param writer:
    :return:
    """
    name = field.name
    if keyword.iskeyword(field.name):
        name =  field.name + get_field_type_name(field.type, use_logical_types)
    writer.write('''
@property
def {name}(self) -> {ret_type_name}:
    """
    :rtype: {ret_type_name}
    """
    return self._inner_dict.get('{name}')

@{name}.setter
def {name}(self, value: {ret_type_name}):
    #"""
    #:param {ret_type_name} value:
    #"""
    self._inner_dict['{name}'] = value

'''.format(name=name, ret_type_name=get_field_type_name(field.type, use_logical_types)))


def get_primitive_field_initializer(field_schema):
    """
    Gets a python code string which represents a type initializer for a primitive field.
    Used for required fields where no default is provided. Output will look like "int()" or similar
    :param schema.PrimitiveSchema field_schema:
    :return:
    """

    if field_schema.type == 'null':
        return 'None'
    return get_field_type_name(field_schema, False) + "()"


def get_field_type_name(field_schema, use_logical_types):
    """
    Gets a python type-hint for a given schema
    :param schema.Schema field_schema:
    :return: String containing python type hint
    """
    if use_logical_types and field_schema.props.get('logicalType'):
        from avrogen.logical import DEFAULT_LOGICAL_TYPES
        lt = DEFAULT_LOGICAL_TYPES.get(field_schema.props.get('logicalType'))
        if lt:
            return lt.typename()

    if isinstance(field_schema, schema.PrimitiveSchema):
        if field_schema.fullname == 'null':
            return ''
        return __PRIMITIVE_TYPE_MAPPING[field_schema.fullname].__name__
    elif isinstance(field_schema, schema.FixedSchema):
        return 'bytes'
    elif isinstance(field_schema, schema.NamedSchema):
        return 'SchemaClasses.' + field_schema.fullname + 'Class'
    elif isinstance(field_schema, schema.ArraySchema):
        return 'list[' + get_field_type_name(field_schema.items, use_logical_types) + ']'
    elif isinstance(field_schema, schema.MapSchema):
        return 'dict[str, ' + get_field_type_name(field_schema.values, use_logical_types) + ']'
    elif isinstance(field_schema, schema.UnionSchema):
        type_names = [get_field_type_name(x, use_logical_types) for x in field_schema.schemas if
                      get_field_type_name(x, use_logical_types)]
        if len(type_names) > 1:
            return ' | '.join(type_names)
        elif len(type_names) == 1:
            return type_names[0]
        return ''


def find_type_of_default(field_type):
    """
    Returns full name of an avro type of the field's default value
    :param schema.Schema field_type:
    :return:
    """

    if isinstance(field_type, schema.UnionSchema):
        non_null_types = [s for s in field_type.schemas if s.type != 'null']
        if non_null_types:
            type_, nullable = find_type_of_default(non_null_types[0])
            nullable = nullable or any(
                f for f in field_type.schemas if isinstance(f, schema.PrimitiveSchema) and f.fullname == 'null')
        else:
            type_, nullable = field_type.schemas[0], True
        return type_, nullable
    elif isinstance(field_type, schema.PrimitiveSchema):
        return field_type, field_type.fullname == 'null'
    else:
        return field_type, False


def start_namespace(current, target, writer):
    """
    Writes a new class corresponding to the target namespace to the schema file and
     closes the prior namespace
    :param tuple[str] current: Current namespace
    :param tuple[str] target: Target namespace we need to generate classes for
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


def write_preamble(writer, use_logical_types, custom_imports):
    """
    Writes a preamble of the file containing schema classes
    :param  writer:
    :return:
    """
    writer.write('import json\n')
    writer.write('import os.path\n')
    writer.write('import decimal\n')
    writer.write('import datetime\n')
    writer.write('import six\n')

    for cs in (custom_imports or []):
        writer.write('import %s\n' % cs)
    writer.write('from avrogen.dict_wrapper import DictWrapper\n')
    writer.write('from avrogen import avrojson\n')
    if use_logical_types:
        writer.write('from avrogen import logical\n')
    writer.write('from avro import schema as avro_schema\n')
    writer.write('if six.PY3:')
    writer.tab()
    writer.write('    from avro.schema import SchemaFromJSONData as make_avsc_object\n')
    writer.untab()
    writer.write('\nelse:\n')
    writer.tab()
    writer.write('    from avro.schema import make_avsc_object\n')
    writer.untab()
    writer.write('\n')


def write_read_file(writer):
    """
    Write a function which reads our schema or protocol
    :param writer:
    :return:
    """
    writer.write('\n\ndef __read_file(file_name):')
    with writer.indent():
        writer.write('\nwith open(file_name, "r") as f:')
        with writer.indent():
            writer.write('\nreturn f.read()')


def write_get_schema(writer):
    """
    Write get_schema_type which is used by concrete classes to resolve their own RecordSchemas
    :param writer:
    :return:
    """
    writer.write('\n__SCHEMAS = {}')
    writer.write('\ndef get_schema_type(fullname):')
    with writer.indent():
        writer.write('\nreturn __SCHEMAS.get(fullname)')


def write_reader_impl(record_types, writer, use_logical_types):
    """
    Write specific reader implementation
    :param list[schema.RecordSchema] record_types:
    :param writer:
    :return:
    """
    writer.write('\n\n\nclass SpecificDatumReader(%s):' % (
        'DatumReader' if not use_logical_types else 'logical.LogicalDatumReader'))
    with writer.indent():
        writer.write('\nSCHEMA_TYPES = {')
        with writer.indent():
            for type in record_types:
                writer.write('\n"{type}": SchemaClasses.{type}Class,'.format(type=type))

        writer.write('\n}')
        writer.write('\ndef __init__(self, readers_schema=None, **kwargs):')
        with writer.indent():
            writer.write('\nwriters_schema = kwargs.pop("writers_schema", readers_schema)')
            writer.write('\nwriters_schema = kwargs.pop("writer_schema", writers_schema)')
            writer.write('\nsuper(SpecificDatumReader, self).__init__(writers_schema, readers_schema, **kwargs)')

        writer.write('\ndef read_record(self, writers_schema, readers_schema, decoder):')
        with writer.indent():
            writer.write(
                '\n\nresult = super(SpecificDatumReader, self).read_record(writers_schema, readers_schema, decoder)')
            writer.write('\n\nif readers_schema.fullname in SpecificDatumReader.SCHEMA_TYPES:')
            with writer.indent():
                writer.write('\nresult = SpecificDatumReader.SCHEMA_TYPES[readers_schema.fullname](result)')
            writer.write('\n\nreturn result')


def generate_namespace_modules(names, output_folder):
    """
    Generate python modules corresponding to schema/protocol namespaces.

    :param names:
    :param output_folder:
    :return: Dictinoary of (namespace, list(name))
    :rtype: dict[str, list[str]]
    """
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


def write_schema_record(record, writer, use_logical_types):
    """
    Writes class representing Avro record schema
    :param avro.schema.RecordSchema record:
    :param TabbedWriter writer:
    :return:
    """

    fullname = clean_fullname(record.fullname)
    namespace, type_name = ns_.split_fullname(record.fullname)
    writer.write('''\nclass {name}Class(DictWrapper):'''.format(name=type_name))

    with writer.indent():
        writer.write("\n\n")
        writer.write('"""\n')
        writer.write(record.doc or '')
        writer.write('\n')
        writer.write('"""\n\n')

        writer.write('\nRECORD_SCHEMA = get_schema_type("%s")' % fullname)

        writer.write('\n\n\ndef __init__(self, inner_dict=None):')
        with writer.indent():
            writer.write('\n')
            writer.write('super(SchemaClasses.{name}Class, self).__init__(inner_dict)'.format(name=fullname))

            writer.write('\nif inner_dict is None:')
            with writer.indent():
                write_defaults(record, writer, use_logical_types=use_logical_types)
        write_fields(record, writer, use_logical_types)


def write_enum(enum, writer):
    """
    Write class representing Avro enum schema
    :param schema.EnumSchema enum:
    :param TabbedWriter writer:
    :return:
    """
    fullname = clean_fullname(enum.fullname)
    namespace, type_name = ns_.split_fullname(enum.fullname)
    writer.write('''\nclass {name}Class(object):'''.format(name=type_name))

    with writer.indent():
        writer.write('\n\n')
        writer.write('"""\n')
        writer.write(enum.doc or '')
        writer.write('\n')
        writer.write('"""\n\n')

        for field in enum.symbols:
            writer.write('{name} = "{name}"\n'.format(name=field))

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
    'null': None,
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
        return (f'_json_converter.from_json_object({full_name}Class.RECORD_SCHEMA.fields[{idx}].default,'
               + f' writers_schema={full_name}Class.RECORD_SCHEMA.fields[{idx}].type)')
    else:
        return f'self.RECORD_SCHEMA.field_map["{idx}"].default'


def get_default(field, use_logical_types, my_full_name=None, f_name=None):
    default_written = False
    f_name = f_name if f_name is not None else field.name
    if keyword.iskeyword(field.name):
        f_name =  field.name + get_field_type_name(field.type, use_logical_types)

    default_type, nullable = find_type_of_default(field.type)
    if field.has_default:
        if use_logical_types and default_type.props.get('logicalType') \
                and default_type.props.get('logicalType') in logical.DEFAULT_LOGICAL_TYPES:
            lt = logical.DEFAULT_LOGICAL_TYPES[default_type.props.get('logicalType')]
            v = lt.initializer(convert_default(my_full_name,
                                                idx=i,
                                                do_json=isinstance(default_type,
                                                schema.RecordSchema)))
            return v
        elif isinstance(default_type, schema.RecordSchema):
            d = convert_default(idx=i, full_name=my_full_name, do_json=True)
            return f'{field.name}Class({d})'
        elif isinstance(default_type, (schema.PrimitiveSchema, schema.EnumSchema, schema.FixedSchema)):
            d = convert_default(full_name=my_full_name, idx=f_name, do_json=False)
            return d

    if not default_written:
        default_written = True
        if nullable:
            return 'None'
        elif use_logical_types and default_type.props.get('logicalType') \
                and default_type.props.get('logicalType') in logical.DEFAULT_LOGICAL_TYPES:
            lt = logical.DEFAULT_LOGICAL_TYPES[default_type.props.get('logicalType')]
            return f'{default}'.format(default=lt.initializer())
        elif isinstance(default_type, schema.PrimitiveSchema) and not default_type.props.get('logicalType'):
            d = get_primitive_field_initializer(default_type)
            return d
        elif isinstance(default_type, schema.EnumSchema):
            f = clean_fullname(default_type.name)
            s = default_type.symbols[0]
            return f'{f}Class.{s}'
        elif isinstance(default_type, schema.MapSchema):
            return 'dict()'
        elif isinstance(default_type, schema.ArraySchema):
            return 'list()'
        elif isinstance(default_type, schema.FixedSchema):
            return 'bytes()'
        elif isinstance(default_type, schema.RecordSchema):
            f = clean_fullname(default_type.name)
            return f'{f}Class()'
    raise AttributeError('cannot get default for field')

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
        default = get_default(field, use_logical_types, my_full_name=my_full_name, f_name=f_name)
        writer.write(f'\nself.{f_name} = {default}')
        something_written = True
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

def get_field_name(field, use_logical_types):
    name = field.name
    if keyword.iskeyword(field.name):
        name =  field.name + get_field_type_name(field.type, use_logical_types)
    return name

def write_field(field, writer, use_logical_types):
    """
    Write a single field definition
    :param field:
    :param writer:
    :return:
    """
    name = get_field_name(field, use_logical_types)
    doc = field.doc
    get_docstring = f'"""Getter: {doc}"""' if doc else "# No docs available."
    set_docstring = f'"""Setter: {doc}"""' if doc else "# No docs available."
    writer.write('''
@property
def {name}(self) -> {ret_type_name}:
    {get_docstring}
    return self._inner_dict.get('{raw_name}')  # type: ignore


@{name}.setter
def {name}(self, value: {ret_type_name}) -> None:
    {set_docstring}
    self._inner_dict['{raw_name}'] = value

'''.format(name=name, get_docstring=get_docstring, set_docstring=set_docstring, raw_name=field.name, ret_type_name=get_field_type_name(field.type, use_logical_types)))


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
            return 'None'
        return __PRIMITIVE_TYPE_MAPPING[field_schema.fullname].__name__
    elif isinstance(field_schema, schema.FixedSchema):
        return 'bytes'
    elif isinstance(field_schema, schema.EnumSchema):
        # For enums, we have their "class" types, but they're actually
        # represented as strings. This is a decent hack to work around
        # the issue.
        return f'Union[str, "{field_schema.name}Class"]'
    elif isinstance(field_schema, schema.NamedSchema):
        return f'"{field_schema.name}Class"'
    elif isinstance(field_schema, schema.ArraySchema):
        return 'List[' + get_field_type_name(field_schema.items, use_logical_types) + ']'
    elif isinstance(field_schema, schema.MapSchema):
        return 'Dict[str, ' + get_field_type_name(field_schema.values, use_logical_types) + ']'
    elif isinstance(field_schema, schema.UnionSchema):
        type_names = [get_field_type_name(x, use_logical_types) for x in field_schema.schemas if
                      get_field_type_name(x, use_logical_types)]
        if len(type_names) > 1:
            return 'Union[' + ', '.join(type_names) + ']'
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
        # For union types, the default is always the first item.
        field, nullable = find_type_of_default(field_type.schemas[0])
        return field, nullable
        # non_null_types = [s for s in field_type.schemas if s.type != 'null']
        # if non_null_types:
        #     type_, nullable = find_type_of_default(non_null_types[0])
        #     nullable = nullable or any(
        #         f for f in field_type.schemas if isinstance(f, schema.PrimitiveSchema) and f.fullname == 'null')
        # else:
        #     type_, nullable = field_type.schemas[0], True
        # return type_, nullable
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

    writer.write('\n\n')
    writer.set_tab(0)
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
        writer.write(f'import {cs}\n')
    writer.write('from avrogen.dict_wrapper import DictWrapper\n')
    writer.write('from avrogen import avrojson\n')
    if use_logical_types:
        writer.write('from avrogen import logical\n')
    writer.write('from avro.schema import RecordSchema, SchemaFromJSONData as make_avsc_object\n')
    writer.write('from avro import schema as avro_schema\n')
    writer.write('from typing import List, Dict, Union, Optional, overload\n')
    writer.write('\n')


def write_read_file(writer):
    """
    Write a function which reads our schema or protocol
    :param writer:
    :return:
    """
    writer.write('\ndef __read_file(file_name):')
    with writer.indent():
        writer.write('\nwith open(file_name, "r") as f:')
        with writer.indent():
            writer.write('\nreturn f.read()\n')


def write_get_schema(writer):
    """
    Write get_schema_type which is used by concrete classes to resolve their own RecordSchemas
    :param writer:
    :return:
    """
    writer.write('\n__SCHEMAS: Dict[str, RecordSchema] = {}\n\n\n')
    writer.write('def get_schema_type(fullname):')
    with writer.indent():
        writer.write('\nreturn __SCHEMAS.get(fullname)\n\n')


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
            for t in record_types:
                t_class = t.split('.')[-1]
                writer.write('\n"{t_class}": {t_class}Class,'.format(t_class=t_class))
                writer.write('\n".{t_class}": {t_class}Class,'.format(t_class=t_class))
                writer.write('\n"{f_class}": {t_class}Class,'.format(t_class=t_class, f_class=t))

        writer.write('\n}')
        writer.write('\n\n\ndef __init__(self, readers_schema=None, **kwargs):')
        with writer.indent():
            writer.write('\nwriters_schema = kwargs.pop("writers_schema", readers_schema)')
            writer.write('\nwriters_schema = kwargs.pop("writer_schema", writers_schema)')
            writer.write('\nsuper(SpecificDatumReader, self).__init__(writers_schema, readers_schema, **kwargs)')

        writer.write('\n\n\ndef read_record(self, writers_schema, readers_schema, decoder):')
        with writer.indent():
            writer.write(
                '\nresult = super(SpecificDatumReader, self).read_record(writers_schema, readers_schema, decoder)')
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

    _, type_name = ns_.split_fullname(record.fullname)
    writer.write('''\nclass {name}Class(DictWrapper):'''.format(name=type_name))

    with writer.indent():
        writer.write('\n')
        if record.doc:
            writer.write(f'"""{record.doc}"""')
        else:
            writer.write('# No docs available.')
        writer.write('\n\nRECORD_SCHEMA = get_schema_type("%s")' % (record.namespace + '.' + record.name))

        write_record_init(record, writer, use_logical_types)

        write_serialization_stubs(record, writer, use_logical_types)

        write_fields(record, writer, use_logical_types)


def write_record_init(record, writer, use_logical_types):
    writer.write('\n\n@overload')
    writer.write('\ndef __init__(self,')
    with writer.indent():
        delayed_lines = []
        for field in record.fields:  # type: schema.Field
            name = get_field_name(field, use_logical_types)
            ret_type_name = get_field_type_name(field.type, use_logical_types)
            default_type, nullable = find_type_of_default(field.type)

            if nullable:
                delayed_lines.append(f'\n{name}: {ret_type_name}=None,')
            else:
                writer.write(f'\n{name}: {ret_type_name},')
            # default = get_default(field, use_logical_types)
            # writer.write(f'\n{name}: {ret_type_name} = {default},')
        for line in delayed_lines:
            writer.write(line)
    writer.write('\n):')
    with writer.indent():
        writer.write('\n...')

    writer.write('\n\n@overload')
    writer.write('\ndef __init__(self, _inner_dict: Optional[dict]=None):')
    with writer.indent():
        writer.write('\n...')

    writer.write('\n\ndef __init__(self, _inner_dict=None, **kwargs):')
    with writer.indent():
        writer.write('\n')
        writer.write('super({name}Class, self).__init__({{}})'.format(name=record.name))

        write_defaults(record, writer, use_logical_types=use_logical_types)

        writer.write('\nif _inner_dict is not None:')
        with writer.indent():
            writer.write('\nfor key, value in _inner_dict.items():')
            with writer.indent():
                writer.write('\ngetattr(self, key)')
                writer.write('\nsetattr(self, key, value)')
        writer.write('\nfor key, value in kwargs.items():')
        with writer.indent():
            writer.write('\nif value is not None:')
            with writer.indent():
                writer.write('\ngetattr(self, key)')
                writer.write('\nsetattr(self, key, value)')


def write_serialization_stubs(record, writer, use_logical_types):
    # writer.write(f'\n\n@classmethod')
    # writer.write(f'\ndef from_obj(cls, obj: dict, tuples=False) -> "{record.name}Class":')
    # with writer.indent():
    #     writer.write('\n...')

    # writer.write(f'\n\ndef to_obj(self, tuples=False) -> dict:')
    # with writer.indent():
    #     writer.write('\n...')
    pass


def write_enum(enum, writer):
    """
    Write class representing Avro enum schema
    :param schema.EnumSchema enum:
    :param TabbedWriter writer:
    :return:
    """
    _, type_name = ns_.split_fullname(enum.fullname)
    writer.write('''\nclass {name}Class(object):'''.format(name=type_name))

    with writer.indent():
        writer.write('\n')
        if enum.doc:
            writer.write(f'"""{enum.doc}"""')
        else:
            writer.write('# No docs available.')

        writer.write('\n\n')
        for field in enum.symbols:
            writer.write('{name} = "{name}"\n'.format(name=field))
        writer.write('\n')

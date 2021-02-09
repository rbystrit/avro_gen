
import json
import os
import six
from avro import schema

from io import StringIO


from avro.schema import SchemaFromJSONData as make_avsc_object

from .core_writer import generate_namespace_modules, clean_fullname
from .tabbed_writer import TabbedWriter
from .core_writer import write_preamble, start_namespace, write_schema_record, write_enum, write_read_file
from .core_writer import write_get_schema, write_reader_impl
import logging

logger = logging.getLogger('avrogen.schema')
logger.setLevel(logging.INFO)


def generate_schema(schema_json, use_logical_types=False, custom_imports=None, avro_json_converter=None):
    """
    Generate file containing concrete classes for RecordSchemas in given avro schema json
    :param str schema_json: JSON representing avro schema
    :param list[str] custom_imports: Add additional import modules
    :param str avro_json_converter: AvroJsonConverter type to use for default values
    :return Dict[str, str]:
    """

    if avro_json_converter is None:
        avro_json_converter = 'avrojson.AvroJsonConverter'

    if '(' not in avro_json_converter:
        avro_json_converter += f'(use_logical_types={use_logical_types}, schema_types=__SCHEMA_TYPES)'

    custom_imports = custom_imports or []
    names = schema.Names()
    make_avsc_object(json.loads(schema_json), names)

    names = [k for k in six.iteritems(names.names) if isinstance(k[1], (schema.RecordSchema, schema.EnumSchema))]
    names = sorted(names, key=lambda x: x[0])

    main_out = StringIO()
    writer = TabbedWriter(main_out)

    write_preamble(writer, use_logical_types, custom_imports)
    write_schema_preamble(writer)
    write_get_schema(writer)
    write_populate_schemas(writer)

    current_namespace = tuple()

    for name, field_schema in names:  # type: str, schema.Schema
        name = clean_fullname(name)
        namespace = tuple(name.split('.')[:-1])
        if namespace != current_namespace:
            current_namespace = namespace
        if isinstance(field_schema, schema.RecordSchema):
            logger.debug(f'Writing schema: {clean_fullname(field_schema.fullname)}')
            write_schema_record(field_schema, writer, use_logical_types)
        elif isinstance(field_schema, schema.EnumSchema):
            logger.debug(f'Writing enum: {field_schema.fullname}', field_schema.fullname)
            write_enum(field_schema, writer)
    writer.set_tab(0)
    writer.write('\n__SCHEMA_TYPES = {')
    writer.tab()

    # Lookup table for fullname.
    for name, field_schema in names:
        n = clean_fullname(field_schema.name)
        full = field_schema.fullname
        writer.write(f"\n'{full}': {n}Class,")

    # Lookup table for names without namespace.
    for name, field_schema in names:
        n = clean_fullname(field_schema.name)
        writer.write(f"\n'{n}': {n}Class,")

    writer.untab()
    writer.write('\n}\n\n')

    writer.write(f'_json_converter = {avro_json_converter}\n\n')

    value = main_out.getvalue()
    main_out.close()
    return value, [clean_fullname(name[0]) for name in names]


def write_schema_preamble(writer):
    """
    Writes a schema-specific preamble: __get_names_and_schema() which is used by concrete classes to resolve
    their own RecordSchema
    :param writer:
    :return:
    """
    write_read_file(writer)
    writer.write('\n\ndef __get_names_and_schema(json_str):')
    with writer.indent():
        writer.write('\nnames = avro_schema.Names()')
        writer.write('\nschema = make_avsc_object(json.loads(json_str), names)')
        writer.write('\nreturn names, schema')
    writer.write('\n\n\nSCHEMA_JSON_STR = __read_file(os.path.join(os.path.dirname(__file__), "schema.avsc"))')
    writer.write('\n\n\n__NAMES, SCHEMA = __get_names_and_schema(SCHEMA_JSON_STR)')


def write_populate_schemas(writer):
    """
    Writes out a __SCHEMAS dict which contains all RecordSchemas by their full name. Used by get_schema_type
    :param writer:
    :return:
    """
    writer.write('\n__SCHEMAS = dict((n.fullname.lstrip("."), n) for n in six.itervalues(__NAMES.names))\n')


def write_namespace_modules(ns_dict, output_folder):
    """
    Writes content of the generated namespace modules. A python module will be created for each namespace
    and will import concrete schema classes from SchemaClasses
    :param ns_dict:
    :param output_folder:
    :return:
    """
    for ns in six.iterkeys(ns_dict):
        with open(os.path.join(output_folder, ns.replace('.', os.path.sep), "__init__.py"), "w+") as f:
            currency = '.'
            if ns != '':
                currency += '.' * len(ns.split('.'))
            for name in ns_dict[ns]:
                f.write(f'from {currency}schema_classes import {name}Class\n')

            f.write('\n\n')

            for name in ns_dict[ns]:
                f.write(f"{name} = {name}Class\n")


def write_specific_reader(record_types, output_folder, use_logical_types):
    """
    Writes specific reader for a avro schema into generated root module
    :param record_types:
    :param output_folder:
    :return:
    """
    with open(os.path.join(output_folder, "__init__.py"), "a+") as f:
        writer = TabbedWriter(f)
        writer.write('from .schema_classes import SCHEMA as get_schema_type')
        writer.write('\nfrom .schema_classes import _json_converter as json_converter')
        for t in record_types:
            writer.write(f'\nfrom .schema_classes import {t.split(".")[-1]}Class')
        writer.write('\nfrom avro.io import DatumReader')
        if use_logical_types:
            writer.write('\nfrom avrogen import logical')

        write_reader_impl(record_types, writer, use_logical_types)


def write_schema_files(schema_json, output_folder, use_logical_types=False, custom_imports=None):
    """
    Generates concrete classes, namespace modules, and a SpecificRecordReader for a given avro schema
    :param str schema_json: JSON containing avro schema
    :param str output_folder: Folder in which to create generated files
    :param list[str] custom_imports: Add additional import modules
    :return:
    """
    schema_py, names = generate_schema(schema_json, use_logical_types, custom_imports)
    names = sorted(names)

    if not os.path.isdir(output_folder):
        os.mkdir(output_folder)

    with open(os.path.join(output_folder, "schema_classes.py"), "w+") as f:
        f.write(schema_py)

    with open(os.path.join(output_folder, "schema.avsc"), "w+") as f:
        f.write(schema_json)

    ns_dict = generate_namespace_modules(names, output_folder)

    with open(os.path.join(output_folder, "__init__.py"), "w+") as f:
        pass  # make sure we create this file from scratch

    write_namespace_modules(ns_dict, output_folder)
    write_specific_reader(names, output_folder, use_logical_types)

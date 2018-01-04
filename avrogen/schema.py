import cStringIO as StringIO
import json
import os

from avro import schema

from .core_writer import generate_namespace_modules
from .tabbed_writer import TabbedWriter
from .core_writer import write_preamble, start_namespace, write_schema_record, write_enum, write_read_file
from .core_writer import write_get_schema, write_reader_impl

import logging

logger = logging.getLogger('avrogen.schema')
logger.setLevel(logging.INFO)


def generate_schema(schema_json, use_logical_types=False, custom_imports=None):
    """
    Generate file containing concrete classes for RecordSchemas in given avro schema json
    :param str schema_json: JSON representing avro schema
    :param list[str] custom_imports: Add additional import modules
    :return Dict[str, str]:
    """

    custom_imports = custom_imports or []
    names = schema.Names()
    schema.make_avsc_object(json.loads(schema_json), names)

    names = [k for k in names.names.iteritems() if isinstance(k[1], (schema.RecordSchema, schema.EnumSchema))]
    names = sorted(names, lambda x, y: cmp(x[0], y[0]))

    main_out = StringIO.StringIO()
    writer = TabbedWriter(main_out)

    write_preamble(writer, use_logical_types, custom_imports)
    write_schema_preamble(writer)
    write_get_schema(writer)
    write_populate_schemas(writer)

    writer.write('\n\n\nclass SchemaClasses(object):')
    writer.tab()
    writer.write('\n\n')

    current_namespace = tuple()

    for name, field_schema in names:  # type: str, schema.Schema
        namespace = tuple(name.split('.')[:-1])

        if namespace != current_namespace:
            start_namespace(current_namespace, namespace, writer)
            current_namespace = namespace
        if isinstance(field_schema, schema.RecordSchema):
            logger.debug('Writing schema: %s', field_schema.fullname)
            write_schema_record(field_schema, writer, use_logical_types)
        elif isinstance(field_schema, schema.EnumSchema):
            logger.debug('Writing enum: %s', field_schema.fullname)
            write_enum(field_schema, writer)
    writer.write('\npass\n')
    value = main_out.getvalue()
    main_out.close()
    return value, [name[0] for name in names]


def write_schema_preamble(writer):
    """
    Writes a schema-specific preamble: __get_names_and_schema() which is used by concrete classes to resolve
    their own RecordSchema
    :param writer:
    :return:
    """
    write_read_file(writer)
    writer.write('\n\ndef __get_names_and_schema(file_name):')
    with writer.indent():
        writer.write('\nnames = avro_schema.Names()')
        writer.write('\nschema = avro_schema.make_avsc_object(json.loads(__read_file(file_name)), names)')
        writer.write('\nreturn names, schema')
    writer.write('\n\n__NAMES, SCHEMA = __get_names_and_schema(os.path.join(os.path.dirname(__file__), "schema.avsc"))')


def write_populate_schemas(writer):
    """
    Writes out a __SCHEMAS dict which contains all RecordSchemas by their full name. Used by get_schema_type
    :param writer:
    :return:
    """
    writer.write('\n__SCHEMAS = dict((n.fullname, n) for n in __NAMES.names.itervalues())')


def write_namespace_modules(ns_dict, output_folder):
    """
    Writes content of the generated namespace modules. A python module will be created for each namespace
    and will import concrete schema classes from SchemaClasses
    :param ns_dict:
    :param output_folder:
    :return:
    """
    for ns in ns_dict.iterkeys():
        with open(os.path.join(output_folder, ns.replace('.', os.path.sep), "__init__.py"), "w+") as f:
            currency = '.'
            if ns != '':
                currency += '.' * len(ns.split('.'))
            f.write('from {currency}schema_classes import SchemaClasses\n'.format(currency=currency))
            for name in ns_dict[ns]:
                f.write("{name} = SchemaClasses.{ns}{name}Class\n".format(name=name, ns=ns if not ns else (ns + ".")))


def write_specific_reader(record_types, output_folder, use_logical_types):
    """
    Writes specific reader for a avro schema into generated root module
    :param record_types:
    :param output_folder:
    :return:
    """
    with open(os.path.join(output_folder, "__init__.py"), "a+") as f:
        writer = TabbedWriter(f)
        writer.write('\n\nfrom .schema_classes import SchemaClasses, SCHEMA as my_schema, get_schema_type')
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

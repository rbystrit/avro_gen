import six
import os

if six.PY3:
    from io import StringIO
else:
    from cStringIO import StringIO

from avro import protocol, schema

from . import namespace as ns_
from .tabbed_writer import TabbedWriter
from .core_writer import write_preamble, write_get_schema, start_namespace, write_reader_impl, clean_fullname
from .core_writer import write_schema_record, write_enum, write_read_file, generate_namespace_modules
from .protocol_writer import write_protocol_request


def generate_protocol(protocol_json, use_logical_types=False, custom_imports=None, avro_json_converter=None):
    """
    Generate content of the file which will contain concrete classes for RecordSchemas and requests contained
    in the avro protocol
    :param str protocol_json: JSON containing avro protocol
    :param bool use_logical_types: Use logical types extensions if true
    :param list[str] custom_imports: Add additional import modules
    :param str avro_json_converter: AvroJsonConverter type to use for default values
    :return:
    """

    if avro_json_converter is None:
        avro_json_converter = 'avrojson.AvroJsonConverter'

    if '(' not in avro_json_converter:
        avro_json_converter += '(use_logical_types=%s, schema_types=__SCHEMA_TYPES)' % use_logical_types

    custom_imports = custom_imports or []

    if not hasattr(protocol, 'parse'):
        # Older versions of avro used a capital P in Parse.
        proto = protocol.Parse(protocol_json)
    else:
        proto = protocol.parse(protocol_json)

    schemas = []
    messages = []
    schema_names = set()
    request_names = set()

    known_types = set()
    for schema_idx, record_schema in enumerate(proto.types):
        if isinstance(record_schema, (schema.RecordSchema, schema.EnumSchema)):
            schemas.append((schema_idx, record_schema))
            known_types.add(clean_fullname(record_schema.fullname))

    for message in (six.itervalues(proto.messages) if six.PY2 else proto.messages):
        messages.append((message, message.request, message.response if isinstance(message.response, (
            schema.EnumSchema, schema.RecordSchema)) and clean_fullname(message.response.fullname) not in known_types else None))
        if isinstance(message.response, (schema.EnumSchema, schema.RecordSchema)):
            known_types.add(clean_fullname(message.response.fullname))

    namespaces = {}
    for schema_idx, record_schema in schemas:
        ns, name = ns_.split_fullname(clean_fullname(record_schema.fullname))
        if ns not in namespaces:
            namespaces[ns] = {'requests': [], 'records': [], 'responses': []}
        namespaces[ns]['records'].append((schema_idx, record_schema))

    for message, request, response in messages:
        fullname = ns_.make_fullname(proto.namespace, clean_fullname(message.name))
        ns, name = ns_.split_fullname(fullname)
        if ns not in namespaces:
            namespaces[ns] = {'requests': [], 'records': [], 'responses': []}
        namespaces[ns]['requests'].append(message)
        if response:
            namespaces[ns]['responses'].append(message)

    main_out = StringIO()
    writer = TabbedWriter(main_out)

    write_preamble(writer, use_logical_types, custom_imports)
    write_protocol_preamble(writer, use_logical_types, custom_imports)
    write_get_schema(writer)
    write_populate_schemas(writer)

    writer.write('\n\n\nclass SchemaClasses(object):')
    with writer.indent():
        writer.write('\n\n')

        current_namespace = tuple()
        all_ns = sorted(namespaces.keys())

        for ns in all_ns:
            if not (namespaces[ns]['responses'] or namespaces[ns]['records']):
                continue

            namespace = ns.split('.')
            if namespace != current_namespace:
                start_namespace(current_namespace, namespace, writer)

            for idx, record in namespaces[ns]['records']:
                schema_names.add(clean_fullname(record.fullname))
                if isinstance(record, schema.RecordSchema):
                    write_schema_record(record, writer, use_logical_types)
                elif isinstance(record, schema.EnumSchema):
                    write_enum(record, writer)

            for message in namespaces[ns]['responses']:
                schema_names.add(clean_fullname(message.response.fullname))
                if isinstance(message.response, schema.RecordSchema):
                    write_schema_record(message.response, writer, use_logical_types)
                elif isinstance(message.response, schema.EnumSchema):
                    write_enum(message.response, writer)

        writer.write('\n\npass')

    writer.set_tab(0)
    writer.write('\n\n\nclass RequestClasses(object):')
    with writer.indent() as indent:
        writer.write('\n\n')

        current_namespace = tuple()
        all_ns = sorted(namespaces.keys())

        for ns in all_ns:
            if not (namespaces[ns]['requests'] or namespaces[ns]['responses']):
                continue

            namespace = ns.split('.')
            if namespace != current_namespace:
                start_namespace(current_namespace, namespace, writer)

            for message in namespaces[ns]['requests']:
                request_names.add(ns_.make_fullname(proto.namespace, clean_fullname(message.name)))
                write_protocol_request(message, proto.namespace, writer, use_logical_types)

        writer.write('\n\npass')

    writer.untab()
    writer.set_tab(0)
    writer.write('\n__SCHEMA_TYPES = {\n')
    writer.tab()

    all_ns = sorted(namespaces.keys())
    for ns in all_ns:
        for idx, record in (namespaces[ns]['records'] or []):
            writer.write("'%s': SchemaClasses.%sClass,\n" % (clean_fullname(record.fullname),
                                                             clean_fullname(record.fullname)))

        for message in (namespaces[ns]['responses'] or []):
            writer.write("'%s': SchemaClasses.%sClass,\n" % (clean_fullname(message.response.fullname),
                                                             clean_fullname(message.response.fullname)))

        for message in (namespaces[ns]['requests'] or []):
            name = ns_.make_fullname(proto.namespace, clean_fullname(message.name))
            writer.write("'%s': RequestClasses.%sRequestClass, \n" % (name, name))

    writer.untab()
    writer.write('\n}\n')

    writer.write('_json_converter = %s\n\n' % avro_json_converter)
    value = main_out.getvalue()
    main_out.close()
    return value, schema_names, request_names


def write_protocol_preamble(writer, use_logical_types, custom_imports):
    """
    Writes a preamble for avro protocol implementation.
    The preamble will contain a function which can load the protocol from the file
    and a global PROTOCOL variable which will contain parsed protocol
    :param writer:
    :param use_logical_types:
    :return:
    """
    write_read_file(writer)
    writer.write('\nfrom avro import protocol as avro_protocol')

    for i in (custom_imports or []):
        writer.write('import %s\n' % i)

    if use_logical_types:
        writer.write('\nfrom avrogen import logical')
    writer.write('\n\ndef __get_protocol(file_name):')
    with writer.indent():
        writer.write('\nproto = avro_protocol.Parse(__read_file(file_name)) if six.PY3 else avro_protocol.parse(__read_file(file_name))')
        writer.write('\nreturn proto')
    writer.write('\n\nPROTOCOL = __get_protocol(os.path.join(os.path.dirname(__file__), "protocol.avpr"))')


def write_populate_schemas(writer):
    """
    Write code which will look through the protocol and populate __SCHEMAS dict which will be used by get_type_schema()
    :param writer:
    :return:
    """
    writer.write('\nfor rec in PROTOCOL.types:')
    with writer.indent():
        writer.write('\n__SCHEMAS[rec.fullname] = rec')

    writer.write('\nfor resp in (six.itervalues(PROTOCOL.messages) if six.PY2 else PROTOCOL.messages):')
    with writer.indent():
        writer.write('\nif isinstance(resp.response, (avro_schema.RecordSchema, avro_schema.EnumSchema)):')
        with writer.indent():
            writer.write('\n__SCHEMAS[resp.response.fullname] = resp.response')

    writer.write('\nPROTOCOL_MESSAGES = {m.name.lstrip("."):m for m in (six.itervalues(PROTOCOL.messages) if six.PY2 else PROTOCOL.messages)}\n')


def write_protocol_files(protocol_json, output_folder, use_logical_types=False, custom_imports=None):
    """
    Generates concrete classes for RecordSchemas and requests and a SpecificReader for types and messages contained
    in the avro protocol.
    :param str protocol_json: JSON containing avro protocol
    :param str output_folder: Folder to write generated files to.
    :param list[str] custom_imports: Add additional import modules
    :return:
    """
    proto_py, record_names, request_names = generate_protocol(protocol_json, use_logical_types, custom_imports)
    names = sorted(list(record_names) + list(request_names))
    if not os.path.isdir(output_folder):
        os.mkdir(output_folder)

    with open(os.path.join(output_folder, "schema_classes.py"), "w+") as f:
        f.write(proto_py)

    with open(os.path.join(output_folder, "protocol.avpr"), "w+") as f:
        f.write(protocol_json)

    ns_dict = generate_namespace_modules(names, output_folder)

    with open(os.path.join(output_folder, "__init__.py"), "w+") as f:
        pass

    write_namespace_modules(ns_dict, request_names, output_folder)
    write_specific_reader(record_names, output_folder, use_logical_types)


def write_specific_reader(record_types, output_folder, use_logical_types):
    """
    Write specific reader implementation for a protocol
    :param list[avro.schema.RecordSchema] record_types:
    :param output_folder:
    :return:
    """
    with open(os.path.join(output_folder, "__init__.py"), "a+") as f:
        writer = TabbedWriter(f)
        writer.write('\n\nfrom .schema_classes import SchemaClasses, PROTOCOL as my_proto, get_schema_type')
        writer.write('\nfrom avro.io import DatumReader')

        write_reader_impl(record_types, writer, use_logical_types)


def write_namespace_modules(ns_dict, request_names, output_folder):
    """
    Writes content of the generated namespace modules. A python module will be created for each namespace
    and will import concrete schema classes from SchemaClasses
    :param ns_dict:
    :param request_names:
    :param output_folder:
    :return:
    """
    for ns in six.iterkeys(ns_dict):
        with open(os.path.join(output_folder, ns.replace('.', os.path.sep), "__init__.py"), "w+") as f:
            currency = '.'
            if ns != '':
                currency += '.' * len(ns.split('.'))
            f.write('from {currency}schema_classes import SchemaClasses\n'.format(currency=currency))
            f.write('from {currency}schema_classes import RequestClasses\n'.format(currency=currency))
            for name in ns_dict[ns]:
                if ns_.make_fullname(ns, name) in request_names:
                    f.write(
                        "{name}Request = RequestClasses.{ns}{name}RequestClass\n".format(name=name,
                                                                                         ns=ns if not ns else (
                                                                                         ns + ".")))
                else:
                    f.write("{name} = SchemaClasses.{ns}{name}Class\n".format(name=name,
                                                                              ns=ns if not ns else (ns + ".")))

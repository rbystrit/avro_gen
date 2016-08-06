import cStringIO as StringIO
import json
import os, os.path

from avro import schema

from avrogen.writer import write_enum, write_record, TabbedWriter

__all__ = ['generate', 'write_files']


def __generate_namespace(current, target, writer):
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


def generate(schema_json):
    """
    Generates files for given avro schema
    :param str schema_json:
    :return Dict[str, str]:
    """

    names = schema.Names()
    schema.make_avsc_object(json.loads(schema_json), names)

    names = [k for k in names.names.iteritems() if isinstance(k[1], (schema.RecordSchema, schema.EnumSchema))]
    names = sorted(names, lambda x, y: cmp(x[0], y[0]))

    main_out = StringIO.StringIO()
    writer = TabbedWriter(main_out)
    writer.write('import json\n')
    writer.write('import os.path\n')
    writer.write('from avrogen.dict_wrapper import DictWrapper\n')
    writer.write('from avro import schema as avro_schema\n')

    writer.write('\n\ndef __read_file(file_name):')
    with writer.indent():
        writer.write('\nwith open(file_name, "r") as f:')
        with writer.indent():
            writer.write('\nreturn f.read()')

    writer.write('\n\ndef __get_names_and_schema(file_name):')
    with writer.indent():
        writer.write('\nnames = avro_schema.Names()')
        writer.write('\nschema = avro_schema.make_avsc_object(json.loads(__read_file(file_name)), names)')
        writer.write('\nreturn names, schema')
    writer.write('\n\nNAMES, SCHEMA = __get_names_and_schema(os.path.join(os.path.dirname(__file__), "schema.avsc"))')

    writer.write('\n\n\nclass SchemaClasses(object):')
    writer.tab()
    writer.write('\n\n')


    current_namespace = tuple()

    for name, field_schema in names:  # type: str, schema.Schema
        namespace = tuple(name.split('.')[:-1])

        if namespace != current_namespace:
            __generate_namespace(current_namespace, namespace, writer)
            current_namespace = namespace
        if isinstance(field_schema, schema.RecordSchema):
            write_record(field_schema, writer)
        elif isinstance(field_schema, schema.EnumSchema):
            write_enum(field_schema, writer)
    writer.write('\npass\n')
    value = main_out.getvalue()
    main_out.close()
    return value, [name[0] for name in names]


def write_files(schema_json, output_folder):
    schema_py, names = generate(schema_json)
    names = sorted(names)

    if not os.path.isdir(output_folder):
        os.mkdir(output_folder)

    with open(os.path.join(output_folder, "schema_classes.py"), "w+") as f:
        f.write(schema_py)

    with open(os.path.join(output_folder, "schema.avsc"), "w+") as f:
        f.write(schema_json)

    ns_dict = __process_namespaces(names, output_folder)

    with open(os.path.join(output_folder, "__init__.py"), "w+") as f:
        pass  # make sure we create this file from scratch

    __dump_namespace_modules(ns_dict, output_folder)
    __dump_specific_reader(ns_dict, output_folder)


def __dump_namespace_modules(ns_dict, output_folder):
    for ns in ns_dict.iterkeys():
        with open(os.path.join(output_folder, ns.replace('.', os.path.sep), "__init__.py"), "w+") as f:
            currency = '.'
            if ns != '':
                currency += '.' * len(ns.split('.'))
            f.write('from {currency}schema_classes import SchemaClasses\n'.format(currency=currency))
            for name in ns_dict[ns]:
                f.write("{name} = SchemaClasses.{ns}{name}Class\n".format(name=name, ns=ns if not ns else (ns + ".")))


def __process_namespaces(names, output_folder):
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


def __dump_specific_reader(ns_dict, output_folder):
    with open(os.path.join(output_folder, "__init__.py"), "a+") as f:
        writer = TabbedWriter(f)
        writer.write('\n\nfrom .schema_classes import SchemaClasses, SCHEMA as my_schema')
        writer.write('\nfrom avro.io import DatumReader')

        writer.write('\n\n\nclass SpecificDatumReader(DatumReader):')
        with writer.indent():
            writer.write('\ndef __init__(self, readers_schema=None, **kwargs):')
            with writer.indent():
                writer.write('\nif readers_schema is None: readers_schema = my_schema')
                writer.write('\nsuper(SpecificDatumReader, self).__init__(readers_schema=readers_schema,**kwargs)')

            writer.write('\n\nNAMEMAP = {')
            with writer.indent():
                for ns in ns_dict.iterkeys():
                    for name in ns_dict[ns]:  # type: unicode
                        fullname = '%s.%s' % (ns, name) if ns else name
                        writer.write("\n'%s': SchemaClasses.%sClass, " % (fullname, fullname))
            writer.write('\n}\n')

            writer.write('\ndef read_record(self, writers_schema, readers_schema, decoder):')
            with writer.indent():
                writer.write(
                    '\n\nresult = super(SpecificDatumReader, self).read_record(writers_schema, readers_schema, decoder)')
                writer.write('\n\nif readers_schema.fullname in SpecificDatumReader.NAMEMAP:')
                with writer.indent():
                    writer.write('\nresult = SpecificDatumReader.NAMEMAP[readers_schema.fullname](result)')
                writer.write('\n\nreturn result')

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
    writer.set_tab(i+1)
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
    writer.write('from avrogen.dict_wrapper import DictWrapper\n')
    writer.write('class SchemaClasses(object):')
    writer.tab()
    writer.write('\n')
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

    ns_dict = {}
    for name in names:
        name_parts = name.split('.')
        full_path = output_folder
        for part in name_parts[:-1]:
            full_path = os.path.join(full_path, part)
            if not os.path.isdir(full_path):
                os.mkdir(full_path)
                with open(os.path.join(full_path, "__init__.py"), "w+"): pass

        ns = '.'.join(name_parts[:-1])
        if not ns in ns_dict:
            ns_dict[ns] = []
        ns_dict[ns].append(name_parts[-1])

    with open(os.path.join(output_folder, "__init__.py"), "w+") as f: pass # make sure we create this file from scratch

    for ns in ns_dict.iterkeys():
        with open(os.path.join(output_folder, ns.replace('.', os.path.sep), "__init__.py"), "w+") as f:
            currency = '.'
            if ns != '':
                currency += '.' * len(ns.split('.'))
            f.write('from {currency}schema_classes import SchemaClasses\n'.format(currency=currency))
            for name in ns_dict[ns]:
                f.write("{name} = SchemaClasses.{ns}{name}Class\n".format(name=name, ns=ns if not ns else (ns + ".")))

    with open(os.path.join(output_folder, "schema_classes.py"), "w+") as f:
        f.write(schema_py)

    with open(os.path.join(output_folder, "__init__.py"), "a+") as f:
        writer = TabbedWriter(f)
        writer.write('from .schema_classes import SchemaClasses')
        writer.write('\nfrom avro.io import DatumReader')

        writer.write('\n\n\nclass SpecificDatumReader(DatumReader):')
        with writer.indent():
            writer.write('\ndef __init__(self, *args, **kwargs):')
            with writer.indent():
                writer.write('\nsuper(SpecificDatumReader, self).__init__(*args,**kwargs)')

            writer.write('\n\nNAMEMAP = {')
            with writer.indent():
                for ns in ns_dict.iterkeys():
                    for name in ns_dict[ns]: #type: unicode
                        fullname = '%s.%s' % (ns, name) if ns else name
                        writer.write("\n'%s': SchemaClasses.%sClass, " % (fullname, fullname))
            writer.write('\n}\n')

            writer.write('\ndef read_record(self, writers_schema, readers_schema, decoder):')
            with writer.indent():
                writer.write('\n\nresult = super(SpecificDatumReader, self).read_record(writers_schema, readers_schema, decoder)')
                writer.write('\n\nif readers_schema.fullname in SpecificDatumReader.NAMEMAP:')
                with writer.indent():
                    writer.write('\nresult = SpecificDatumReader.NAMEMAP[readers_schema.fullname](result)')
                writer.write('\n\nreturn result')



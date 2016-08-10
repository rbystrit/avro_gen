from . import namespace as ns_
from .core_writer import write_defaults, write_fields


def write_protocol_request(message, namespace, writer):
    """
    Write concrete class for a protocol request
    :param avro.protocol.Message message: Message whose request to write
    :param TabbedWriter writer: writer to write to
    :param str namespace: protocol's namespace
    :return:
    """

    fullname = ns_.make_fullname(namespace, message.name)
    namespace, type_name = ns_.split_fullname(fullname)

    writer.write('''\nclass {name}RequestClass(DictWrapper):'''.format(name=type_name))

    with writer.indent():
        writer.write("\n\n")

        writer.write('\nRECORD_SCHEMA = PROTOCOL.messages["%s"].request' % message.name)

        writer.write('\n\n\ndef __init__(self, inner_dict=None):')
        with writer.indent():
            writer.write('\n')
            writer.write('super(RequestClasses.{name}RequestClass, self).__init__(inner_dict)'.format(name=fullname))

            writer.write('\nif inner_dict is None:')
            with writer.indent():
                write_defaults(message.request, writer, my_full_name=fullname + "Request")
        write_fields(message.request, writer)

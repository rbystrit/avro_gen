import six

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
        assert isinstance(text, six.string_types)

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

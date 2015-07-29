from ._compat import PY2

if PY2:
    import ConfigParser as configparser
else:
    import configparser


class Parser(configparser.RawConfigParser):

    def __remove_quotes(self, value):
        quotes = ["'", "\""]
        for quote in quotes:
            if value[0] == quote and value[-1] == quote:
                return value.strip(quote)
        return value

    def get(self, section, option):
        value = configparser.RawConfigParser.get(self, section, option)
        return self.__remove_quotes(value)

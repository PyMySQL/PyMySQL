from ._compat import PY2

if PY2:
    import ConfigParser as configparser
else:
    import configparser


class Parser(configparser.RawConfigParser):

    def __remove_quotes(self, value):
        quotes = ["'", "\""]
        for quote in quotes:
            if len(value) >= 2 and value[0] == value[-1] == quote:
                return value[1:-1]
        return value

    def get(self, section, option, var):
        """Get *option* from *section* if *var* is not supplied."""
        if var:
            return var
        try:
            value = configparser.RawConfigParser.get(self, section, option)
        except configparser.Error:
            return var
        else:
            return self.__remove_quotes(value)

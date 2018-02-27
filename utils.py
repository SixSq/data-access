import os
from ConfigParser import SafeConfigParser

CONFIG_FILE = os.path.expanduser('~/.aws/credentials')
CONFIG_SECTION = 'default'

config_parser = None


def _get_conf_parser(fn):
    global config_parser
    if config_parser is None:
        config_parser = SafeConfigParser()
        config_parser.optionxform = str
        config_parser.read(fn)
    return config_parser


def config_get(opt, default=''):
    parser = _get_conf_parser(CONFIG_FILE)
    return parser.get(CONFIG_SECTION, opt, default)



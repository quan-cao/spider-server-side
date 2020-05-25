import configparser as cfg

def get_one(config, *args):
    parser = cfg.ConfigParser()
    parser.read(config)
    return parser.get(*args)

def get_section(config, section):
    parser = cfg.ConfigParser()
    parser.read(config)
    return parser[section]
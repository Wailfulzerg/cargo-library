import configparser

from loguru import logger


def read_config(path):
    logger.info(f"Loading config with path : {path}")
    config = configparser.RawConfigParser()
    config.read(path)
    sections = config._sections
    logger.info(f"Loaded with sections: {sections}")
    return sections

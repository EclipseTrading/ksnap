"""cli interface lives here
"""
import logging
import sys
from ksnap.config import KsnapConfig
from ksnap.manager import KsnapManager


def set_up_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        '%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)
    return logger


def main():
    set_up_logger()
    config = KsnapConfig.from_cli_args()
    ksnap_manager = KsnapManager(config)
    ksnap_manager.run()
    return


if __name__ == "__main__":
    sys.exit(main())

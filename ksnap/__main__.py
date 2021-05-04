"""cli interface lives here
"""
import logging
import sys
from ksnap.config import KsnapConfig
from ksnap.manager import KsnapManager

logger = logging.getLogger(__name__)


def set_up_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        '%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)
    return logger


def main():
    set_up_logger()
    config = KsnapConfig.from_cli_args()
    logger.info(f'Config: {config}')
    ksnap_manager = KsnapManager(config)
    ksnap_manager.run()
    return


if __name__ == "__main__":
    sys.exit(main())

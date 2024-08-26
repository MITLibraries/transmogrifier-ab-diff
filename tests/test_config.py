import logging

from abdiff.config import configure_logger


def test_configure_logger_not_verbose():
    logger = logging.getLogger(__name__)
    result = configure_logger(logger, verbose=False)
    info_log_level = 20
    assert logger.getEffectiveLevel() == info_log_level
    assert result == "Logger 'tests.test_config' configured with level=INFO"


def test_configure_logger_verbose():
    logger = logging.getLogger(__name__)
    result = configure_logger(logger, verbose=True)
    debug_log_level = 10
    assert logger.getEffectiveLevel() == debug_log_level
    assert result == "Logger 'tests.test_config' configured with level=DEBUG"

import logging


class Logger:
    """
    Classse responsável por efetuar a impressão de mensagens de LOG no console.
    """

    def __init__(self, path: str = None, name: str = "LoggerNameNotDefined"):
        if path is None:
            self._logger = logging.getLogger(f"{0}".format(name))
        else:
            self._logger = logging.getLogger(f"{0}.{1}".format(path, name))

        self._logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        formatter_spec = '%(asctime)s %(name)s %(levelname)s - %(message)s'
        formatter = logging.Formatter(formatter_spec)
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

    def debug(self, msg):
        self._logger.debug(msg)

    def info(self, msg):
        self._logger.info(msg)

    def warn(self, msg):
        self._logger.warning(msg)

    def error(self, msg):
        self._logger.error(msg)

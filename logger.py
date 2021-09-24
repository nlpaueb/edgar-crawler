import os
import logging
from time import strftime, gmtime
from data import LOGGING_DIR


class Logger:
    """
    Creates and initializes a logger using the `logging` Python module
    """

    # TODO: Modify filename to *.log

    def __init__(self, name, filename="log.txt"):
        """
        Initializes the logger
        :param filename:
        """
        self.timestamp = strftime("%Y_%m_%d_%H_%M_%S", gmtime())
        self.filename = f'{name}_{self.timestamp}_{filename}'
        self.name = name

        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            datefmt='%m-%d-%Y %H:%M',
                            filename=os.path.join(LOGGING_DIR, self.filename),
                            filemode='a')

        # Define a Handler which writes INFO messages or higher to the sys.stderr
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)

        # Set a format which is simpler for console use
        formatter = logging.Formatter('%(message)s')

        # Tell the handler to use this format
        console.setFormatter(formatter)

        # Add the handler to the root logger
        logging.getLogger('').addHandler(console)

        self.logger_object = logging.getLogger(name)

    def get_logger(self):
        """
        :return: the logger object
        """
        return self.logger_object
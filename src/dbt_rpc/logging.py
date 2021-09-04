
import logging
import logbook
import logbook.queues
import multiprocessing

import dbt.logger as dbt_logger
from dbt.logger import GLOBAL_LOGGER as logger

from . import fsapi

class LogManager(object):
    def __init__(self, log_path):
        self.log_path = log_path

        fsapi.ensure_dir_exists(self.log_path)

        json_formatter = dbt_logger.JsonFormatter(
            format_string=dbt_logger.STDOUT_LOG_FORMAT
        )

        self.logs_redirect_handler = logbook.FileHandler(
            filename=self.log_path,
            level=logbook.DEBUG,
            bubble=True,
            # TODO : Do we want to filter these?
            filter=self._dbt_logs_only_filter,
        )

        # Big hack?
        self.logs_redirect_handler.formatter = json_formatter

        dbt_logger.log_manager.set_path(None)

    def _dbt_logs_only_filter(self, record, handler):
        """
        DUPLICATE OF LogbookStepLogsStreamWriter._dbt_logs_only_filter
        """
        return record.channel.split(".")[0] == "dbt"

    def setup_handlers(self):
        logger.info("Setting up log handlers...")

        dbt_logger.log_manager.objects = [
            handler
            for handler in dbt_logger.log_manager.objects
            if type(handler) is not logbook.NullHandler
        ]

        handlers = [
            logbook.NullHandler(),
            self.logs_redirect_handler
        ]

        self.log_context = logbook.NestedSetup(handlers)
        self.log_context.push_application()

        logger.info("Done setting up log handlers.")

    def cleanup(self):
        self.log_context.pop_application()

def getQueueSubscriber(queue):
    return logbook.queues.MultiProcessingSubscriber(queue)

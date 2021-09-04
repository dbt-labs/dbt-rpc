
import logging
import logbook
import logbook.queues
import multiprocessing

import dbt.logger as dbt_logger
from dbt.logger import GLOBAL_LOGGER as logger

class LogManager(object):
    def __init__(self, queue):
        self.queue = queue
        self.info_logs_redirect_handler = logbook.queues.MultiProcessingHandler(
            queue=self.queue,
            level=logbook.INFO,
            bubble=True,
            filter=self._dbt_logs_only_filter,
        )
        self.info_logs_redirect_handler.format_string = (
            dbt_logger.STDOUT_LOG_FORMAT
        )

        # self.info_logs_redirect_handler = logbook.StreamHandler(
        #     stream=self.out,
        #     level=logbook.INFO,
        #     bubble=True,
        #     filter=self._dbt_logs_only_filter,
        # )
        # self.info_logs_redirect_handler.format_string = (
        #     dbt_logger.STDOUT_LOG_FORMAT
        # )

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
            dbt_logger.log_manager,
        ]

        handlers.append(self.info_logs_redirect_handler)
        self.log_context = logbook.NestedSetup(handlers)
        self.log_context.push_application()

        logger.info("Done setting up log handlers.")

    def cleanup(self):
        self.log_context.pop_application()
        self.queue.close()

def getQueueSubscriber(queue):
    return logbook.queues.MultiProcessingSubscriber(queue)

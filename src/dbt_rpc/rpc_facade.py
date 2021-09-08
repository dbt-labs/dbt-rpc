
from .logging import GLOBAL_LOGGER as logger, LogManager
import signal
from typing import List, Optional, Dict, Any, Union
from fastapi import HTTPException
from .views import app

from dbt.contracts.rpc import ManifestStatus

from dbt_rpc.services import dbt_service, logs_service

import datetime
import uuid
import os

import threading
import asyncio

ENABLED = False
PROCESS_KEY = uuid.uuid4()
SHIM = None

DBT_PROJECT_DIR = os.getenv('DBT_PROJECT_DIR')

class CompatibilityShim(object):
    def __init__(self, project_path=None):
        # Assume server is running from path to project dir
        if not project_path:
            project_path = os.getcwd()

        self._pid = os.getpid()
        self._status = ManifestStatus.Compiling
        self._status_set_at = datetime.datetime.utcnow()
        self._parse_error = None
        self._internal_manifest = None
        self._logs = []

        # Cache this so we can operate on internal vars while requests are
        # coming in. We don't want to deal with locking or weird race cond's
        self.lastState = self.getCurrentState()
        self.project_path = project_path

    def getCurrentState(self):
        return {
            "status": self._status,
            "error": self._parse_error,
            "logs": self._logs,
            "timestamp": self._status_set_at,
            "pid": self._pid
        }

    def updateState(self, status, error, manifest, logs):
        if status != self._status:
            self._status = status
            self._status_set_at = datetime.datetime.utcnow()

        self._parse_error = error
        self._internal_manifest = manifest
        self._logs = logs

        self.lastState = self.getCurrentState()

    def load_internal_manifest(self):
        """
        Called at startup. This function reads the dbt project located at
        `self.project_path` and parses it out into a manifest. This manifest
        is then stored internally to this object, and is used for subsequent
        operations.
        """

        # 1. Look at the code at the path and generate a manifest
        # 2. Capture logs invoked during parsing and store them on the object
        # 3. Store that manifest and those logs in memory
        # 4. Update status/parse error, etc
        logger.info(f"Loading internal manifest from {self.project_path}")

        manifest = None
        error = None
        parse_logs = []
        try:
            log_path = os.path.join(self.project_path, "target", "dbt.log")
            with logs_service.capture_logs(log_path, parse_logs):
                manifest = dbt_service.parse_to_manifest(self.project_path)

        except Exception as e:
            # TODO : what kinds of exceptions shall we catch in here?
            logger.exception("Could not load project")
            error = e.msg

        logger.info(f"Loaded internal manifest successfully")
        if error:
            self.updateState(ManifestStatus.Error, error, manifest, parse_logs)
        else:
            self.updateState(ManifestStatus.Ready, None, manifest, parse_logs)

def sighup_handler(signum, stack_frame):
    logger.info(f'INTERRUPT: Received signal {signum}')

def enable():
    logger.info("Running in compatibility mode")

    # Set a flag so everyone knows we're in compatibility mode
    global ENABLED
    ENABLED = True

    # This is where the in-memory manifest is actually created and configured
    global SHIM
    SHIM = CompatibilityShim(project_path=DBT_PROJECT_DIR)

    # TODO : Is this like.... a cardinal sin? Or is it ok? I have no idea
    # if i'm allowed to abuse memory like this....
    new_loop = asyncio.new_event_loop()
    t = threading.Thread(target=SHIM.load_internal_manifest)
    t.start()

    # Once the server is loaded, add a signal handler for HUP signals.
    # When a HUP is received, the shim will re-parse the project and
    # generate a new manifest.
    # To test this, run `kill -HUP $pid`
    signal.signal(signal.SIGHUP, sighup_handler)

def handle_status(args):
    return SHIM.lastState

def handle_ps(args):
    return []

DISPATCH = {
    "status": handle_status,
    "ps": handle_ps,
}

def dispatch_rpc(args):
    if args.method in DISPATCH:
        DISPATCH[args.method](args)
    else:
        raise RuntimeError("bad method")

@app.get("/jsonrpc")
async def jsonrpc():
    if not ENABLED:
        raise HTTPException(400, "Compatibility mode is not enabled. Cannot use jsonrpc")

    return {
        "id": "the id you sent me",
        "jsonrpc": "2.0",
        "result": handle_status(None)
    }

# RPC API

# This is gross..... can we not do this?
# from pydantic import BaseModel
# class CommandParams(BaseModel):
#     task_tags: Optional[Dict[str, Any]] = None
#     timeout: Optional[float] = None
# 
# class RPCExecParams(CommandParams):
#     name: str
#     sql: str
#     macros: Optional[str] = None
# 
# class RPCListParams(CommandParams):
#     resource_types: Optional[List[str]] = None
#     models: Union[None, str, List[str]] = None
#     exclude: Union[None, str, List[str]] = None
#     select: Union[None, str, List[str]] = None
#     selector: Optional[str] = None
#     output: Optional[str] = 'json'
# 
# class RPCDocsGenerateParams(CommandParams):
#     compile: bool = True
#     state: Optional[str] = None
# 
# class RPCCliParams(CommandParams):
#     cli: str
# 
# class RPCDepsParams(CommandParams):
#     pass
# 
# class KillParams(CommandParams):
#     task_id: str
# 
# class PollParams(CommandParams):
#     request_token: str
#     logs: bool = True
#     logs_start: int = 0
# 
# class PSParams(CommandParams):
#     active: bool = True
#     completed: bool = False
# 
# class Command(BaseModel):
#     jsonrpc: str
#     method: str
#     id: str
#     task_id: Optional[str]
#     params: Optional[dict]

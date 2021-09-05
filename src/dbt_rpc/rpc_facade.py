
from .logging import GLOBAL_LOGGER as logger, LogManager
import signal
from typing import List, Optional, Dict, Any, Union

ENABLED = False

def sighup_handler(signum, stack_frame):
    logger.info(f'INTERRUPT: Received signal {signum}')

    # What the actual fuck lol
    # so... this is where we'll manage global state?
    # Call the same things that /parse and shit call under the hood
    # I guess this needs to consume the server functions somehow
    # ok... so.... make "server.py" more of the view layer
    #          ... add a more real controller layer
    #          ... this thing consumes the controller layer?
    #          ... or like.... more of a service layer?

def enable():
    global ENABLED
    ENABLED = True

    logger.info("Running in compatibility mode")
    signal.signal(signal.SIGHUP, sighup_handler)

    # Need to get a path to project (maybe from env var?)
    # Need to make that globally available
    # Need to be able to reparse it when this signal comes in
    # Also - add /jsonrpc endpoint that 422s if compatibility mode is disabled (or sth)

def handle_status(command):
    return {
        "result": {
            "status": "ready",
            "error": None,
            "logs": [],
            "timestamp": 'TODO',
            "pid": -1
        },
        "id": command.id,
        "jsonrpc": "2.0"
    }

# RPC API

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

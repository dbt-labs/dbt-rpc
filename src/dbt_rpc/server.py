# import these so we can find them
import dbt
import dbt.adapters.factory
from dbt.parser.manifest import ManifestLoader, process_node
from dbt.config.runtime import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest

from dbt.task.run import RunTask
from dbt.parser.rpc import RPCCallParser
from dbt.rpc.node_runners import RPCExecuteRunner, RPCCompileRunner

import uvicorn
from fastapi import FastAPI, WebSocket, BackgroundTasks
from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder

import json, os, io

from . import fsapi

# Logging bullshit
import dbt.logger as dbt_logger
from dbt.logger import GLOBAL_LOGGER as logger, log_manager
import logging
import logbook
import logbook.queues
import multiprocessing

from typing import List


app = FastAPI()
io_streams = {}

# This fucks with my shit
dbt.tracking.disable_tracking()

class UnparsedManifestBlob(BaseModel):
    state_id: str
    body: str

class State(BaseModel):
    state_id: str

class Config(BaseModel):
    project_dir: str
    profiles_dir: str
    single_threaded: bool = False

    @classmethod
    def new(cls, project_dir):
        return cls(
            project_dir=project_dir,
            profiles_dir="/Users/drew/.dbt"
        )

class RunArgs(BaseModel):
    state_id: str
    models: List[str] = None
    exclude: List[str] = None
    single_threaded: bool = False
    state: str = None
    selector_name: str = None
    defer: bool = None

class SQLConfig(BaseModel):
    state_id: str
    sql: str

@app.get("/")
async def test():
    return {"abc": 123}

@app.post("/push")
async def push_unparsed_manifest(manifest: UnparsedManifestBlob):
    # Parse / validate it
    state_id = manifest.state_id
    body = manifest.body

    path = fsapi.get_root_path(state_id)
    reuse = True

    # Stupid example of reusing an existing manifest
    if not os.path.exists(path):
        reuse = False
        unparsed_manifest_dict = json.loads(body)
        fsapi.write_unparsed_manifest_to_disk(state_id, unparsed_manifest_dict)

    # Write messagepack repr to disk
    # Return a key that the client can use to operate on it?
    return {"ok": True, "state": state_id, "bytes": len(body), "reuse": reuse}


@app.post("/parse")
async def parse_project(state: State):
    path = fsapi.get_root_path(state.state_id)
    serialize_path = fsapi.get_path(state.state_id, 'manifest.msgpack')

    parsed = False
    if not os.path.exists(serialize_path):
        parsed = True

        # Construct a phony config
        config = RuntimeConfig.from_args(Config.new(path))

        # Load the relevant adapter
        dbt.adapters.factory.register_adapter(config)

        manifest = ManifestLoader.get_full_manifest(config)

        # Serialize repr
        with open (serialize_path, 'wb') as fh:
            packed = manifest.to_msgpack()
            fh.write(packed)

    return {"parsing": state.state_id, "path": serialize_path, "parsed": parsed}


@app.post("/run")
async def run_models(args: RunArgs):
    path = fsapi.get_root_path(args.state_id)
    serialize_path = fsapi.get_path(args.state_id, 'manifest.msgpack')

    # Deerialize repr
    with open (serialize_path, 'rb') as fh:
        packed = fh.read()
        manifest = Manifest.from_msgpack(packed)

    config = RuntimeConfig.from_args(Config.new(path))
    taskCls = RunTask(args, config)

    res = taskCls.run()
    encoded = jsonable_encoder(res)

    return {
        "parsing": args.state_id,
        "path": serialize_path,
        "res": encoded,
        "ok": True,
    }


def run_dbt(task_id, args):
    path = fsapi.get_root_path(args.state_id)
    serialize_path = fsapi.get_path(args.state_id, 'manifest.msgpack')

    queue = io_streams[task_id]
    log_manager = LogManager(queue)
    log_manager.setup_handlers()

    # Deerialize repr
    with open (serialize_path, 'rb') as fh:
        packed = fh.read()
        manifest = Manifest.from_msgpack(packed)

    config = RuntimeConfig.from_args(Config.new(path))
    taskCls = RunTask(args, config)

    res = taskCls.run()

    log_manager.cleanup()


@app.post("/run-async")
async def run_models(args: RunArgs, background_tasks: BackgroundTasks):

    # I want to be able to capture the logs emitted here....
    # how do i do that.....
    # I need them sweet realtime logs....
    # I need need need em!!!
    import uuid
    task_id = str(uuid.uuid4())

    import multiprocessing
    queue = multiprocessing.Queue(-1)
    io_streams[task_id] = queue

    background_tasks.add_task(run_dbt, task_id, args)

    return {
        "parsing": args.state_id,
        "task_id": task_id
    }

@app.post("/preview")
async def preview_sql(sql: SQLConfig):
    path = fsapi.get_root_path(sql.state_id)
    serialize_path = fsapi.get_path(sql.state_id, 'manifest.msgpack')

    # Deerialize repr
    with open (serialize_path, 'rb') as fh:
        packed = fh.read()
        manifest = Manifest.from_msgpack(packed)

    config = RuntimeConfig.from_args(Config.new(path))
    rpc_parser = RPCCallParser(
        project=config,
        manifest=manifest,
        root_project=config,
    )

    adapter = dbt.adapters.factory.get_adapter(config)
    rpc_node = rpc_parser.parse_remote(sql.sql, 'name')
    process_node(config, manifest, rpc_node)
    runner = RPCExecuteRunner(config, adapter, rpc_node, 1, 1)

    # compile SQL in the context of the manifest?

    compiled = runner.compile(manifest)
    res = runner.execute(compiled, manifest)

    return {
        "state": sql.state_id,
        "path": serialize_path,
        "ok": True,
        "res": jsonable_encoder(res),
    }

@app.post("/compile")
async def compile_sql(sql: SQLConfig):
    path = fsapi.get_root_path(sql.state_id)
    serialize_path = fsapi.get_path(sql.state_id, 'manifest.msgpack')

    # Deerialize repr
    with open (serialize_path, 'rb') as fh:
        packed = fh.read()
        manifest = Manifest.from_msgpack(packed)

    config = RuntimeConfig.from_args(Config.new(path))
    rpc_parser = RPCCallParser(
        project=config,
        manifest=manifest,
        root_project=config,
    )

    adapter = dbt.adapters.factory.get_adapter(config)
    rpc_node = rpc_parser.parse_remote(sql.sql, 'name')
    process_node(config, manifest, rpc_node)
    runner = RPCCompileRunner(config, adapter, rpc_node, 1, 1)

    # compile SQL in the context of the manifest?

    compiled = runner.compile(manifest)
    res = runner.execute(compiled, manifest)

    return {
        "state": sql.state_id,
        "path": serialize_path,
        "ok": True,
        "res": jsonable_encoder(res),
    }


class Task(BaseModel):
    task_id: str

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    task_id = await websocket.receive_text()
    queue = io_streams[task_id]

    subscriber = logbook.queues.MultiProcessingSubscriber(queue)
    while True:
        try:
            record = subscriber.recv()
            await websocket.send_text(record.message)
        # Queue is closed. Better way to handle this?
        except Exception:
            break

    await websocket.close(code=1000)



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

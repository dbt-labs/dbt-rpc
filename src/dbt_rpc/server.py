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
from typing import List

import json, os, io

from . import fsapi, logging

from dbt.logger import LogManager


app = FastAPI()

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
        # TODO: How do we handle creds more.... dynamically?
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
async def test(tasks: BackgroundTasks):
    return {"abc": 123, "tasks": tasks.tasks}

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
    log_path = fsapi.get_path(args.state_id, task_id, 'logs.stdout')

    log_manager = logging.LogManager(log_path)
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
    # task_id = str(uuid.uuid4())
    task_id = 'my_uuid'

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
    message = await websocket.receive_text()
    message_data = json.loads(message)

    # TODO: Assuming it's a tail command at offset 0
    state_id = message_data['state_id']
    task_id = message_data['task_id']
    command = message_data['command']

    log_path = fsapi.get_path(state_id, task_id, 'logs.stdout')

    # with open(log_path) as fh:
    # subscriber = logging.getQueueSubscriber(queue)
    fh = open(log_path)
    import time
    while True:
        try:
            # record = subscriber.recv()
            res = fh.readline()
            if len(res) == 0:
                time.sleep(0.5)
                continue
            await websocket.send_text(res)
        # Queue is closed. Better way to handle this?
        except Exception:
            break

    await websocket.close(code=1000)


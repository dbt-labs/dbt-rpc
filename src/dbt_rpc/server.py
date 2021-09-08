import os

from . import rpc_facade
from . import crud, models, schemas
from .database import SessionLocal, engine

from .logging import GLOBAL_LOGGER as logger

from .views import app
from .services import dbt_service

COMPATIBILITY_MODE = (os.getenv("DBT_RPC_COMPAT_MODE", "0") == "1")

# Where... does this actually go?
# And what the heck do we do about migrations?
models.Base.metadata.create_all(bind=engine)

# TODO : This messes with stuff
dbt_service.disable_tracking()


@app.on_event("startup")
async def startup_event():
    if COMPATIBILITY_MODE:
        rpc_facade.enable()

@app.on_event("shutdown")
def shutdown_event():
    logger.info("In shutdown function")

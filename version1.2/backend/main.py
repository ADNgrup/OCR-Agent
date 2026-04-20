import asyncio
import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from cores.schema_mongo import ensure_indexes, get_db
from cores.config import POLL_INTERVAL
from cores.pipelines.pipeline_service_v2 import PipelineServiceV2
from routers.api_v2 import router as api_v2_router
from routers.config_router import ensure_config_document, router as config_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


app = FastAPI(title="KVM OCR Pipeline", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(api_v2_router)
app.include_router(config_router)

pipeline_v2_service = PipelineServiceV2()

poll_stop = asyncio.Event()
poll_task = None


@app.on_event("startup")
async def startup_event():
    global poll_task
    db = get_db()
    ensure_indexes(db)
    ensure_config_document(db)

    if db.kvm_sources.count_documents({}) == 0:
        pipeline_v2_service.create_source(
            db,
                [{
                    "name": "default-kvm",
                    "host": "10.128.0.4",
                    "port": 9081,
                    "base_path": "kx",
                    "poll_seconds": POLL_INTERVAL,
                    "enabled": False,
                    "monitor_keys": ["default"],
                    "headers": {},
                    "similarity_threshold": 0.92,
                    "mode": "v2",
                },
                {
                    "name": "dense-log",
                    "host": "10.128.0.4",
                    "port": 9082,
                    "base_path": "kx",
                    "poll_seconds": POLL_INTERVAL,
                    "enabled": False,
                    "monitor_keys": ["default"],
                    "headers": {},
                    "similarity_threshold": 0.92,
                    "mode": "v2",
                },
                {
                    "name": "hmi",
                    "host": "10.128.0.4",
                    "port": 9083,
                    "base_path": "kx",
                    "poll_seconds": POLL_INTERVAL,
                    "enabled": False,
                    "monitor_keys": ["default"],
                    "headers": {},
                    "similarity_threshold": 0.92,
                    "mode": "v2",
                },
            ]
        )

    poll_stop.clear()
    poll_task = asyncio.create_task(pipeline_v2_service.poll_loop(db, poll_stop))


@app.on_event("shutdown")
async def shutdown_event():
    poll_stop.set()
    if poll_task:
        await poll_task


@app.get("/health")
def health():
    return {"status": "ok"}

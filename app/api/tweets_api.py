from fastapi import APIRouter, FastAPI
from contextlib import asynccontextmanager

from app.processor import ElasticSearchProcessor

elastic_processor = ElasticSearchProcessor()
DATA_PROCESSED = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global DATA_PROCESSED
    elastic_processor.process()
    DATA_PROCESSED = True
    app.include_router(router)
    yield


app = FastAPI(
    lifespan=lifespan,
    title="Tweets Database API",
    summary="A FastAPI backend service for retrieving tweets from ElasticSearch index.",
)

router = APIRouter(prefix="/tweets", tags=["tweets"])


@router.get(
    "/antisemitic-with-weapons",
    response_description="List all antisemitic tweets with weapons.",
)
def get_all_antisemitic_with_weapons():
    if not DATA_PROCESSED:
        return {"status": "processing not finished"}
    return elastic_processor.get_antisemitic_with_weapons()


@router.get(
    "/two-or-more-weapons",
    response_description="List all tweets with two or more weapons.",
)
def get_all_not_antisemitic():
    if not DATA_PROCESSED:
        return {"status": "processing not finished"}
    return elastic_processor.get_with_two_weapons()

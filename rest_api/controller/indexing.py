from fastapi import FastAPI, APIRouter, HTTPException
from haystack import Pipeline

from rest_api.utils import get_app, get_pipelines


router = APIRouter()
app: FastAPI = get_app()
indexing_pipeline: Pipeline = get_pipelines().get("indexing_pipeline", None)




@router.post("/update-index")
def update():
    """
    Update Elasticsearch from wiki api
    """
    if not indexing_pipeline:
        raise HTTPException(status_code=501, detail="Indexing Pipeline is not configured.")

    
    indexing_pipeline.run(file_paths=[], meta={}, params={})

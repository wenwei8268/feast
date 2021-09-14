import feast
from fastapi import FastAPI, Request, Security, Depends, HTTPException
from fastapi.security.api_key import APIKeyQuery, APIKeyCookie, APIKeyHeader, APIKey
from mangum import Mangum
from pydantic import BaseModel

app = FastAPI()

API_KEY = "Feast-key 63d37ce9b84f4bbfb4093ae239f37370"
# api_key_header = APIKeyHeader(name="Authorization", auto_error=False)

from feast.feature_server import get_app

store = feast.FeatureStore(repo_path="feature_repo")

app = get_app(store)


# async def get_api_key(api_key_header: str = Security(api_key_header)):
#     print(api_key_header)
#     if api_key_header == API_KEY:
#         return api_key_header
#     else:
#         raise HTTPException(
#             status_code=403, detail="Could not validate credentials"
#         )
#
#
# @app.get("/items/{item_id}")
# async def read_item(item_id: int, query: str, request: Request, api_key: APIKey = Depends(get_api_key)):
#     return {"item_id": item_id, "query": query, "request": await request.json()}

handler = Mangum(app)

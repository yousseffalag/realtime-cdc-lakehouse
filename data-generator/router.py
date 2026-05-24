from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import logging

app = FastAPI()
logger = logging.getLogger(__name__)

# Import your generator
from main import DataGenerator


class GenerateRequest(BaseModel):
    customers: int = 50
    products: int = 100
    orders: int = 200


class IncrementalRequest(BaseModel):
    new_customers: int = 10
    new_orders_per_customer: int = 2


@app.post("/generate")
async def generate_data(request: GenerateRequest):
    """Generate initial dataset"""
    try:
        generator = DataGenerator("postgresql://user:pass@localhost:5432/db")
        results = generator.generate_all_data(
            num_customers=request.customers,
            num_products=request.products,
            num_orders=request.orders
        )
        return {"status": "success", "results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/generate/incremental")
async def generate_incremental(request: IncrementalRequest):
    """Generate incremental data"""
    try:
        generator = DataGenerator("postgresql://user:pass@localhost:5432/db")
        results = generator.generate_incremental(
            new_customers=request.new_customers,
            new_orders_per_customer=request.new_orders_per_customer
        )
        return {"status": "success", "results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    return {"status": "healthy"}
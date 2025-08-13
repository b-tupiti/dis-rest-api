from fastapi import FastAPI, HTTPException
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

app = FastAPI()

dynamodb = boto3.resource('dynamodb', region_name='eu-west-2')
products_table = dynamodb.Table('products')
reviews_table = dynamodb.Table('reviews')
inventory_table = dynamodb.Table('inventory')

@app.get("/products/{product_id}")
async def get_product(product_id: str):
    try:

        response = products_table.get_item(Key={'product_id': product_id})
        print(response)

        item = response.get('Item')
        if not item:
            raise HTTPException(status_code=404, detail=f"Product with ID {product_id} not found")
        
        return item
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        raise HTTPException(status_code=500, detail=f"DynamoDB error: {error_code} - {error_message}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")





@app.get("/products/{product_id}/reviews")
async def get_reviews(product_id: str):
    """
    Fetches all reviews for a specific product from the reviews table.
    The reviews table has product_id as the partition key.
    """
    try:
        response = reviews_table.query(
            KeyConditionExpression=Key('product_id').eq(product_id)
        )

        reviews = response.get('Items', [])
        
        if not reviews:
            return []

        return reviews
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        raise HTTPException(status_code=500, detail=f"DynamoDB error: {error_code} - {error_message}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    


@app.get("/products/{product_id}/inventory")
async def get_reviews(product_id: str):
    """
    Fetches all reviews for a specific product from the reviews table.
    The reviews table has product_id as the partition key.
    """
    try:
        response = inventory_table.query(
            KeyConditionExpression=Key('product_id').eq(product_id)
        )

        inventory = response.get('Items', [])
        
        if not inventory:
            return []

        return inventory
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        raise HTTPException(status_code=500, detail=f"DynamoDB error: {error_code} - {error_message}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
from fastapi import FastAPI, HTTPException
import boto3
from botocore.exceptions import ClientError
from fastapi import Query
from typing import Optional

# Initialize FastAPI app
app = FastAPI()

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='eu-west-2')
products_table = dynamodb.Table('products')
reviews_table = dynamodb.Table('reviews')

@app.get("/products/{product_id}")
async def get_product(product_id: str):
    try:
        # Fetch item from DynamoDB
        response = products_table.get_item(Key={'product_id': product_id})
        print(response)
        # Check if item exists
        item = response.get('Item')
        if not item:
            raise HTTPException(status_code=404, detail=f"Product with ID {product_id} not found")
        
        return item
    except ClientError as e:
        # Handle DynamoDB-specific errors
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        raise HTTPException(status_code=500, detail=f"DynamoDB error: {error_code} - {error_message}")
    except Exception as e:
        # Handle unexpected errors
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")



from boto3.dynamodb.conditions import Key
# This is the new handler you requested
@app.get("/products/{product_id}/reviews")
async def get_reviews(product_id: str):
    """
    Fetches all reviews for a specific product from the reviews table.
    The reviews table has product_id as the partition key.
    """
    try:
        # Use the query operation to get all items with a matching product_id
        response = reviews_table.query(
            KeyConditionExpression=Key('product_id').eq(product_id)
        )
        
        # The 'Items' key contains the list of all matching reviews
        reviews = response.get('Items', [])
        
        # If no reviews are found, return an empty list with a 200 OK status
        if not reviews:
            # You could also raise a 404, but returning an empty list is a common
            # and often preferred pattern for a collection endpoint.
            return []

        return reviews
    except ClientError as e:
        # Handle DynamoDB-specific errors
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        raise HTTPException(status_code=500, detail=f"DynamoDB error: {error_code} - {error_message}")
    except Exception as e:
        # Handle unexpected errors
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
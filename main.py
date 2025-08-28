from fastapi import FastAPI, HTTPException, Query
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from fastapi import FastAPI, HTTPException, Body
app = FastAPI()
from decimal import Decimal

dynamodb = boto3.resource('dynamodb', region_name='eu-west-2')
products_table = dynamodb.Table('products')
reviews_table = dynamodb.Table('reviews')
inventory_table = dynamodb.Table('inventory')

@app.get("/product/{product_id}")
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


@app.put("/product/{product_id}")
async def update_product(
    product_id: str,
    product_data: dict = Body(...)
):
    """
    Updates an existing product in the DynamoDB table.

    Args:
        product_id: The ID of the product to update.
        product_data: The new product data (name and price) from the request body.
    
    Returns:
        The updated product item.
    """
    try:
        # Check if the item exists before attempting to update it.
        # This is a good practice to avoid creating new items with PUT.
        response = products_table.get_item(Key={'product_id': product_id})
        if 'Item' not in response:
            raise HTTPException(status_code=404, detail=f"Product with ID {product_id} not found")
            
        # Extract name and price from the dictionary
        name = product_data.get("name")
        price = product_data.get("price")

        if name is None or price is None:
            raise HTTPException(status_code=422, detail="Missing 'name' or 'price' in request body")
            
        # Convert float to Decimal for DynamoDB compatibility
        price_decimal = Decimal(str(price))
            
        # Perform the update operation on the DynamoDB table
        update_response = products_table.update_item(
            Key={'product_id': product_id},
            UpdateExpression="SET #n = :name, price = :price",
            ExpressionAttributeNames={
                "#n": "name"  # Use an alias for 'name' since it's a reserved word in DynamoDB
            },
            ExpressionAttributeValues={
                ":name": name,
                ":price": price_decimal
            },
            ReturnValues="UPDATED_NEW" # Return the newly updated attributes
        )
        
        # Return the updated attributes from the response
        return update_response.get("Attributes")

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        raise HTTPException(status_code=500, detail=f"DynamoDB error: {error_code} - {error_message}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/inventory/{product_id}")
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
    
@app.get("/reviews/{product_id}")
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
    

@app.get("/products/")
async def get_products_under_price(
    max_price: float = Query(None, description="Maximum price to filter products by."),
    num_products: int = Query(30, description="The number of products to return."),
    category: str = Query(None, description="The category to search for. If provided, uses an efficient query on the partition key.")
):
    """
    Fetches a specific number of products with a price less than the specified max_price.
    It can perform an efficient query on the category partition key if provided.

    Args:
        max_price: The maximum price (exclusive) to filter products by.
        num_products: The number of products to return.
        category: An optional category to use for an efficient query lookup.
    
    Returns:
        A list of products that meet the criteria.
    """
    try:
        # If a category is provided, use a query for efficiency
        if category:
            response = products_table.query(
                KeyConditionExpression=Key('category').eq(category),
                FilterExpression=Attr('price').lt(Decimal(str(max_price))) if max_price is not None else None,
                Limit=num_products
            )
            return response.get('Items', [])
        
        # If no partition key (category) is provided, fall back to a less efficient scan
        else:
            # The input price is converted to a Decimal for DynamoDB compatibility
            max_price_decimal = Decimal(str(max_price)) if max_price is not None else None
            
            products = []
            response = None
            
            while True:
                scan_kwargs = {}
                if max_price_decimal is not None:
                    scan_kwargs['FilterExpression'] = Attr('price').lt(max_price_decimal)
                
                if response is not None and 'LastEvaluatedKey' in response:
                    scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']

                response = products_table.scan(**scan_kwargs)
                products.extend(response.get('Items', []))

                if len(products) >= num_products or 'LastEvaluatedKey' not in response:
                    break

            return products[:num_products]

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        raise HTTPException(status_code=500, detail=f"DynamoDB error: {error_code} - {error_message}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    

###############################

# Helper function to get inventory information for a single product.
# This is a good practice to keep the main handler clean.
async def get_inventory_info(product_id: str):
    """Fetches inventory data for a given product ID."""
    try:
        response = inventory_table.get_item(Key={'product_id': product_id})
        return response.get('Item', {})
    except ClientError as e:
        print(f"Error fetching inventory for {product_id}: {e}")
        return {"error": "Inventory lookup failed"}


# Helper function to get reviews information for a single product.
async def get_reviews_info(product_id: str):
    """Fetches reviews for a given product ID."""
    try:
        response = reviews_table.query(
            KeyConditionExpression=Key('product_id').eq(product_id)
        )
        return response.get('Items', [])
    except ClientError as e:
        print(f"Error fetching reviews for {product_id}: {e}")
        return {"error": "Reviews lookup failed"}

import asyncio
@app.get("/products_v2/")
async def get_products_under_price_2(
    max_price: float = Query(None, description="Maximum price to filter products by."),
    num_products: int = Query(30, description="The number of products to return."),
    category: str = Query(None, description="The category to search for. If provided, uses an efficient query on the partition key.")
):
    """
    Fetches a specific number of products with a price less than the specified max_price
    and includes their inventory and review information.
    
    Args:
        max_price: The maximum price (exclusive) to filter products by.
        num_products: The number of products to return.
        category: An optional category to use for an efficient query lookup.
    
    Returns:
        A list of products with their inventory and reviews.
    """
    try:
        products = []

        # If a category is provided, use a query for efficiency
        if category:
            response = products_table.query(
                KeyConditionExpression=Key('category').eq(category),
                FilterExpression=Attr('price').lt(Decimal(str(max_price))) if max_price is not None else None,
                Limit=num_products
            )
            products = response.get('Items', [])
        
        # If no partition key (category) is provided, fall back to a less efficient scan
        else:
            # The input price is converted to a Decimal for DynamoDB compatibility
            max_price_decimal = Decimal(str(max_price)) if max_price is not None else None
            
            response = None
            while True:
                scan_kwargs = {}
                if max_price_decimal is not None:
                    scan_kwargs['FilterExpression'] = Attr('price').lt(max_price_decimal)
                
                if response is not None and 'LastEvaluatedKey' in response:
                    scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']

                response = products_table.scan(**scan_kwargs)
                products.extend(response.get('Items', []))

                if len(products) >= num_products or 'LastEvaluatedKey' not in response:
                    break
            products = products[:num_products]

        # Use asyncio to fetch inventory and reviews concurrently for all products found
        tasks = []
        for product in products:
            product_id = product.get('product_id')
            if product_id:
                tasks.append(get_inventory_info(product_id))
                tasks.append(get_reviews_info(product_id))
        
        # Await all concurrent tasks
        results = await asyncio.gather(*tasks)

        # Merge the fetched data back into the product list
        for i, product in enumerate(products):
            inventory_data = results[i * 2]
            reviews_data = results[i * 2 + 1]
            
            product['inventory'] = inventory_data
            product['reviews'] = reviews_data

        return products

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        raise HTTPException(status_code=500, detail=f"DynamoDB error: {error_code} - {error_message}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    

@app.get("/products_v3/")
async def get_all_products_with_details():
    """
    Fetches all products and includes their inventory and review information.
    
    Returns:
        A list of all products with their inventory and reviews.
    """
    try:
        all_products = []
        response = products_table.scan()
        all_products.extend(response.get('Items', []))
        
        # Continue scanning if there are more items
        while 'LastEvaluatedKey' in response:
            response = products_table.scan(
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            all_products.extend(response.get('Items', []))

        # Use asyncio to fetch inventory and reviews concurrently for all products found
        tasks = []
        for product in all_products:
            product_id = product.get('product_id')
            if product_id:
                tasks.append(get_inventory_info(product_id))
                tasks.append(get_reviews_info(product_id))
        
        # Await all concurrent tasks
        results = await asyncio.gather(*tasks)

        # Merge the fetched data back into the product list
        for i, product in enumerate(all_products):
            inventory_data = results[i * 2]
            reviews_data = results[i * 2 + 1]
            
            product['inventory'] = inventory_data
            product['reviews'] = reviews_data

        return all_products

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        raise HTTPException(status_code=500, detail=f"DynamoDB error: {error_code} - {error_message}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
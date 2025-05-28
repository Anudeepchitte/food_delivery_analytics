"""
ETL Implementation for Food Delivery Data Engineering Project

This script implements the ETL pipeline for loading data from raw files into the
Snowflake data warehouse, following the schema design in snowflake_schema_design.md.

The ETL process includes:
1. Initial data loading to Stage layer
2. Transformation logic for Clean layer
3. SCD2 implementation for dimension tables
4. Fact table population
5. Setup for incremental loads using Snowflake streams and tasks

Requirements:
- snowflake-connector-python
- pandas
"""

import os
import pandas as pd
import snowflake.connector
from datetime import datetime
import logging
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_process.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Snowflake connection parameters (would be stored in environment variables or config file in production)
SNOWFLAKE_CONFIG = {
    "account": "your_account",
    "user": "your_user",
    "password": "your_password",
    "warehouse": "your_warehouse",
    "database": "FOOD_DELIVERY_DW",
    "role": "your_role"
}

# Data file paths
DATA_DIR = "/home/ubuntu/food_delivery_data"
DATA_FILES = {
    "restaurants": os.path.join(DATA_DIR, "UberEats_restaurants.csv"),
    "zomato": os.path.join(DATA_DIR, "zomato.csv"),
    "products": os.path.join(DATA_DIR, "product.csv"),
    "promotions": os.path.join(DATA_DIR, "promotions.csv"),
    "customers": os.path.join(DATA_DIR, "customers.csv"),
    "orders": os.path.join(DATA_DIR, "orders.csv"),
    "order_items": os.path.join(DATA_DIR, "order_item.csv"),
    "delivery": os.path.join(DATA_DIR, "delivery_data.csv")
}

class SnowflakeConnector:
    """Class to handle Snowflake connections and operations"""
    
    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.conn = None
    
    def connect(self):
        """Establish connection to Snowflake"""
        try:
            self.conn = snowflake.connector.connect(
                account=self.config["account"],
                user=self.config["user"],
                password=self.config["password"],
                warehouse=self.config["warehouse"],
                database=self.config["database"],
                role=self.config["role"]
            )
            logger.info("Connected to Snowflake successfully")
            return self.conn
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {e}")
            raise
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results"""
        if not self.conn:
            self.connect()
        
        try:
            cursor = self.conn.cursor(snowflake.connector.DictCursor)
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            logger.error(f"Query: {query}")
            raise
    
    def execute_file(self, file_path: str) -> None:
        """Execute SQL commands from a file"""
        try:
            with open(file_path, 'r') as f:
                sql = f.read()
            
            # Split the SQL file by semicolons to execute multiple statements
            statements = sql.split(';')
            for statement in statements:
                if statement.strip():
                    self.execute_query(statement)
            
            logger.info(f"Successfully executed SQL from file: {file_path}")
        except Exception as e:
            logger.error(f"Error executing SQL file {file_path}: {e}")
            raise
    
    def load_dataframe_to_table(self, df: pd.DataFrame, table_name: str, schema: str) -> None:
        """Load a pandas DataFrame to a Snowflake table"""
        try:
            # Create a temporary CSV file
            temp_file = f"/tmp/{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
            df.to_csv(temp_file, index=False, header=True)
            
            # Create a stage for file upload
            stage_name = f"{schema}.TEMP_STAGE_{table_name}"
            self.execute_query(f"CREATE OR REPLACE TEMPORARY STAGE {stage_name}")
            
            # Upload file to stage
            cursor = self.conn.cursor()
            cursor.execute(f"PUT file://{temp_file} @{stage_name}")
            
            # Copy data from stage to table
            copy_query = f"""
            COPY INTO {schema}.{table_name}
            FROM @{stage_name}
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE'
            """
            cursor.execute(copy_query)
            
            # Clean up
            os.remove(temp_file)
            logger.info(f"Successfully loaded data to {schema}.{table_name}")
        except Exception as e:
            logger.error(f"Error loading data to {schema}.{table_name}: {e}")
            raise
    
    def close(self) -> None:
        """Close the Snowflake connection"""
        if self.conn:
            self.conn.close()
            logger.info("Snowflake connection closed")


class ETLPipeline:
    """Main ETL pipeline class"""
    
    def __init__(self, snowflake_config: Dict[str, str], data_files: Dict[str, str]):
        self.snowflake = SnowflakeConnector(snowflake_config)
        self.data_files = data_files
    
    def create_schemas(self) -> None:
        """Create the required schemas if they don't exist"""
        schemas = ["STAGE_SCHEMA", "CLEAN_SCHEMA", "CONSUMPTION_SCHEMA"]
        for schema in schemas:
            self.snowflake.execute_query(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        logger.info("Created schemas successfully")
    
    def load_stage_tables(self) -> None:
        """Load data from CSV files to stage tables"""
        logger.info("Starting data load to stage tables")
        
        # Load restaurants data
        restaurants_df = pd.read_csv(self.data_files["restaurants"])
        # Add metadata columns
        restaurants_df["source_system"] = "UberEats"
        restaurants_df["source_file"] = os.path.basename(self.data_files["restaurants"])
        restaurants_df["load_timestamp"] = datetime.now()
        self.snowflake.load_dataframe_to_table(
            restaurants_df, "STG_RESTAURANTS", "STAGE_SCHEMA"
        )
        
        # Load zomato data (additional restaurant data)
        zomato_df = pd.read_csv(self.data_files["zomato"])
        zomato_df["source_system"] = "Zomato"
        zomato_df["source_file"] = os.path.basename(self.data_files["zomato"])
        zomato_df["load_timestamp"] = datetime.now()
        # Transform and load to stage table
        # This would require mapping Zomato fields to the restaurant schema
        
        # Load products data
        products_df = pd.read_csv(self.data_files["products"])
        products_df["source_system"] = "Internal"
        products_df["source_file"] = os.path.basename(self.data_files["products"])
        products_df["load_timestamp"] = datetime.now()
        self.snowflake.load_dataframe_to_table(
            products_df, "STG_PRODUCTS", "STAGE_SCHEMA"
        )
        
        # Load promotions data
        promotions_df = pd.read_csv(self.data_files["promotions"])
        promotions_df["source_system"] = "Internal"
        promotions_df["source_file"] = os.path.basename(self.data_files["promotions"])
        promotions_df["load_timestamp"] = datetime.now()
        self.snowflake.load_dataframe_to_table(
            promotions_df, "STG_PROMOTIONS", "STAGE_SCHEMA"
        )
        
        # Load customers data
        customers_df = pd.read_csv(self.data_files["customers"])
        customers_df["source_system"] = "Internal"
        customers_df["source_file"] = os.path.basename(self.data_files["customers"])
        customers_df["load_timestamp"] = datetime.now()
        self.snowflake.load_dataframe_to_table(
            customers_df, "STG_CUSTOMERS", "STAGE_SCHEMA"
        )
        
        # Load orders data
        orders_df = pd.read_csv(self.data_files["orders"])
        orders_df["source_system"] = "Internal"
        orders_df["source_file"] = os.path.basename(self.data_files["orders"])
        orders_df["load_timestamp"] = datetime.now()
        self.snowflake.load_dataframe_to_table(
            orders_df, "STG_ORDERS", "STAGE_SCHEMA"
        )
        
        # Load order items data
        order_items_df = pd.read_csv(self.data_files["order_items"])
        order_items_df["source_system"] = "Internal"
        order_items_df["source_file"] = os.path.basename(self.data_files["order_items"])
        order_items_df["load_timestamp"] = datetime.now()
        self.snowflake.load_dataframe_to_table(
            order_items_df, "STG_ORDER_ITEMS", "STAGE_SCHEMA"
        )
        
        # Load delivery data
        delivery_df = pd.read_csv(self.data_files["delivery"])
        delivery_df["source_system"] = "Internal"
        delivery_df["source_file"] = os.path.basename(self.data_files["delivery"])
        delivery_df["load_timestamp"] = datetime.now()
        self.snowflake.load_dataframe_to_table(
            delivery_df, "STG_DELIVERY", "STAGE_SCHEMA"
        )
        
        logger.info("Completed data load to stage tables")
    
    def transform_to_clean_layer(self) -> None:
        """Transform data from stage to clean layer"""
        logger.info("Starting transformation to clean layer")
        
        # Transform restaurants data
        restaurant_transform_query = """
        INSERT INTO CLEAN_SCHEMA.CLEAN_RESTAURANTS (
            restaurant_id,
            restaurant_name,
            cuisine_type,
            address,
            city,
            state,
            country,
            postal_code,
            latitude,
            longitude,
            phone_number,
            email,
            operating_hours,
            created_at,
            updated_at,
            effective_from,
            effective_to,
            is_current,
            source_system,
            dw_created_at
        )
        SELECT
            loc_number AS restaurant_id,
            loc_name AS restaurant_name,
            cuisines AS cuisine_type,
            address,
            searched_city AS city,
            searched_state AS state,
            'USA' AS country,
            searched_zipcode AS postal_code,
            latitude,
            longitude,
            phone AS phone_number,
            NULL AS email,
            NULL AS operating_hours,
            CURRENT_TIMESTAMP() AS created_at,
            CURRENT_TIMESTAMP() AS updated_at,
            CURRENT_TIMESTAMP() AS effective_from,
            NULL AS effective_to,
            TRUE AS is_current,
            source_system,
            CURRENT_TIMESTAMP() AS dw_created_at
        FROM STAGE_SCHEMA.STG_RESTAURANTS
        WHERE loc_number IS NOT NULL
        """
        self.snowflake.execute_query(restaurant_transform_query)
        
        # Transform products data
        product_transform_query = """
        INSERT INTO CLEAN_SCHEMA.CLEAN_PRODUCTS (
            product_id,
            restaurant_id,
            product_name,
            description,
            category,
            price,
            cost,
            is_vegetarian,
            is_vegan,
            is_gluten_free,
            calories,
            preparation_time,
            created_at,
            updated_at,
            effective_from,
            effective_to,
            is_current,
            source_system,
            dw_created_at
        )
        SELECT
            id AS product_id,
            NULL AS restaurant_id,
            name AS product_name,
            description,
            NULL AS category,
            REPLACE(price, '$', '')::FLOAT AS price,
            REPLACE(cost, '$', '')::FLOAT AS cost,
            FALSE AS is_vegetarian,
            FALSE AS is_vegan,
            FALSE AS is_gluten_free,
            NULL AS calories,
            NULL AS preparation_time,
            CURRENT_TIMESTAMP() AS created_at,
            CURRENT_TIMESTAMP() AS updated_at,
            CURRENT_TIMESTAMP() AS effective_from,
            NULL AS effective_to,
            TRUE AS is_current,
            source_system,
            CURRENT_TIMESTAMP() AS dw_created_at
        FROM STAGE_SCHEMA.STG_PRODUCTS
        """
        self.snowflake.execute_query(product_transform_query)
        
        # Transform promotions data
        promotion_transform_query = """
        INSERT INTO CLEAN_SCHEMA.CLEAN_PROMOTIONS (
            promotion_id,
            promotion_name,
            description,
            discount_type,
            discount_value,
            start_date,
            end_date,
            min_order_value,
            max_discount,
            restaurant_id,
            is_active,
            created_at,
            updated_at,
            effective_from,
            effective_to,
            is_current,
            source_system,
            dw_created_at
        )
        SELECT
            promotion_id,
            name AS promotion_name,
            description,
            discount_type,
            discount_value,
            start_date,
            end_date,
            min_order_value,
            max_discount,
            restaurant_id,
            is_active,
            CURRENT_TIMESTAMP() AS created_at,
            CURRENT_TIMESTAMP() AS updated_at,
            CURRENT_TIMESTAMP() AS effective_from,
            NULL AS effective_to,
            TRUE AS is_current,
            source_system,
            CURRENT_TIMESTAMP() AS dw_created_at
        FROM STAGE_SCHEMA.STG_PROMOTIONS
        """
        self.snowflake.execute_query(promotion_transform_query)
        
        # Transform customers data
        customer_transform_query = """
        INSERT INTO CLEAN_SCHEMA.CLEAN_CUSTOMERS (
            customer_id,
            first_name,
            last_name,
            email,
            phone_number,
            address,
            city,
            state,
            country,
            postal_code,
            latitude,
            longitude,
            created_at,
            updated_at,
            referral_customer_id,
            effective_from,
            effective_to,
            is_current,
            source_system,
            dw_created_at
        )
        SELECT
            id AS customer_id,
            first_name,
            last_name,
            NULL AS email,
            NULL AS phone_number,
            NULL AS address,
            city,
            NULL AS state,
            NULL AS country,
            NULL AS postal_code,
            NULL AS latitude,
            NULL AS longitude,
            CURRENT_TIMESTAMP() AS created_at,
            CURRENT_TIMESTAMP() AS updated_at,
            referral_customer_id,
            CURRENT_TIMESTAMP() AS effective_from,
            NULL AS effective_to,
            TRUE AS is_current,
            source_system,
            CURRENT_TIMESTAMP() AS dw_created_at
        FROM STAGE_SCHEMA.STG_CUSTOMERS
        """
        self.snowflake.execute_query(customer_transform_query)
        
        # Transform orders data
        order_transform_query = """
        INSERT INTO CLEAN_SCHEMA.CLEAN_ORDERS (
            order_id,
            customer_id,
            restaurant_id,
            order_date,
            order_status,
            delivery_address,
            delivery_city,
            delivery_state,
            delivery_postal_code,
            delivery_latitude,
            delivery_longitude,
            order_total,
            tax_amount,
            tip_amount,
            promotion_id,
            payment_method,
            payment_status,
            created_at,
            updated_at,
            source_system,
            dw_created_at
        )
        SELECT
            id AS order_id,
            customer_id,
            NULL AS restaurant_id,
            created_at AS order_date,
            updated_status AS order_status,
            NULL AS delivery_address,
            NULL AS delivery_city,
            NULL AS delivery_state,
            NULL AS delivery_postal_code,
            NULL AS delivery_latitude,
            NULL AS delivery_longitude,
            NULL AS order_total,
            NULL AS tax_amount,
            tips AS tip_amount,
            NULL AS promotion_id,
            NULL AS payment_method,
            NULL AS payment_status,
            created_at,
            updated_at,
            source_system,
            CURRENT_TIMESTAMP() AS dw_created_at
        FROM STAGE_SCHEMA.STG_ORDERS
        """
        self.snowflake.execute_query(order_transform_query)
        
        # Transform order items data
        order_item_transform_query = """
        INSERT INTO CLEAN_SCHEMA.CLEAN_ORDER_ITEMS (
            order_item_id,
            order_id,
            product_id,
            quantity,
            unit_price,
            item_total,
            special_instructions,
            created_at,
            updated_at,
            source_system,
            dw_created_at
        )
        SELECT
            id AS order_item_id,
            order_id,
            product_id,
            item_quantity AS quantity,
            price AS unit_price,
            price * item_quantity AS item_total,
            NULL AS special_instructions,
            CURRENT_TIMESTAMP() AS created_at,
            CURRENT_TIMESTAMP() AS updated_at,
            source_system,
            CURRENT_TIMESTAMP() AS dw_created_at
        FROM STAGE_SCHEMA.STG_ORDER_ITEMS
        """
        self.snowflake.execute_query(order_item_transform_query)
        
        # Transform delivery data
        delivery_transform_query = """
        INSERT INTO CLEAN_SCHEMA.CLEAN_DELIVERY (
            delivery_id,
            order_id,
            delivery_person_id,
            delivery_status,
            pickup_time,
            delivery_time,
            estimated_delivery_time,
            actual_delivery_time,
            delivery_distance,
            weather_conditions,
            traffic_density,
            vehicle_type,
            created_at,
            updated_at,
            source_system,
            dw_created_at
        )
        SELECT
            ID AS delivery_id,
            NULL AS order_id,
            Delivery_person_ID AS delivery_person_id,
            NULL AS delivery_status,
            Time_Orderd AS pickup_time,
            Time_Order_picked AS delivery_time,
            NULL AS estimated_delivery_time,
            NULL AS actual_delivery_time,
            NULL AS delivery_distance,
            Weatherconditions AS weather_conditions,
            Road_traffic_density AS traffic_density,
            Type_of_vehicle AS vehicle_type,
            Order_Date AS created_at,
            Order_Date AS updated_at,
            source_system,
            CURRENT_TIMESTAMP() AS dw_created_at
        FROM STAGE_SCHEMA.STG_DELIVERY
        """
        self.snowflake.execute_query(delivery_transform_query)
        
        # Transform ratings data
        rating_transform_query = """
        INSERT INTO CLEAN_SCHEMA.CLEAN_RATINGS (
            rating_id,
            order_id,
            customer_id,
            restaurant_id,
            delivery_person_id,
            food_rating,
            delivery_rating,
            overall_rating,
            comments,
            created_at,
            source_system,
            dw_created_at
        )
        SELECT
            o.id || '-rating' AS rating_id,
            o.id AS order_id,
            o.customer_id,
            NULL AS restaurant_id,
            NULL AS delivery_person_id,
            NULL AS food_rating,
            NULL AS delivery_rating,
            o.rating AS overall_rating,
            NULL AS comments,
            o.updated_at AS created_at,
            o.source_system,
            CURRENT_TIMESTAMP() AS dw_created_at
        FROM STAGE_SCHEMA.STG_ORDERS o
        WHERE o.rating IS NOT NULL
        """
        self.snowflake.execute_query(rating_transform_query)
        
        logger.info("Completed transformation to clean layer")
    
    def populate_dimension_tables(self) -> None:
        """Populate dimension tables with SCD2 logic"""
        logger.info("Starting dimension table population with SCD2 logic")
        
        # Populate date dimension
        date_dimension_query = """
        INSERT INTO CONSUMPTION_SCHEMA.DIM_DATE (
            date_sk,
            date_id,
            day_of_week,
            day_of_week_name,
            day_of_month,
            day_of_year,
            week_of_year,
            month_number,
            month_name,
            quarter,
            year,
            is_weekend,
            is_holiday,
            holiday_name,
            fiscal_year,
            fiscal_quarter
        )
        WITH date_range AS (
            SELECT DATEADD(DAY, seq, '2020-01-01'::DATE) AS date_id
            FROM TABLE(GENERATOR(ROWCOUNT => 1825)) -- 5 years of dates
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY date_id) AS date_sk,
            date_id,
            DAYOFWEEK(date_id) AS day_of_week,
            DAYNAME(date_id) AS day_of_week_name,
            DAYOFMONTH(date_id) AS day_of_month,
            DAYOFYEAR(date_id) AS day_of_year,
            WEEKOFYEAR(date_id) AS week_of_year,
            MONTH(date_id) AS month_number,
            MONTHNAME(date_id) AS month_name,
            QUARTER(date_id) AS quarter,
            YEAR(date_id) AS year,
            CASE WHEN DAYOFWEEK(date_id) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
            FALSE AS is_holiday, -- Would need a holiday calendar to populate
            NULL AS holiday_name,
            YEAR(date_id) AS fiscal_year, -- Assuming calendar year = fiscal year
            QUARTER(date_id) AS fiscal_quarter
        FROM date_range
        """
        self.snowflake.execute_query(date_dimension_query)
        
        # Populate time dimension
        time_dimension_query = """
        INSERT INTO CONSUMPTION_SCHEMA.DIM_TIME (
            time_sk,
            time_id,
            hour_of_day,
            minute_of_hour,
            second_of_minute,
            am_pm,
            time_of_day_category
        )
        WITH time_range AS (
            SELECT TIMEADD(SECOND, seq, '00:00:00'::TIME) AS time_id
            FROM TABLE(GENERATOR(ROWCOUNT => 86400)) -- Seconds in a day
            WHERE MOD(seq, 60) = 0 -- Every minute
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY time_id) AS time_sk,
            time_id,
            HOUR(time_id) AS hour_of_day,
            MINUTE(time_id) AS minute_of_hour,
            SECOND(time_id) AS second_of_minute,
            CASE WHEN HOUR(time_id) < 12 THEN 'AM' ELSE 'PM' END AS am_pm,
            CASE
                WHEN HOUR(time_id) BETWEEN 5 AND 11 THEN 'Morning'
                WHEN HOUR(time_id) BETWEEN 12 AND 16 THEN 'Afternoon'
                WHEN HOUR(time_id) BETWEEN 17 AND 20 THEN 'Evening'
                ELSE 'Night'
            END AS time_of_day_category
        FROM time_range
        """
        self.snowflake.execute_query(time_dimension_query)
        
        # Populate restaurant dimension with SCD2
        restaurant_dimension_query = """
        MERGE INTO CONSUMPTION_SCHEMA.DIM_RESTAURANT target
        USING (
            SELECT 
                restaurant_sk,
                restaurant_id,
                restaurant_name,
                cuisine_type,
                address,
                city,
                state,
                country,
                postal_code,
                latitude,
                longitude,
                phone_number,
                email,
                operating_hours,
                created_at,
                updated_at,
                effective_from,
                effective_to,
                is_current,
                source_system,
                dw_created_at
            FROM CLEAN_SCHEMA.CLEAN_RESTAURANTS
        ) source
        ON target.restaurant_id = source.restaurant_id AND target.is_current = TRUE
        WHEN MATCHED AND (
            target.restaurant_name != source.restaurant_name OR
            target.cuisine_type != source.cuisine_type OR
            target.address != source.address OR
            target.city != source.city OR
            target.state != source.state OR
            target.country != source.country OR
            target.postal_code != source.postal_code OR
            target.latitude != source.latitude OR
            target.longitude != source.longitude OR
            target.phone_number != source.phone_number OR
            target.email != source.email OR
            target.operating_hours != source.operating_hours
        ) THEN
            UPDATE SET
                effective_to = CURRENT_TIMESTAMP(),
                is_current = FALSE
        WHEN NOT MATCHED THEN
            INSERT (
                restaurant_id,
                restaurant_name,
                cuisine_type,
                address,
                city,
                state,
                country,
                postal_code,
                latitude,
                longitude,
                phone_number,
                email,
                operating_hours,
                created_at,
                updated_at,
                effective_from,
                effective_to,
                is_current,
                source_system,
                dw_created_at
            )
            VALUES (
                source.restaurant_id,
                source.restaurant_name,
                source.cuisine_type,
                source.address,
                source.city,
                source.state,
                source.country,
                source.postal_code,
                source.latitude,
                source.longitude,
                source.phone_number,
                source.email,
                source.operating_hours,
                source.created_at,
                source.updated_at,
                CURRENT_TIMESTAMP(),
                NULL,
                TRUE,
                source.source_system,
                CURRENT_TIMESTAMP()
            )
        """
        self.snowflake.execute_query(restaurant_dimension_query)
        
        # Insert new records for updated rows
        restaurant_insert_query = """
        INSERT INTO CONSUMPTION_SCHEMA.DIM_RESTAURANT (
            restaurant_id,
            restaurant_name,
            cuisine_type,
            address,
            city,
            state,
            country,
            postal_code,
            latitude,
            longitude,
            phone_number,
            email,
            operating_hours,
            created_at,
            updated_at,
            effective_from,
            effective_to,
            is_current,
            source_system,
            dw_created_at
        )
        SELECT
            source.restaurant_id,
            source.restaurant_name,
            source.cuisine_type,
            source.address,
            source.city,
            source.state,
            source.country,
            source.postal_code,
            source.latitude,
            source.longitude,
            source.phone_number,
            source.email,
            source.operating_hours,
            source.created_at,
            source.updated_at,
            CURRENT_TIMESTAMP(),
            NULL,
            TRUE,
            source.source_system,
            CURRENT_TIMESTAMP()
        FROM CLEAN_SCHEMA.CLEAN_RESTAURANTS source
        JOIN CONSUMPTION_SCHEMA.DIM_RESTAURANT target
            ON target.restaurant_id = source.restaurant_id
            AND target.is_current = FALSE
            AND target.effective_to = CURRENT_TIMESTAMP()
        """
        self.snowflake.execute_query(restaurant_insert_query)
        
        # Similar SCD2 implementation for other dimension tables
        # DIM_PRODUCT
        # DIM_PROMOTION
        # DIM_CUSTOMER
        # DIM_DELIVERY_PERSON
        
        logger.info("Completed dimension table population with SCD2 logic")
    
    def populate_fact_tables(self) -> None:
        """Populate fact tables"""
        logger.info("Starting fact table population")
        
        # Populate FACT_ORDER
        fact_order_query = """
        INSERT INTO CONSUMPTION_SCHEMA.FACT_ORDER (
            order_sk,
            order_id,
            customer_sk,
            restaurant_sk,
            order_date_sk,
            order_time_sk,
            promotion_sk,
            order_status,
            delivery_address,
            delivery_city,
            delivery_state,
            delivery_postal_code,
            delivery_latitude,
            delivery_longitude,
            order_total,
            tax_amount,
            tip_amount,
            payment_method,
            payment_status,
            created_at,
            updated_at,
            source_system,
            dw_created_at
        )
        SELECT
            o.order_sk,
            o.order_id,
            c.customer_sk,
            r.restaurant_sk,
            d.date_sk AS order_date_sk,
            t.time_sk AS order_time_sk,
            p.promotion_sk,
            o.order_status,
            o.delivery_address,
            o.delivery_city,
            o.delivery_state,
            o.delivery_postal_code,
            o.delivery_latitude,
            o.delivery_longitude,
            o.order_total,
            o.tax_amount,
            o.tip_amount,
            o.payment_method,
            o.payment_status,
            o.created_at,
            o.updated_at,
            o.source_system,
            CURRENT_TIMESTAMP() AS dw_created_at
        FROM CLEAN_SCHEMA.CLEAN_ORDERS o
        LEFT JOIN CONSUMPTION_SCHEMA.DIM_CUSTOMER c
            ON o.customer_id = c.customer_id AND c.is_current = TRUE
        LEFT JOIN CONSUMPTION_SCHEMA.DIM_RESTAURANT r
            ON o.restaurant_id = r.restaurant_id AND r.is_current = TRUE
        LEFT JOIN CONSUMPTION_SCHEMA.DIM_DATE d
            ON DATE(o.order_date) = d.date_id
        LEFT JOIN CONSUMPTION_SCHEMA.DIM_TIME t
            ON TIME(o.order_date) = t.time_id
        LEFT JOIN CONSUMPTION_SCHEMA.DIM_PROMOTION p
            ON o.promotion_id = p.promotion_id AND p.is_current = TRUE
        """
        self.snowflake.execute_query(fact_order_query)
        
        # Populate FACT_ORDER_ITEM
        fact_order_item_query = """
        INSERT INTO CONSUMPTION_SCHEMA.FACT_ORDER_ITEM (
            order_item_sk,
            order_item_id,
            order_sk,
            product_sk,
            quantity,
            unit_price,
            item_total,
            special_instructions,
            created_at,
            updated_at,
            source_system,
            dw_created_at
        )
        SELECT
            oi.order_item_sk,
            oi.order_item_id,
            o.order_sk,
            p.product_sk,
            oi.quantity,
            oi.unit_price,
            oi.item_total,
            oi.special_instructions,
            oi.created_at,
            oi.updated_at,
            oi.source_system,
            CURRENT_TIMESTAMP() AS dw_created_at
        FROM CLEAN_SCHEMA.CLEAN_ORDER_ITEMS oi
        JOIN CONSUMPTION_SCHEMA.FACT_ORDER o
            ON oi.order_id = o.order_id
        LEFT JOIN CONSUMPTION_SCHEMA.DIM_PRODUCT p
            ON oi.product_id = p.product_id AND p.is_current = TRUE
        """
        self.snowflake.execute_query(fact_order_item_query)
        
        # Populate FACT_DELIVERY
        fact_delivery_query = """
        INSERT INTO CONSUMPTION_SCHEMA.FACT_DELIVERY (
            delivery_sk,
            delivery_id,
            order_sk,
            delivery_person_sk,
            delivery_status,
            pickup_date_sk,
            pickup_time_sk,
            delivery_date_sk,
            delivery_time_sk,
            estimated_delivery_time,
            actual_delivery_time,
            delivery_duration_minutes,
            delivery_distance,
            weather_conditions,
            traffic_density,
            created_at,
            updated_at,
            source_system,
            dw_created_at
        )
        SELECT
            d.delivery_sk,
            d.delivery_id,
            o.order_sk,
            dp.delivery_person_sk,
            d.delivery_status,
            pd.date_sk AS pickup_date_sk,
            pt.time_sk AS pickup_time_sk,
            dd.date_sk AS delivery_date_sk,
            dt.time_sk AS delivery_time_sk,
            d.estimated_delivery_time,
            d.actual_delivery_time,
            DATEDIFF(MINUTE, d.pickup_time, d.delivery_time) AS delivery_duration_minutes,
            d.delivery_distance,
            d.weather_conditions,
            d.traffic_density,
            d.created_at,
            d.updated_at,
            d.source_system,
            CURRENT_TIMESTAMP() AS dw_created_at
        FROM CLEAN_SCHEMA.CLEAN_DELIVERY d
        LEFT JOIN CONSUMPTION_SCHEMA.FACT_ORDER o
            ON d.order_id = o.order_id
        LEFT JOIN CONSUMPTION_SCHEMA.DIM_DELIVERY_PERSON dp
            ON d.delivery_person_id = dp.delivery_person_id AND dp.is_current = TRUE
        LEFT JOIN CONSUMPTION_SCHEMA.DIM_DATE pd
            ON DATE(d.pickup_time) = pd.date_id
        LEFT JOIN CONSUMPTION_SCHEMA.DIM_TIME pt
            ON TIME(d.pickup_time) = pt.time_id
        LEFT JOIN CONSUMPTION_SCHEMA.DIM_DATE dd
            ON DATE(d.delivery_time) = dd.date_id
        LEFT JOIN CONSUMPTION_SCHEMA.DIM_TIME dt
            ON TIME(d.delivery_time) = dt.time_id
        """
        self.snowflake.execute_query(fact_delivery_query)
        
        # Populate FACT_RATING
        fact_rating_query = """
        INSERT INTO CONSUMPTION_SCHEMA.FACT_RATING (
            rating_sk,
            rating_id,
            order_sk,
            customer_sk,
            restaurant_sk,
            delivery_person_sk,
            rating_date_sk,
            rating_time_sk,
            food_rating,
            delivery_rating,
            overall_rating,
            comments,
            created_at,
            source_system,
            dw_created_at
        )
        SELECT
            r.rating_sk,
            r.rating_id,
            o.order_sk,
            c.customer_sk,
            res.restaurant_sk,
            dp.delivery_person_sk,
            d.date_sk AS rating_date_sk,
            t.time_sk AS rating_time_sk,
            r.food_rating,
            r.delivery_rating,
            r.overall_rating,
            r.comments,
            r.created_at,
            r.source_system,
            CURRENT_TIMESTAMP() AS dw_created_at
        FROM CLEAN_SCHEMA.CLEAN_RATINGS r
        LEFT JOIN CONSUMPTION_SCHEMA.FACT_ORDER o
            ON r.order_id = o.order_id
        LEFT JOIN CONSUMPTION_SCHEMA.DIM_CUSTOMER c
            ON r.customer_id = c.customer_id AND c.is_current = TRUE
        LEFT JOIN CONSUMPTION_SCHEMA.DIM_RESTAURANT res
            ON r.restaurant_id = res.restaurant_id AND res.is_current = TRUE
        LEFT JOIN CONSUMPTION_SCHEMA.DIM_DELIVERY_PERSON dp
            ON r.delivery_person_id = dp.delivery_person_id AND dp.is_current = TRUE
        LEFT JOIN CONSUMPTION_SCHEMA.DIM_DATE d
            ON DATE(r.created_at) = d.date_id
        LEFT JOIN CONSUMPTION_SCHEMA.DIM_TIME t
            ON TIME(r.created_at) = t.time_id
        """
        self.snowflake.execute_query(fact_rating_query)
        
        logger.info("Completed fact table population")
    
    def setup_incremental_loads(self) -> None:
        """Set up Snowflake streams and tasks for incremental loads"""
        logger.info("Setting up incremental loads with streams and tasks")
        
        # Create streams on stage tables
        streams_query = """
        -- Create streams on stage tables to capture changes
        CREATE OR REPLACE STREAM STAGE_SCHEMA.RESTAURANT_STREAM ON TABLE STAGE_SCHEMA.STG_RESTAURANTS;
        CREATE OR REPLACE STREAM STAGE_SCHEMA.PRODUCT_STREAM ON TABLE STAGE_SCHEMA.STG_PRODUCTS;
        CREATE OR REPLACE STREAM STAGE_SCHEMA.PROMOTION_STREAM ON TABLE STAGE_SCHEMA.STG_PROMOTIONS;
        CREATE OR REPLACE STREAM STAGE_SCHEMA.CUSTOMER_STREAM ON TABLE STAGE_SCHEMA.STG_CUSTOMERS;
        CREATE OR REPLACE STREAM STAGE_SCHEMA.ORDER_STREAM ON TABLE STAGE_SCHEMA.STG_ORDERS;
        CREATE OR REPLACE STREAM STAGE_SCHEMA.ORDER_ITEM_STREAM ON TABLE STAGE_SCHEMA.STG_ORDER_ITEMS;
        CREATE OR REPLACE STREAM STAGE_SCHEMA.DELIVERY_STREAM ON TABLE STAGE_SCHEMA.STG_DELIVERY;
        CREATE OR REPLACE STREAM STAGE_SCHEMA.RATING_STREAM ON TABLE STAGE_SCHEMA.STG_RATINGS;
        """
        self.snowflake.execute_query(streams_query)
        
        # Create tasks to process streams
        tasks_query = """
        -- Create warehouse for tasks
        CREATE OR REPLACE WAREHOUSE ETL_WH WITH
            WAREHOUSE_SIZE = 'XSMALL'
            AUTO_SUSPEND = 60
            AUTO_RESUME = TRUE;
            
        -- Create task to process restaurant stream
        CREATE OR REPLACE TASK STAGE_SCHEMA.PROCESS_RESTAURANT_STREAM
            WAREHOUSE = ETL_WH
            SCHEDULE = 'USING CRON 0 */4 * * * UTC'
        WHEN
            SYSTEM$STREAM_HAS_DATA('STAGE_SCHEMA.RESTAURANT_STREAM')
        AS
        BEGIN
            -- Process changes to clean layer
            INSERT INTO CLEAN_SCHEMA.CLEAN_RESTAURANTS (
                restaurant_id,
                restaurant_name,
                cuisine_type,
                address,
                city,
                state,
                country,
                postal_code,
                latitude,
                longitude,
                phone_number,
                email,
                operating_hours,
                created_at,
                updated_at,
                effective_from,
                effective_to,
                is_current,
                source_system,
                dw_created_at
            )
            SELECT
                loc_number AS restaurant_id,
                loc_name AS restaurant_name,
                cuisines AS cuisine_type,
                address,
                searched_city AS city,
                searched_state AS state,
                'USA' AS country,
                searched_zipcode AS postal_code,
                latitude,
                longitude,
                phone AS phone_number,
                NULL AS email,
                NULL AS operating_hours,
                CURRENT_TIMESTAMP() AS created_at,
                CURRENT_TIMESTAMP() AS updated_at,
                CURRENT_TIMESTAMP() AS effective_from,
                NULL AS effective_to,
                TRUE AS is_current,
                source_system,
                CURRENT_TIMESTAMP() AS dw_created_at
            FROM STAGE_SCHEMA.RESTAURANT_STREAM
            WHERE METADATA$ACTION = 'INSERT';
            
            -- Apply SCD2 logic to dimension table
            MERGE INTO CONSUMPTION_SCHEMA.DIM_RESTAURANT target
            USING (
                SELECT 
                    restaurant_id,
                    restaurant_name,
                    cuisine_type,
                    address,
                    city,
                    state,
                    country,
                    postal_code,
                    latitude,
                    longitude,
                    phone_number,
                    email,
                    operating_hours,
                    created_at,
                    updated_at,
                    effective_from,
                    effective_to,
                    is_current,
                    source_system,
                    dw_created_at
                FROM CLEAN_SCHEMA.CLEAN_RESTAURANTS
                WHERE effective_from >= (SELECT NVL(MAX(dw_created_at), '1900-01-01'::TIMESTAMP_NTZ) FROM CONSUMPTION_SCHEMA.DIM_RESTAURANT)
            ) source
            ON target.restaurant_id = source.restaurant_id AND target.is_current = TRUE
            WHEN MATCHED AND (
                target.restaurant_name != source.restaurant_name OR
                target.cuisine_type != source.cuisine_type OR
                target.address != source.address OR
                target.city != source.city OR
                target.state != source.state OR
                target.country != source.country OR
                target.postal_code != source.postal_code OR
                target.latitude != source.latitude OR
                target.longitude != source.longitude OR
                target.phone_number != source.phone_number OR
                target.email != source.email OR
                target.operating_hours != source.operating_hours
            ) THEN
                UPDATE SET
                    effective_to = CURRENT_TIMESTAMP(),
                    is_current = FALSE
            WHEN NOT MATCHED THEN
                INSERT (
                    restaurant_id,
                    restaurant_name,
                    cuisine_type,
                    address,
                    city,
                    state,
                    country,
                    postal_code,
                    latitude,
                    longitude,
                    phone_number,
                    email,
                    operating_hours,
                    created_at,
                    updated_at,
                    effective_from,
                    effective_to,
                    is_current,
                    source_system,
                    dw_created_at
                )
                VALUES (
                    source.restaurant_id,
                    source.restaurant_name,
                    source.cuisine_type,
                    source.address,
                    source.city,
                    source.state,
                    source.country,
                    source.postal_code,
                    source.latitude,
                    source.longitude,
                    source.phone_number,
                    source.email,
                    source.operating_hours,
                    source.created_at,
                    source.updated_at,
                    CURRENT_TIMESTAMP(),
                    NULL,
                    TRUE,
                    source.source_system,
                    CURRENT_TIMESTAMP()
                );
                
            -- Insert new records for updated rows
            INSERT INTO CONSUMPTION_SCHEMA.DIM_RESTAURANT (
                restaurant_id,
                restaurant_name,
                cuisine_type,
                address,
                city,
                state,
                country,
                postal_code,
                latitude,
                longitude,
                phone_number,
                email,
                operating_hours,
                created_at,
                updated_at,
                effective_from,
                effective_to,
                is_current,
                source_system,
                dw_created_at
            )
            SELECT
                source.restaurant_id,
                source.restaurant_name,
                source.cuisine_type,
                source.address,
                source.city,
                source.state,
                source.country,
                source.postal_code,
                source.latitude,
                source.longitude,
                source.phone_number,
                source.email,
                source.operating_hours,
                source.created_at,
                source.updated_at,
                CURRENT_TIMESTAMP(),
                NULL,
                TRUE,
                source.source_system,
                CURRENT_TIMESTAMP()
            FROM CLEAN_SCHEMA.CLEAN_RESTAURANTS source
            JOIN CONSUMPTION_SCHEMA.DIM_RESTAURANT target
                ON target.restaurant_id = source.restaurant_id
                AND target.is_current = FALSE
                AND target.effective_to = CURRENT_TIMESTAMP();
        END;
        
        -- Similar tasks for other entities
        -- PROCESS_PRODUCT_STREAM
        -- PROCESS_PROMOTION_STREAM
        -- PROCESS_CUSTOMER_STREAM
        -- PROCESS_ORDER_STREAM
        -- PROCESS_ORDER_ITEM_STREAM
        -- PROCESS_DELIVERY_STREAM
        -- PROCESS_RATING_STREAM
        
        -- Enable tasks
        ALTER TASK STAGE_SCHEMA.PROCESS_RESTAURANT_STREAM RESUME;
        """
        self.snowflake.execute_query(tasks_query)
        
        logger.info("Completed setup for incremental loads")
    
    def run_etl_pipeline(self) -> None:
        """Run the complete ETL pipeline"""
        try:
            logger.info("Starting ETL pipeline")
            
            # Create schemas
            self.create_schemas()
            
            # Load data to stage tables
            self.load_stage_tables()
            
            # Transform data to clean layer
            self.transform_to_clean_layer()
            
            # Populate dimension tables with SCD2 logic
            self.populate_dimension_tables()
            
            # Populate fact tables
            self.populate_fact_tables()
            
            # Set up incremental loads
            self.setup_incremental_loads()
            
            logger.info("ETL pipeline completed successfully")
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise
        finally:
            # Close Snowflake connection
            self.snowflake.close()


if __name__ == "__main__":
    # Run the ETL pipeline
    etl = ETLPipeline(SNOWFLAKE_CONFIG, DATA_FILES)
    etl.run_etl_pipeline()

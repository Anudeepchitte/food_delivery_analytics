# Snowflake Data Warehouse Schema Design for Food Delivery Platform

## Overview
This document outlines the schema design for a food delivery platform data warehouse in Snowflake, following a multi-layer architecture with stage, clean, and consumption layers. The design implements dimensional modeling (star schema) with Slowly Changing Dimensions (SCD2) for tracking historical changes.

## Schema Layers

### 1. Stage Schema Layer (`STAGE_SCHEMA`)
The stage layer serves as the initial landing zone for raw data from source systems. Data is loaded here with minimal transformations to preserve the original structure.

```sql
-- Create Stage Schema
CREATE SCHEMA IF NOT EXISTS STAGE_SCHEMA;

-- Stage Tables
CREATE OR REPLACE TABLE STAGE_SCHEMA.STG_RESTAURANTS (
    restaurant_id VARCHAR,
    restaurant_name VARCHAR,
    cuisine_type VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    postal_code VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    phone_number VARCHAR,
    email VARCHAR,
    operating_hours VARCHAR,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    source_system VARCHAR,
    source_file VARCHAR,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE STAGE_SCHEMA.STG_PRODUCTS (
    product_id VARCHAR,
    restaurant_id VARCHAR,
    product_name VARCHAR,
    description VARCHAR,
    category VARCHAR,
    price FLOAT,
    cost FLOAT,
    is_vegetarian BOOLEAN,
    is_vegan BOOLEAN,
    is_gluten_free BOOLEAN,
    calories INTEGER,
    preparation_time INTEGER,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    source_system VARCHAR,
    source_file VARCHAR,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE STAGE_SCHEMA.STG_PROMOTIONS (
    promotion_id VARCHAR,
    promotion_name VARCHAR,
    description VARCHAR,
    discount_type VARCHAR,
    discount_value FLOAT,
    start_date DATE,
    end_date DATE,
    min_order_value FLOAT,
    max_discount FLOAT,
    restaurant_id VARCHAR,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    source_system VARCHAR,
    source_file VARCHAR,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE STAGE_SCHEMA.STG_CUSTOMERS (
    customer_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    phone_number VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    postal_code VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    referral_customer_id VARCHAR,
    source_system VARCHAR,
    source_file VARCHAR,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE STAGE_SCHEMA.STG_ORDERS (
    order_id VARCHAR,
    customer_id VARCHAR,
    restaurant_id VARCHAR,
    order_date TIMESTAMP_NTZ,
    order_status VARCHAR,
    delivery_address VARCHAR,
    delivery_city VARCHAR,
    delivery_state VARCHAR,
    delivery_postal_code VARCHAR,
    delivery_latitude FLOAT,
    delivery_longitude FLOAT,
    order_total FLOAT,
    tax_amount FLOAT,
    tip_amount FLOAT,
    promotion_id VARCHAR,
    payment_method VARCHAR,
    payment_status VARCHAR,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    source_system VARCHAR,
    source_file VARCHAR,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE STAGE_SCHEMA.STG_ORDER_ITEMS (
    order_item_id VARCHAR,
    order_id VARCHAR,
    product_id VARCHAR,
    quantity INTEGER,
    unit_price FLOAT,
    item_total FLOAT,
    special_instructions VARCHAR,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    source_system VARCHAR,
    source_file VARCHAR,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE STAGE_SCHEMA.STG_DELIVERY (
    delivery_id VARCHAR,
    order_id VARCHAR,
    delivery_person_id VARCHAR,
    delivery_status VARCHAR,
    pickup_time TIMESTAMP_NTZ,
    delivery_time TIMESTAMP_NTZ,
    estimated_delivery_time TIMESTAMP_NTZ,
    actual_delivery_time TIMESTAMP_NTZ,
    delivery_distance FLOAT,
    weather_conditions VARCHAR,
    traffic_density VARCHAR,
    vehicle_type VARCHAR,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    source_system VARCHAR,
    source_file VARCHAR,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE STAGE_SCHEMA.STG_RATINGS (
    rating_id VARCHAR,
    order_id VARCHAR,
    customer_id VARCHAR,
    restaurant_id VARCHAR,
    delivery_person_id VARCHAR,
    food_rating INTEGER,
    delivery_rating INTEGER,
    overall_rating INTEGER,
    comments VARCHAR,
    created_at TIMESTAMP_NTZ,
    source_system VARCHAR,
    source_file VARCHAR,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### 2. Clean Schema Layer (`CLEAN_SCHEMA`)
The clean layer contains transformed and validated data, with standardized formats and business rules applied.

```sql
-- Create Clean Schema
CREATE SCHEMA IF NOT EXISTS CLEAN_SCHEMA;

-- Clean Tables
CREATE OR REPLACE TABLE CLEAN_SCHEMA.CLEAN_RESTAURANTS (
    restaurant_sk INTEGER AUTOINCREMENT,
    restaurant_id VARCHAR,
    restaurant_name VARCHAR,
    cuisine_type VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    postal_code VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    phone_number VARCHAR,
    email VARCHAR,
    operating_hours VARCHAR,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    effective_from TIMESTAMP_NTZ,
    effective_to TIMESTAMP_NTZ,
    is_current BOOLEAN,
    source_system VARCHAR,
    dw_created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE CLEAN_SCHEMA.CLEAN_PRODUCTS (
    product_sk INTEGER AUTOINCREMENT,
    product_id VARCHAR,
    restaurant_id VARCHAR,
    product_name VARCHAR,
    description VARCHAR,
    category VARCHAR,
    price FLOAT,
    cost FLOAT,
    is_vegetarian BOOLEAN,
    is_vegan BOOLEAN,
    is_gluten_free BOOLEAN,
    calories INTEGER,
    preparation_time INTEGER,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    effective_from TIMESTAMP_NTZ,
    effective_to TIMESTAMP_NTZ,
    is_current BOOLEAN,
    source_system VARCHAR,
    dw_created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE CLEAN_SCHEMA.CLEAN_PROMOTIONS (
    promotion_sk INTEGER AUTOINCREMENT,
    promotion_id VARCHAR,
    promotion_name VARCHAR,
    description VARCHAR,
    discount_type VARCHAR,
    discount_value FLOAT,
    start_date DATE,
    end_date DATE,
    min_order_value FLOAT,
    max_discount FLOAT,
    restaurant_id VARCHAR,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    effective_from TIMESTAMP_NTZ,
    effective_to TIMESTAMP_NTZ,
    is_current BOOLEAN,
    source_system VARCHAR,
    dw_created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE CLEAN_SCHEMA.CLEAN_CUSTOMERS (
    customer_sk INTEGER AUTOINCREMENT,
    customer_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    phone_number VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    postal_code VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    referral_customer_id VARCHAR,
    effective_from TIMESTAMP_NTZ,
    effective_to TIMESTAMP_NTZ,
    is_current BOOLEAN,
    source_system VARCHAR,
    dw_created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE CLEAN_SCHEMA.CLEAN_ORDERS (
    order_sk INTEGER AUTOINCREMENT,
    order_id VARCHAR,
    customer_id VARCHAR,
    restaurant_id VARCHAR,
    order_date TIMESTAMP_NTZ,
    order_status VARCHAR,
    delivery_address VARCHAR,
    delivery_city VARCHAR,
    delivery_state VARCHAR,
    delivery_postal_code VARCHAR,
    delivery_latitude FLOAT,
    delivery_longitude FLOAT,
    order_total FLOAT,
    tax_amount FLOAT,
    tip_amount FLOAT,
    promotion_id VARCHAR,
    payment_method VARCHAR,
    payment_status VARCHAR,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    source_system VARCHAR,
    dw_created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE CLEAN_SCHEMA.CLEAN_ORDER_ITEMS (
    order_item_sk INTEGER AUTOINCREMENT,
    order_item_id VARCHAR,
    order_id VARCHAR,
    product_id VARCHAR,
    quantity INTEGER,
    unit_price FLOAT,
    item_total FLOAT,
    special_instructions VARCHAR,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    source_system VARCHAR,
    dw_created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE CLEAN_SCHEMA.CLEAN_DELIVERY (
    delivery_sk INTEGER AUTOINCREMENT,
    delivery_id VARCHAR,
    order_id VARCHAR,
    delivery_person_id VARCHAR,
    delivery_status VARCHAR,
    pickup_time TIMESTAMP_NTZ,
    delivery_time TIMESTAMP_NTZ,
    estimated_delivery_time TIMESTAMP_NTZ,
    actual_delivery_time TIMESTAMP_NTZ,
    delivery_distance FLOAT,
    weather_conditions VARCHAR,
    traffic_density VARCHAR,
    vehicle_type VARCHAR,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    source_system VARCHAR,
    dw_created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE CLEAN_SCHEMA.CLEAN_RATINGS (
    rating_sk INTEGER AUTOINCREMENT,
    rating_id VARCHAR,
    order_id VARCHAR,
    customer_id VARCHAR,
    restaurant_id VARCHAR,
    delivery_person_id VARCHAR,
    food_rating INTEGER,
    delivery_rating INTEGER,
    overall_rating INTEGER,
    comments VARCHAR,
    created_at TIMESTAMP_NTZ,
    source_system VARCHAR,
    dw_created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### 3. Consumption Schema Layer (`CONSUMPTION_SCHEMA`)
The consumption layer implements a dimensional model (star schema) optimized for analytics and reporting.

```sql
-- Create Consumption Schema
CREATE SCHEMA IF NOT EXISTS CONSUMPTION_SCHEMA;

-- Dimension Tables
CREATE OR REPLACE TABLE CONSUMPTION_SCHEMA.DIM_DATE (
    date_sk INTEGER PRIMARY KEY,
    date_id DATE,
    day_of_week INTEGER,
    day_of_week_name VARCHAR,
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    month_number INTEGER,
    month_name VARCHAR,
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name VARCHAR,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
);

CREATE OR REPLACE TABLE CONSUMPTION_SCHEMA.DIM_TIME (
    time_sk INTEGER PRIMARY KEY,
    time_id TIME,
    hour_of_day INTEGER,
    minute_of_hour INTEGER,
    second_of_minute INTEGER,
    am_pm VARCHAR,
    time_of_day_category VARCHAR -- 'Morning', 'Afternoon', 'Evening', 'Night'
);

CREATE OR REPLACE TABLE CONSUMPTION_SCHEMA.DIM_RESTAURANT (
    restaurant_sk INTEGER PRIMARY KEY,
    restaurant_id VARCHAR,
    restaurant_name VARCHAR,
    cuisine_type VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    postal_code VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    phone_number VARCHAR,
    email VARCHAR,
    operating_hours VARCHAR,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    effective_from TIMESTAMP_NTZ,
    effective_to TIMESTAMP_NTZ,
    is_current BOOLEAN,
    source_system VARCHAR,
    dw_created_at TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE CONSUMPTION_SCHEMA.DIM_PRODUCT (
    product_sk INTEGER PRIMARY KEY,
    product_id VARCHAR,
    restaurant_sk INTEGER,
    restaurant_id VARCHAR,
    product_name VARCHAR,
    description VARCHAR,
    category VARCHAR,
    price FLOAT,
    cost FLOAT,
    is_vegetarian BOOLEAN,
    is_vegan BOOLEAN,
    is_gluten_free BOOLEAN,
    calories INTEGER,
    preparation_time INTEGER,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    effective_from TIMESTAMP_NTZ,
    effective_to TIMESTAMP_NTZ,
    is_current BOOLEAN,
    source_system VARCHAR,
    dw_created_at TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE CONSUMPTION_SCHEMA.DIM_PROMOTION (
    promotion_sk INTEGER PRIMARY KEY,
    promotion_id VARCHAR,
    promotion_name VARCHAR,
    description VARCHAR,
    discount_type VARCHAR,
    discount_value FLOAT,
    start_date DATE,
    end_date DATE,
    min_order_value FLOAT,
    max_discount FLOAT,
    restaurant_sk INTEGER,
    restaurant_id VARCHAR,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    effective_from TIMESTAMP_NTZ,
    effective_to TIMESTAMP_NTZ,
    is_current BOOLEAN,
    source_system VARCHAR,
    dw_created_at TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE CONSUMPTION_SCHEMA.DIM_CUSTOMER (
    customer_sk INTEGER PRIMARY KEY,
    customer_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    phone_number VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    postal_code VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    referral_customer_id VARCHAR,
    effective_from TIMESTAMP_NTZ,
    effective_to TIMESTAMP_NTZ,
    is_current BOOLEAN,
    source_system VARCHAR,
    dw_created_at TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE CONSUMPTION_SCHEMA.DIM_DELIVERY_PERSON (
    delivery_person_sk INTEGER PRIMARY KEY,
    delivery_person_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    phone_number VARCHAR,
    email VARCHAR,
    vehicle_type VARCHAR,
    rating FLOAT,
    active_since DATE,
    is_active BOOLEAN,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    effective_from TIMESTAMP_NTZ,
    effective_to TIMESTAMP_NTZ,
    is_current BOOLEAN,
    source_system VARCHAR,
    dw_created_at TIMESTAMP_NTZ
);

-- Fact Tables
CREATE OR REPLACE TABLE CONSUMPTION_SCHEMA.FACT_ORDER (
    order_sk INTEGER PRIMARY KEY,
    order_id VARCHAR,
    customer_sk INTEGER,
    restaurant_sk INTEGER,
    order_date_sk INTEGER,
    order_time_sk INTEGER,
    promotion_sk INTEGER,
    order_status VARCHAR,
    delivery_address VARCHAR,
    delivery_city VARCHAR,
    delivery_state VARCHAR,
    delivery_postal_code VARCHAR,
    delivery_latitude FLOAT,
    delivery_longitude FLOAT,
    order_total FLOAT,
    tax_amount FLOAT,
    tip_amount FLOAT,
    payment_method VARCHAR,
    payment_status VARCHAR,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    source_system VARCHAR,
    dw_created_at TIMESTAMP_NTZ,
    FOREIGN KEY (customer_sk) REFERENCES CONSUMPTION_SCHEMA.DIM_CUSTOMER(customer_sk),
    FOREIGN KEY (restaurant_sk) REFERENCES CONSUMPTION_SCHEMA.DIM_RESTAURANT(restaurant_sk),
    FOREIGN KEY (order_date_sk) REFERENCES CONSUMPTION_SCHEMA.DIM_DATE(date_sk),
    FOREIGN KEY (order_time_sk) REFERENCES CONSUMPTION_SCHEMA.DIM_TIME(time_sk),
    FOREIGN KEY (promotion_sk) REFERENCES CONSUMPTION_SCHEMA.DIM_PROMOTION(promotion_sk)
);

CREATE OR REPLACE TABLE CONSUMPTION_SCHEMA.FACT_ORDER_ITEM (
    order_item_sk INTEGER PRIMARY KEY,
    order_item_id VARCHAR,
    order_sk INTEGER,
    product_sk INTEGER,
    quantity INTEGER,
    unit_price FLOAT,
    item_total FLOAT,
    special_instructions VARCHAR,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    source_system VARCHAR,
    dw_created_at TIMESTAMP_NTZ,
    FOREIGN KEY (order_sk) REFERENCES CONSUMPTION_SCHEMA.FACT_ORDER(order_sk),
    FOREIGN KEY (product_sk) REFERENCES CONSUMPTION_SCHEMA.DIM_PRODUCT(product_sk)
);

CREATE OR REPLACE TABLE CONSUMPTION_SCHEMA.FACT_DELIVERY (
    delivery_sk INTEGER PRIMARY KEY,
    delivery_id VARCHAR,
    order_sk INTEGER,
    delivery_person_sk INTEGER,
    delivery_status VARCHAR,
    pickup_date_sk INTEGER,
    pickup_time_sk INTEGER,
    delivery_date_sk INTEGER,
    delivery_time_sk INTEGER,
    estimated_delivery_time TIMESTAMP_NTZ,
    actual_delivery_time TIMESTAMP_NTZ,
    delivery_duration_minutes INTEGER,
    delivery_distance FLOAT,
    weather_conditions VARCHAR,
    traffic_density VARCHAR,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    source_system VARCHAR,
    dw_created_at TIMESTAMP_NTZ,
    FOREIGN KEY (order_sk) REFERENCES CONSUMPTION_SCHEMA.FACT_ORDER(order_sk),
    FOREIGN KEY (delivery_person_sk) REFERENCES CONSUMPTION_SCHEMA.DIM_DELIVERY_PERSON(delivery_person_sk),
    FOREIGN KEY (pickup_date_sk) REFERENCES CONSUMPTION_SCHEMA.DIM_DATE(date_sk),
    FOREIGN KEY (pickup_time_sk) REFERENCES CONSUMPTION_SCHEMA.DIM_TIME(time_sk),
    FOREIGN KEY (delivery_date_sk) REFERENCES CONSUMPTION_SCHEMA.DIM_DATE(date_sk),
    FOREIGN KEY (delivery_time_sk) REFERENCES CONSUMPTION_SCHEMA.DIM_TIME(time_sk)
);

CREATE OR REPLACE TABLE CONSUMPTION_SCHEMA.FACT_RATING (
    rating_sk INTEGER PRIMARY KEY,
    rating_id VARCHAR,
    order_sk INTEGER,
    customer_sk INTEGER,
    restaurant_sk INTEGER,
    delivery_person_sk INTEGER,
    rating_date_sk INTEGER,
    rating_time_sk INTEGER,
    food_rating INTEGER,
    delivery_rating INTEGER,
    overall_rating INTEGER,
    comments VARCHAR,
    created_at TIMESTAMP_NTZ,
    source_system VARCHAR,
    dw_created_at TIMESTAMP_NTZ,
    FOREIGN KEY (order_sk) REFERENCES CONSUMPTION_SCHEMA.FACT_ORDER(order_sk),
    FOREIGN KEY (customer_sk) REFERENCES CONSUMPTION_SCHEMA.DIM_CUSTOMER(customer_sk),
    FOREIGN KEY (restaurant_sk) REFERENCES CONSUMPTION_SCHEMA.DIM_RESTAURANT(restaurant_sk),
    FOREIGN KEY (delivery_person_sk) REFERENCES CONSUMPTION_SCHEMA.DIM_DELIVERY_PERSON(delivery_person_sk),
    FOREIGN KEY (rating_date_sk) REFERENCES CONSUMPTION_SCHEMA.DIM_DATE(date_sk),
    FOREIGN KEY (rating_time_sk) REFERENCES CONSUMPTION_SCHEMA.DIM_TIME(time_sk)
);
```

## SCD Type 2 Implementation

For dimension tables that require historical tracking (SCD Type 2), we implement the following approach:

1. **Tracking Columns**: Each dimension table includes:
   - `effective_from`: Timestamp when the record becomes active
   - `effective_to`: Timestamp when the record becomes inactive (NULL for current records)
   - `is_current`: Boolean flag indicating if this is the current version (TRUE/FALSE)

2. **SCD2 Process**:
   - When a dimension record changes, the current record is expired by setting `effective_to` to the current timestamp and `is_current` to FALSE
   - A new record is inserted with updated values, `effective_from` set to the current timestamp, `effective_to` set to NULL, and `is_current` set to TRUE
   - The surrogate key (`_sk` columns) is incremented for the new record

3. **Example Merge Statement for SCD2 Updates**:

```sql
-- Example SCD2 merge statement for DIM_RESTAURANT
MERGE INTO CONSUMPTION_SCHEMA.DIM_RESTAURANT target
USING (
    SELECT 
        r.restaurant_id,
        r.restaurant_name,
        r.cuisine_type,
        r.address,
        r.city,
        r.state,
        r.country,
        r.postal_code,
        r.latitude,
        r.longitude,
        r.phone_number,
        r.email,
        r.operating_hours,
        r.created_at,
        r.updated_at,
        r.source_system,
        r.dw_created_at
    FROM CLEAN_SCHEMA.CLEAN_RESTAURANTS r
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
```

## Data Flow and ETL Process

1. **Initial Load**:
   - Raw data is loaded into the Stage schema tables
   - Data is transformed and loaded into Clean schema tables
   - Dimension and fact tables in the Consumption schema are populated

2. **Incremental Updates**:
   - New data is loaded into Stage schema tables
   - Changes are identified and applied to Clean schema tables
   - SCD Type 2 logic is applied to dimension tables in the Consumption schema
   - Fact tables are updated with new transactions

3. **Snowflake Streams and Tasks**:
   - Streams capture changes in Stage tables
   - Tasks process these changes and update Clean and Consumption layers
   - Scheduled tasks automate the incremental load process

## Analytics Views

```sql
-- Example analytics views for business insights

-- View for Order Performance by Restaurant
CREATE OR REPLACE VIEW CONSUMPTION_SCHEMA.VW_RESTAURANT_ORDER_PERFORMANCE AS
SELECT
    r.restaurant_name,
    r.cuisine_type,
    r.city,
    r.state,
    d.year,
    d.month_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.order_total) AS total_revenue,
    AVG(o.order_total) AS avg_order_value,
    SUM(o.tip_amount) AS total_tips,
    AVG(o.tip_amount) AS avg_tip_amount
FROM CONSUMPTION_SCHEMA.FACT_ORDER o
JOIN CONSUMPTION_SCHEMA.DIM_RESTAURANT r ON o.restaurant_sk = r.restaurant_sk
JOIN CONSUMPTION_SCHEMA.DIM_DATE d ON o.order_date_sk = d.date_sk
WHERE r.is_current = TRUE
GROUP BY 1, 2, 3, 4, 5, 6;

-- View for Delivery Performance
CREATE OR REPLACE VIEW CONSUMPTION_SCHEMA.VW_DELIVERY_PERFORMANCE AS
SELECT
    dp.delivery_person_id,
    d.year,
    d.month_name,
    COUNT(fd.delivery_id) AS total_deliveries,
    AVG(fd.delivery_duration_minutes) AS avg_delivery_time,
    AVG(fd.delivery_distance) AS avg_delivery_distance,
    AVG(fr.delivery_rating) AS avg_delivery_rating
FROM CONSUMPTION_SCHEMA.FACT_DELIVERY fd
JOIN CONSUMPTION_SCHEMA.DIM_DELIVERY_PERSON dp ON fd.delivery_person_sk = dp.delivery_person_sk
JOIN CONSUMPTION_SCHEMA.DIM_DATE d ON fd.delivery_date_sk = d.date_sk
LEFT JOIN CONSUMPTION_SCHEMA.FACT_RATING fr ON fd.order_sk = fr.order_sk
WHERE dp.is_current = TRUE
GROUP BY 1, 2, 3;

-- View for Customer Order Patterns
CREATE OR REPLACE VIEW CONSUMPTION_SCHEMA.VW_CUSTOMER_ORDER_PATTERNS AS
SELECT
    c.customer_id,
    c.city,
    c.state,
    d.year,
    d.month_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.order_total) AS total_spent,
    AVG(o.order_total) AS avg_order_value,
    COUNT(DISTINCT r.restaurant_id) AS unique_restaurants_ordered
FROM CONSUMPTION_SCHEMA.FACT_ORDER o
JOIN CONSUMPTION_SCHEMA.DIM_CUSTOMER c ON o.customer_sk = c.customer_sk
JOIN CONSUMPTION_SCHEMA.DIM_DATE d ON o.order_date_sk = d.date_sk
JOIN CONSUMPTION_SCHEMA.DIM_RESTAURANT r ON o.restaurant_sk = r.restaurant_sk
WHERE c.is_current = TRUE AND r.is_current = TRUE
GROUP BY 1, 2, 3, 4, 5;

-- View for Product Performance
CREATE OR REPLACE VIEW CONSUMPTION_SCHEMA.VW_PRODUCT_PERFORMANCE AS
SELECT
    p.product_name,
    p.category,
    r.restaurant_name,
    d.year,
    d.month_name,
    SUM(oi.quantity) AS total_quantity_sold,
    SUM(oi.item_total) AS total_revenue,
    COUNT(DISTINCT o.order_id) AS order_count
FROM CONSUMPTION_SCHEMA.FACT_ORDER_ITEM oi
JOIN CONSUMPTION_SCHEMA.DIM_PRODUCT p ON oi.product_sk = p.product_sk
JOIN CONSUMPTION_SCHEMA.FACT_ORDER o ON oi.order_sk = o.order_sk
JOIN CONSUMPTION_SCHEMA.DIM_RESTAURANT r ON p.restaurant_sk = r.restaurant_sk
JOIN CONSUMPTION_SCHEMA.DIM_DATE d ON o.order_date_sk = d.date_sk
WHERE p.is_current = TRUE AND r.is_current = TRUE
GROUP BY 1, 2, 3, 4, 5;

-- View for Promotion Effectiveness
CREATE OR REPLACE VIEW CONSUMPTION_SCHEMA.VW_PROMOTION_EFFECTIVENESS AS
SELECT
    p.promotion_name,
    p.discount_type,
    p.discount_value,
    d.year,
    d.month_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.order_total) AS total_revenue,
    AVG(o.order_total) AS avg_order_value,
    SUM(o.order_total) / COUNT(DISTINCT o.order_id) AS revenue_per_order
FROM CONSUMPTION_SCHEMA.FACT_ORDER o
JOIN CONSUMPTION_SCHEMA.DIM_PROMOTION p ON o.promotion_sk = p.promotion_sk
JOIN CONSUMPTION_SCHEMA.DIM_DATE d ON o.order_date_sk = d.date_sk
WHERE p.is_current = TRUE
GROUP BY 1, 2, 3, 4, 5;
```

## Schema Diagram

The schema follows a star schema design with dimension tables surrounding fact tables:

- **Dimension Tables**: DIM_DATE, DIM_TIME, DIM_RESTAURANT, DIM_PRODUCT, DIM_PROMOTION, DIM_CUSTOMER, DIM_DELIVERY_PERSON
- **Fact Tables**: FACT_ORDER, FACT_ORDER_ITEM, FACT_DELIVERY, FACT_RATING

Key relationships:
- FACT_ORDER connects to dimension tables via surrogate keys
- FACT_ORDER_ITEM connects to FACT_ORDER and DIM_PRODUCT
- FACT_DELIVERY connects to FACT_ORDER and DIM_DELIVERY_PERSON
- FACT_RATING connects to FACT_ORDER, DIM_CUSTOMER, DIM_RESTAURANT, and DIM_DELIVERY_PERSON

## Conclusion

This schema design provides a comprehensive data warehouse structure for a food delivery platform, supporting:

1. Efficient data loading through the stage layer
2. Data quality and transformation in the clean layer
3. Dimensional modeling in the consumption layer for analytics
4. Historical tracking through SCD Type 2 implementation
5. Business insights through pre-built analytics views

The design can be extended with additional dimensions or measures as business requirements evolve.

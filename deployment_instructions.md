# Food Delivery Data Engineering Project - Deployment Instructions

## Project Overview

This document provides instructions for deploying and maintaining the Food Delivery Data Engineering Project, which consists of:

1. A Snowflake data warehouse with dimensional modeling (star schema)
2. ETL pipeline with SCD2 implementation for dimension tables
3. Streamlit dashboard for visualizing food order booking and receiving flow

## Data Warehouse Setup

### Snowflake Configuration

1. Create a Snowflake account if you don't have one already
2. Set up the required schemas as defined in `snowflake_schema_design.md`:
   - `STAGE_SCHEMA`: Initial landing zone for raw data
   - `CLEAN_SCHEMA`: Transformed and validated data
   - `CONSUMPTION_SCHEMA`: Dimensional model (star schema) for analytics

3. Create a dedicated warehouse for ETL operations:
```sql
CREATE WAREHOUSE ETL_WH WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;
```

4. Create a dedicated warehouse for dashboard queries:
```sql
CREATE WAREHOUSE DASHBOARD_WH WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;
```

5. Set up appropriate roles and access controls for ETL processes and dashboard users

## ETL Pipeline Deployment

### Initial Setup

1. Install required Python packages:
```bash
pip install snowflake-connector-python pandas
```

2. Configure Snowflake connection parameters in a secure environment file or secrets manager:
```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=ETL_WH
SNOWFLAKE_DATABASE=FOOD_DELIVERY_DW
SNOWFLAKE_ROLE=your_role
```

3. Update the `DATA_FILES` dictionary in `etl_implementation.py` to point to your raw data file locations

### Running the ETL Pipeline

1. For initial data load:
```bash
python etl_implementation.py
```

2. For scheduled incremental loads, set up a cron job or scheduler:
```bash
# Example cron entry for daily ETL run at 2 AM
0 2 * * * /path/to/python /path/to/etl_implementation.py >> /path/to/etl_logs/etl_$(date +\%Y\%m\%d).log 2>&1
```

### Monitoring and Maintenance

1. Monitor ETL logs for errors and performance issues
2. Set up alerts for failed ETL jobs
3. Periodically review and optimize Snowflake resource usage
4. Update the ETL scripts as data sources or business requirements change

## Streamlit Dashboard Deployment

### Local Development

1. Install required Python packages:
```bash
pip install streamlit pandas plotly numpy snowflake-connector-python
```

2. Run the dashboard locally:
```bash
streamlit run streamlit_dashboard.py
```

### Production Deployment

1. Set up a dedicated server or cloud instance for the dashboard
2. Install required dependencies:
```bash
pip install streamlit pandas plotly numpy snowflake-connector-python
```

3. Configure Snowflake connection parameters securely
4. Set up a service to run the Streamlit app:
```bash
# Example systemd service file
[Unit]
Description=Food Delivery Dashboard
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu
ExecStart=/usr/local/bin/streamlit run /home/ubuntu/streamlit_dashboard.py --server.port 8501
Restart=always
Environment="SNOWFLAKE_ACCOUNT=your_account"
Environment="SNOWFLAKE_USER=your_user"
Environment="SNOWFLAKE_PASSWORD=your_password"
Environment="SNOWFLAKE_WAREHOUSE=DASHBOARD_WH"
Environment="SNOWFLAKE_DATABASE=FOOD_DELIVERY_DW"
Environment="SNOWFLAKE_ROLE=your_role"

[Install]
WantedBy=multi-user.target
```

5. Enable and start the service:
```bash
sudo systemctl enable food-delivery-dashboard
sudo systemctl start food-delivery-dashboard
```

6. Set up a reverse proxy (Nginx, Apache) to handle HTTPS and domain routing

### Current Deployment

The dashboard is currently deployed and accessible at:
https://8501-ipqlzifsvoof8bm7rh6ll-c0653f8b.manusvm.computer

Note: This is a temporary URL for demonstration purposes. For production use, set up a permanent deployment as described above.

## Data Refresh and Maintenance

### Incremental Data Loading

The ETL pipeline is configured to use Snowflake streams and tasks for incremental data loading:

1. Streams capture changes in stage tables
2. Tasks process these changes and update clean and consumption layers
3. Tasks are scheduled to run every 4 hours

### Manual Data Refresh

If needed, you can trigger a manual data refresh:

1. Connect to Snowflake
2. Execute the following command to run the task manually:
```sql
EXECUTE TASK STAGE_SCHEMA.PROCESS_RESTAURANT_STREAM;
-- Repeat for other entity tasks as needed
```

## Troubleshooting

### Common Issues

1. **ETL Pipeline Failures**
   - Check ETL logs for specific error messages
   - Verify Snowflake connection parameters
   - Ensure raw data files are in the expected format and location

2. **Dashboard Connection Issues**
   - Verify Snowflake credentials and connection parameters
   - Check network connectivity between the dashboard server and Snowflake
   - Review Streamlit server logs for errors

3. **Performance Issues**
   - Monitor Snowflake query performance and optimize as needed
   - Consider scaling up the Snowflake warehouse size during peak usage
   - Implement caching for frequently accessed dashboard data

## Contact and Support

For questions or issues, please contact the project maintainer.

---

Last Updated: May 28, 2025

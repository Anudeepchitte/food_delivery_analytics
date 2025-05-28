# Food Delivery Data Engineering Project

This repository contains a complete data engineering solution for a food delivery platform, including:

1. Snowflake data warehouse schema design
2. ETL pipeline implementation with SCD2
3. Interactive Streamlit dashboard

## Dashboard Features

- Order booking and receiving flow visualization
- Restaurant performance metrics
- Delivery analytics by weather and traffic conditions
- Promotion effectiveness analysis
- Interactive filters for date range, city, cuisine, and weather

## Project Structure

- `streamlit_dashboard.py`: Main Streamlit application code
- `requirements.txt`: Python dependencies
- `data/`: Sample data files (if using GitHub for data storage)

## Deployment Instructions

### Prerequisites

- Python 3.8+
- Streamlit account (for cloud deployment)

### Local Setup

1. Clone this repository:
```bash
git clone https://github.com/YOUR_USERNAME/food-delivery-analytics.git
cd food-delivery-analytics
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the Streamlit app:
```bash
streamlit run streamlit_dashboard.py
```

### Streamlit Cloud Deployment

1. Fork or clone this repository to your GitHub account
2. Sign in to [Streamlit Cloud](https://streamlit.io/cloud)
3. Create a new app, selecting this repository
4. Set the main file path to `streamlit_dashboard.py`
5. Deploy!

## Data Sources

This project uses data from various sources including:
- Restaurant data from food delivery platforms
- Order and delivery metrics
- Synthetic promotion data

## Future Enhancements

- Integration with live Snowflake data warehouse
- Additional analytics views
- User authentication for dashboard access
- Mobile-optimized views

## License

MIT

## Contact

For questions or support, please open an issue in this repository.

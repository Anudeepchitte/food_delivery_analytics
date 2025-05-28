import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import os
import json
from datetime import datetime, timedelta
import random

# Set page configuration
st.set_page_config(
    page_title="Food Delivery Analytics Dashboard",
    page_icon="🍔",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #FF5722;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        font-weight: 600;
        color: #2E7D32;
        margin-bottom: 1rem;
    }
    .card {
        border-radius: 5px;
        padding: 1rem;
        background-color: #f9f9f9;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 1rem;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: #1976D2;
    }
    .metric-label {
        font-size: 1rem;
        color: #616161;
    }
    .trend-up {
        color: #4CAF50;
    }
    .trend-down {
        color: #F44336;
    }
    .footer {
        margin-top: 3rem;
        text-align: center;
        color: #9E9E9E;
        font-size: 0.8rem;
    }
</style>
""", unsafe_allow_html=True)

# Function to load data
@st.cache_data(ttl=3600)
def load_data():
    """
    In a real implementation, this would connect to Snowflake and pull data.
    For this demo, we'll simulate data based on our collected datasets.
    """
    # Load restaurant data
    try:
        restaurants_df = pd.read_csv('/home/ubuntu/food_delivery_data/UberEats_restaurants.csv')
    except:
        # Create sample data if file doesn't exist
        restaurants_df = pd.DataFrame({
            'loc_number': range(1, 101),
            'loc_name': [f"Restaurant {i}" for i in range(1, 101)],
            'cuisines': np.random.choice(['Italian', 'Chinese', 'Indian', 'Mexican', 'American'], 100),
            'searched_city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'], 100),
            'rating': np.random.uniform(3.0, 5.0, 100).round(1)
        })
    
    # Load order data
    try:
        orders_df = pd.read_csv('/home/ubuntu/food_delivery_data/orders.csv')
    except:
        # Create sample order data
        orders_df = pd.DataFrame({
            'id': range(1, 1001),
            'customer_id': np.random.randint(1, 101, 1000),
            'created_at': [datetime.now() - timedelta(days=np.random.randint(0, 90)) for _ in range(1000)],
            'updated_status': np.random.choice(['Delivered', 'Cancelled', 'In Progress', 'Preparing'], 1000),
            'tips': np.random.uniform(0, 15, 1000).round(2),
            'rating': np.random.choice([None, 3, 4, 5], 1000, p=[0.2, 0.2, 0.3, 0.3])
        })
        orders_df['created_at'] = pd.to_datetime(orders_df['created_at'])
    
    # Load delivery data
    try:
        delivery_df = pd.read_csv('/home/ubuntu/food_delivery_data/delivery_data.csv')
    except:
        # Create sample delivery data
        delivery_df = pd.DataFrame({
            'ID': range(1, 1001),
            'Delivery_person_ID': [f"DRIVER{i:03d}" for i in range(1, 101)],
            'Time_Orderd': [(datetime.now() - timedelta(hours=np.random.randint(1, 24))).strftime('%H:%M:%S') for _ in range(1000)],
            'Time_Order_picked': [(datetime.now() - timedelta(minutes=np.random.randint(10, 60))).strftime('%H:%M:%S') for _ in range(1000)],
            'Weatherconditions': np.random.choice(['Sunny', 'Cloudy', 'Foggy', 'Windy', 'Stormy', 'Sandstorms'], 1000),
            'Road_traffic_density': np.random.choice(['Low', 'Medium', 'High', 'Jam'], 1000),
            'Type_of_vehicle': np.random.choice(['motorcycle', 'scooter', 'electric_scooter'], 1000),
            'Type_of_order': np.random.choice(['Snack', 'Meal', 'Drinks', 'Buffer'], 1000),
            'multiple_deliveries': np.random.choice([0, 1, 2, 3], 1000, p=[0.7, 0.2, 0.07, 0.03])
        })
    
    # Load promotions data
    try:
        promotions_df = pd.read_csv('/home/ubuntu/food_delivery_data/promotions.csv')
    except:
        # Create sample promotions data
        promotions_df = pd.DataFrame({
            'promotion_id': range(1, 16),
            'name': [f"Promo {i}" for i in range(1, 16)],
            'discount_type': np.random.choice(['percentage', 'fixed', 'bogo'], 15),
            'discount_value': np.random.choice([10, 15, 20, 25, 30, 50, 100], 15),
            'start_date': pd.date_range(start='2023-01-01', periods=15),
            'end_date': pd.date_range(start='2023-06-01', periods=15),
            'is_active': np.random.choice([0, 1], 15, p=[0.2, 0.8])
        })
    
    # Process data for dashboard
    
    # Order metrics by date
    if 'created_at' in orders_df.columns:
        orders_df['date'] = pd.to_datetime(orders_df['created_at']).dt.date
        daily_orders = orders_df.groupby('date').size().reset_index(name='order_count')
        daily_orders['date'] = pd.to_datetime(daily_orders['date'])
        daily_orders = daily_orders.sort_values('date')
    else:
        # Create sample daily orders data
        dates = pd.date_range(end=datetime.now(), periods=90)
        daily_orders = pd.DataFrame({
            'date': dates,
            'order_count': np.random.randint(50, 200, size=90)
        })
    
    # Add some trend to the data
    daily_orders['order_count'] = daily_orders['order_count'] + daily_orders.index * 0.5
    
    # Calculate delivery metrics
    if 'Time_Orderd' in delivery_df.columns and 'Time_Order_picked' in delivery_df.columns:
        delivery_df['Time_Orderd'] = pd.to_datetime(delivery_df['Time_Orderd'], errors='coerce')
        delivery_df['Time_Order_picked'] = pd.to_datetime(delivery_df['Time_Order_picked'], errors='coerce')
        
        # Handle potential parsing errors
        valid_times = ~(delivery_df['Time_Orderd'].isna() | delivery_df['Time_Order_picked'].isna())
        if valid_times.any():
            delivery_df.loc[valid_times, 'delivery_time_minutes'] = (
                (delivery_df.loc[valid_times, 'Time_Order_picked'] - 
                 delivery_df.loc[valid_times, 'Time_Orderd']).dt.total_seconds() / 60
            )
        else:
            delivery_df['delivery_time_minutes'] = np.random.uniform(15, 60, len(delivery_df))
    else:
        delivery_df['delivery_time_minutes'] = np.random.uniform(15, 60, len(delivery_df))
    
    # Ensure delivery times are positive and reasonable
    delivery_df['delivery_time_minutes'] = delivery_df['delivery_time_minutes'].apply(
        lambda x: abs(x) if isinstance(x, (int, float)) else np.random.uniform(15, 60)
    )
    delivery_df['delivery_time_minutes'] = delivery_df['delivery_time_minutes'].clip(5, 120)
    
    # Restaurant performance
    if 'loc_name' in restaurants_df.columns and 'rating' in restaurants_df.columns:
        top_restaurants = restaurants_df.sort_values('rating', ascending=False).head(10)
    else:
        # Create sample top restaurants
        top_restaurants = pd.DataFrame({
            'loc_name': [f"Top Restaurant {i}" for i in range(1, 11)],
            'rating': np.random.uniform(4.0, 5.0, 10).round(1),
            'cuisines': np.random.choice(['Italian', 'Chinese', 'Indian', 'Mexican', 'American'], 10)
        })
    
    # Order status distribution
    if 'updated_status' in orders_df.columns:
        status_counts = orders_df['updated_status'].value_counts().reset_index()
        status_counts.columns = ['status', 'count']
    else:
        # Create sample status distribution
        status_counts = pd.DataFrame({
            'status': ['Delivered', 'Cancelled', 'In Progress', 'Preparing'],
            'count': [700, 100, 150, 50]
        })
    
    # Weather impact on delivery
    if 'Weatherconditions' in delivery_df.columns and 'delivery_time_minutes' in delivery_df.columns:
        weather_impact = delivery_df.groupby('Weatherconditions')['delivery_time_minutes'].mean().reset_index()
    else:
        # Create sample weather impact data
        weather_impact = pd.DataFrame({
            'Weatherconditions': ['Sunny', 'Cloudy', 'Foggy', 'Windy', 'Stormy', 'Sandstorms'],
            'delivery_time_minutes': [25, 30, 40, 35, 50, 55]
        })
    
    # Traffic impact on delivery
    if 'Road_traffic_density' in delivery_df.columns and 'delivery_time_minutes' in delivery_df.columns:
        traffic_impact = delivery_df.groupby('Road_traffic_density')['delivery_time_minutes'].mean().reset_index()
        # Ensure proper ordering
        traffic_order = {'Low': 0, 'Medium': 1, 'High': 2, 'Jam': 3}
        if all(density in traffic_order for density in traffic_impact['Road_traffic_density']):
            traffic_impact['order'] = traffic_impact['Road_traffic_density'].map(traffic_order)
            traffic_impact = traffic_impact.sort_values('order').drop('order', axis=1)
    else:
        # Create sample traffic impact data
        traffic_impact = pd.DataFrame({
            'Road_traffic_density': ['Low', 'Medium', 'High', 'Jam'],
            'delivery_time_minutes': [20, 30, 45, 60]
        })
    
    # Calculate key metrics
    total_orders = len(orders_df)
    avg_delivery_time = delivery_df['delivery_time_minutes'].mean()
    if 'rating' in orders_df.columns:
        avg_rating = orders_df['rating'].dropna().mean()
    else:
        avg_rating = 4.2
    
    if 'tips' in orders_df.columns:
        avg_tip = orders_df['tips'].mean()
    else:
        avg_tip = 3.5
    
    # Calculate month-over-month growth
    if 'date' in daily_orders.columns:
        daily_orders['month'] = daily_orders['date'].dt.to_period('M')
        monthly_orders = daily_orders.groupby('month')['order_count'].sum().reset_index()
        if len(monthly_orders) >= 2:
            current_month = monthly_orders.iloc[-1]['order_count']
            previous_month = monthly_orders.iloc[-2]['order_count']
            mom_growth = ((current_month - previous_month) / previous_month) * 100
        else:
            mom_growth = 5.7  # Default value
    else:
        mom_growth = 5.7  # Default value
    
    return {
        'restaurants': restaurants_df,
        'orders': orders_df,
        'delivery': delivery_df,
        'promotions': promotions_df,
        'daily_orders': daily_orders,
        'top_restaurants': top_restaurants,
        'status_counts': status_counts,
        'weather_impact': weather_impact,
        'traffic_impact': traffic_impact,
        'metrics': {
            'total_orders': total_orders,
            'avg_delivery_time': avg_delivery_time,
            'avg_rating': avg_rating,
            'avg_tip': avg_tip,
            'mom_growth': mom_growth
        }
    }

# Load data
data = load_data()

# Sidebar filters
st.sidebar.markdown("<div class='sub-header'>Dashboard Filters</div>", unsafe_allow_html=True)

# Date range filter
if 'date' in data['daily_orders'].columns:
    min_date = data['daily_orders']['date'].min().date()
    max_date = data['daily_orders']['date'].max().date()
else:
    min_date = datetime.now().date() - timedelta(days=90)
    max_date = datetime.now().date()

date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(max_date - timedelta(days=30), max_date),
    min_value=min_date,
    max_value=max_date
)

# City filter
if 'searched_city' in data['restaurants'].columns:
    cities = ['All'] + sorted(data['restaurants']['searched_city'].unique().tolist())
else:
    cities = ['All', 'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']

selected_city = st.sidebar.selectbox("Select City", cities)

# Cuisine filter
if 'cuisines' in data['restaurants'].columns:
    cuisines = ['All'] + sorted(data['restaurants']['cuisines'].unique().tolist())
else:
    cuisines = ['All', 'Italian', 'Chinese', 'Indian', 'Mexican', 'American']

selected_cuisine = st.sidebar.selectbox("Select Cuisine", cuisines)

# Weather conditions filter
if 'Weatherconditions' in data['delivery'].columns:
    weather_conditions = ['All'] + sorted(data['delivery']['Weatherconditions'].unique().tolist())
else:
    weather_conditions = ['All', 'Sunny', 'Cloudy', 'Foggy', 'Windy', 'Stormy', 'Sandstorms']

selected_weather = st.sidebar.selectbox("Select Weather Condition", weather_conditions)

# Apply filters to data
# In a real implementation, these filters would be applied in the Snowflake query
# For this demo, we'll filter the data after loading

# Filter daily orders by date
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
    filtered_daily_orders = data['daily_orders'][
        (data['daily_orders']['date'].dt.date >= start_date) & 
        (data['daily_orders']['date'].dt.date <= end_date)
    ]
else:
    filtered_daily_orders = data['daily_orders']

# Main dashboard content
st.markdown("<div class='main-header'>Food Delivery Analytics Dashboard</div>", unsafe_allow_html=True)

# Key metrics row
st.markdown("<div class='sub-header'>Key Performance Indicators</div>", unsafe_allow_html=True)
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.markdown(f"<div class='metric-value'>{data['metrics']['total_orders']:,}</div>", unsafe_allow_html=True)
    st.markdown("<div class='metric-label'>Total Orders</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

with col2:
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.markdown(f"<div class='metric-value'>{data['metrics']['avg_delivery_time']:.1f} min</div>", unsafe_allow_html=True)
    st.markdown("<div class='metric-label'>Avg. Delivery Time</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

with col3:
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.markdown(f"<div class='metric-value'>{data['metrics']['avg_rating']:.1f}⭐</div>", unsafe_allow_html=True)
    st.markdown("<div class='metric-label'>Avg. Rating</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

with col4:
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.markdown(f"<div class='metric-value'>${data['metrics']['avg_tip']:.2f}</div>", unsafe_allow_html=True)
    st.markdown("<div class='metric-label'>Avg. Tip Amount</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

with col5:
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    growth_class = "trend-up" if data['metrics']['mom_growth'] >= 0 else "trend-down"
    growth_symbol = "↑" if data['metrics']['mom_growth'] >= 0 else "↓"
    st.markdown(
        f"<div class='metric-value {growth_class}'>{abs(data['metrics']['mom_growth']):.1f}% {growth_symbol}</div>", 
        unsafe_allow_html=True
    )
    st.markdown("<div class='metric-label'>Month-over-Month Growth</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

# Order trends chart
st.markdown("<div class='sub-header'>Order Trends</div>", unsafe_allow_html=True)

# Create a line chart for order trends
fig_orders = px.line(
    filtered_daily_orders, 
    x='date', 
    y='order_count',
    title='Daily Order Volume',
    labels={'date': 'Date', 'order_count': 'Number of Orders'},
    template='plotly_white'
)

fig_orders.update_layout(
    height=400,
    xaxis_title='Date',
    yaxis_title='Number of Orders',
    hovermode='x unified'
)

st.plotly_chart(fig_orders, use_container_width=True)

# Order flow and delivery metrics
st.markdown("<div class='sub-header'>Order Flow & Delivery Metrics</div>", unsafe_allow_html=True)
col1, col2 = st.columns(2)

with col1:
    # Order status distribution
    fig_status = px.pie(
        data['status_counts'], 
        values='count', 
        names='status',
        title='Order Status Distribution',
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    fig_status.update_layout(
        height=350,
        legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5)
    )
    
    st.plotly_chart(fig_status, use_container_width=True)

with col2:
    # Delivery time by traffic conditions
    fig_traffic = px.bar(
        data['traffic_impact'],
        x='Road_traffic_density',
        y='delivery_time_minutes',
        title='Delivery Time by Traffic Conditions',
        labels={'Road_traffic_density': 'Traffic Density', 'delivery_time_minutes': 'Avg. Delivery Time (min)'},
        color='delivery_time_minutes',
        color_continuous_scale='Viridis'
    )
    
    fig_traffic.update_layout(
        height=350,
        xaxis_title='Traffic Density',
        yaxis_title='Avg. Delivery Time (min)'
    )
    
    st.plotly_chart(fig_traffic, use_container_width=True)

# Restaurant and weather analysis
col1, col2 = st.columns(2)

with col1:
    # Top restaurants by rating
    fig_restaurants = px.bar(
        data['top_restaurants'].head(10),
        x='rating',
        y='loc_name',
        title='Top 10 Restaurants by Rating',
        labels={'rating': 'Rating', 'loc_name': 'Restaurant'},
        orientation='h',
        color='rating',
        color_continuous_scale='RdYlGn'
    )
    
    fig_restaurants.update_layout(
        height=400,
        xaxis_title='Rating',
        yaxis_title='Restaurant',
        yaxis=dict(autorange="reversed")
    )
    
    st.plotly_chart(fig_restaurants, use_container_width=True)

with col2:
    # Weather impact on delivery time
    fig_weather = px.bar(
        data['weather_impact'],
        x='Weatherconditions',
        y='delivery_time_minutes',
        title='Weather Impact on Delivery Time',
        labels={'Weatherconditions': 'Weather Condition', 'delivery_time_minutes': 'Avg. Delivery Time (min)'},
        color='delivery_time_minutes',
        color_continuous_scale='Viridis'
    )
    
    fig_weather.update_layout(
        height=400,
        xaxis_title='Weather Condition',
        yaxis_title='Avg. Delivery Time (min)'
    )
    
    st.plotly_chart(fig_weather, use_container_width=True)

# Promotions analysis
st.markdown("<div class='sub-header'>Promotion Analysis</div>", unsafe_allow_html=True)

# Create a sample promotion effectiveness dataset
# In a real implementation, this would come from the Snowflake data warehouse
promotion_effectiveness = pd.DataFrame({
    'promotion_name': data['promotions']['name'],
    'discount_type': data['promotions']['discount_type'],
    'discount_value': data['promotions']['discount_value'],
    'orders': np.random.randint(50, 500, len(data['promotions'])),
    'revenue': np.random.uniform(1000, 10000, len(data['promotions'])).round(2),
    'avg_order_value': np.random.uniform(15, 50, len(data['promotions'])).round(2),
    'is_active': data['promotions']['is_active']
})

# Filter active promotions
active_promotions = promotion_effectiveness[promotion_effectiveness['is_active'] == 1]

# Create a horizontal bar chart for promotion effectiveness
fig_promotions = px.bar(
    active_promotions.sort_values('orders', ascending=True).tail(10),
    x='orders',
    y='promotion_name',
    title='Top 10 Promotions by Order Volume',
    labels={'orders': 'Number of Orders', 'promotion_name': 'Promotion'},
    orientation='h',
    color='discount_value',
    color_continuous_scale='Viridis',
    hover_data=['discount_type', 'discount_value', 'avg_order_value', 'revenue']
)

fig_promotions.update_layout(
    height=400,
    xaxis_title='Number of Orders',
    yaxis_title='Promotion',
    yaxis=dict(autorange="reversed")
)

st.plotly_chart(fig_promotions, use_container_width=True)

# Footer
st.markdown("<div class='footer'>Food Delivery Data Engineering Project - Powered by Snowflake & Streamlit</div>", unsafe_allow_html=True)

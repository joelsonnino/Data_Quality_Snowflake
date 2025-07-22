import streamlit as st
import pandas as pd
from pathlib import Path
import json
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np

# --- Page Config ---
st.set_page_config(
    page_title="Data Quality Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Custom CSS ---
st.markdown("""
<style>
    /* Main dashboard styling */
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        color: white;
        text-align: center;
    }
    
    .main-header h1 {
        margin: 0;
        font-size: 2.5rem;
        font-weight: 700;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
    }
    
    .main-header p {
        margin: 0.5rem 0 0 0;
        font-size: 1.1rem;
        opacity: 0.9;
    }
    
    /* Status badges (retained for consistency, though st.dataframe styling takes precedence) */
    .status-badge {
        display: inline-block;
        padding: 0.3rem 0.8rem;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.85rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        white-space: nowrap; /* Prevent badge text from wrapping */
    }
    
    .status-pass { background-color: #d4edda; color: #155724; }
    .status-fail { background-color: #f8d7da; color: #721c24; }
    .status-error { background-color: #fff3cd; color: #856404; }
    .status-timeout { background-color: #cfe2f3; color: #0c5460; } /* Adjusted timeout color for better visibility */
    
    /* Metric cards - improved shadow and border */
    .st-emotion-cache-1wv7cff.e1nzilvr4 { /* Target Streamlit metric cards specifically */
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.15); /* Softer, larger shadow */
        border-left: 5px solid #667eea; /* Thicker border */
        margin-bottom: 1rem;
        transition: transform 0.2s ease-in-out; /* Add hover effect */
    }
    .st-emotion-cache-1wv7cff.e1nzilvr4:hover {
        transform: translateY(-5px);
    }

    /* Alert styling */
    .alert {
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    }
    
    .alert-success {
        background-color: #d4edda;
        border-left: 4px solid #28a745;
        color: #155724;
    }
    
    .alert-warning {
        background-color: #fff3cd;
        border-left: 4px solid #ffc107;
        color: #856404;
    }
    
    .alert-danger {
        background-color: #f8d7da;
        border-left: 4px solid #dc3545;
        color: #721c24;
    }
    
    /* Hide streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display:none;}

    /* Custom styling for dataframe cells based on status */
    .dataframe-status-pass { background-color: #d4edda; color: #155724; }
    .dataframe-status-fail { background-color: #f8d7da; color: #721c24; }
    .dataframe-status-error { background-color: #fff3cd; color: #856404; }
    .dataframe-status-timeout { background-color: #cfe2f3; color: #0c5460; }

</style>
""", unsafe_allow_html=True)

# --- Constants & Paths ---
PROJECT_ROOT = Path(__file__).resolve().parent
REPORTS_DIR = PROJECT_ROOT / "dq_reports"
RESULTS_FILE = REPORTS_DIR / "dq_results.json"

# --- Helper Functions ---
@st.cache_data(ttl=60)
def load_results():
    """Loads the latest DQ results from the JSON file."""
    if not RESULTS_FILE.exists():
        st.error(f"Error: File '{RESULTS_FILE}' not found. Please ensure the DQ results JSON file exists in the 'dq_reports' directory.")
        return pd.DataFrame()
    try:
        with open(RESULTS_FILE, 'r') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except (json.JSONDecodeError, pd.errors.EmptyDataError) as e:
        st.error(f"Error loading DQ results: {e}. The JSON file might be empty or malformed.")
        return pd.DataFrame()

def get_status_color(status):
    """Returns color for status-based styling."""
    colors = {
        'pass': '#28a745',
        'fail': '#dc3545',
        'error': '#ffc107',
        'timeout': '#0c5460' # Darker blue for timeout
    }
    return colors.get(status, '#6c757d') # Default grey

def format_status_badge(status):
    """Creates HTML badge for status. Not directly used in st.dataframe styling but kept for reference/consistency."""
    return f'<span class="status-badge status-{status}">{status}</span>'

def create_health_score(results_df):
    """Calculate overall health score based on test results."""
    if results_df.empty:
        return 100
    
    total_tests = len(results_df)
    passed_tests = len(results_df[results_df['status'] == 'pass'])
    failed_tests = len(results_df[results_df['status'] == 'fail'])
    error_tests = len(results_df[results_df['status'] == 'error'])
    timeout_tests = len(results_df[results_df['status'] == 'timeout'])
    
    # Weight different failure types
    health_score = (passed_tests * 1.0 + 
                   error_tests * 0.5 +  # Errors are bad but might not be data issues directly
                   timeout_tests * 0.3 + # Timeouts are problematic but not necessarily data issues
                   failed_tests * 0.0) / total_tests * 100
    
    return round(health_score, 1)

def create_health_gauge_chart(health_score):
    """Create a gauge chart for the overall health score."""
    fig = go.Figure(go.Indicator(
        mode = "gauge+number+delta",
        value = health_score,
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': "Overall Health Score", 'font': {'size': 18}},
        gauge = {
            'axis': {'range': [None, 100], 'tickwidth': 1, 'tickcolor': "darkblue"},
            'bar': {'color': "#667eea"},
            'bgcolor': "white",
            'borderwidth': 2,
            'bordercolor': "gray",
            'steps': [
                {'range': [0, 70], 'color': "red"},
                {'range': [70, 85], 'color': "orange"},
                {'range': [85, 95], 'color': "yellowgreen"},
                {'range': [95, 100], 'color': "green"}],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': health_score}}))

    fig.update_layout(margin=dict(l=20, r=30, t=50, b=20), height=250)
    return fig

def create_status_distribution_chart(results_df):
    """Create a donut chart showing status distribution."""
    if results_df.empty:
        return go.Figure()
    
    status_counts = results_df['status'].value_counts()
    colors = [get_status_color(status) for status in status_counts.index]
    
    fig = go.Figure(data=[go.Pie(
        labels=status_counts.index,
        values=status_counts.values,
        hole=0.5,
        marker_colors=colors,
        textinfo='label+percent',
        textposition='outside',
        hovertemplate='<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>'
    )])
    
    fig.update_layout(
        title={
            'text': 'Test Status Distribution',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 18, 'color': '#2c3e50'} # Larger title font
        },
        showlegend=True,
        height=350, # Slightly reduced height to fit better with gauge
        margin=dict(t=50, b=50, l=50, r=50),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

def create_model_performance_chart(results_df, limit_n=None, sort_order='ascending'):
    """
    Create a bar chart showing performance by model, with optional limiting and sorting.
    :param results_df: DataFrame with results.
    :param limit_n: Limits the number of models to show (e.g., Top 10, Worst 10).
    :param sort_order: 'ascending' for worst, 'descending' for best.
    """
    if results_df.empty:
        return go.Figure()
    
    model_stats = results_df.groupby('model_name')['status'].apply(
        lambda x: (x == 'pass').sum() / len(x) * 100
    ).reset_index()
    model_stats.columns = ['Model', 'Pass Rate']
    
    # Calculate failure count for coloring
    model_stats['Fail Count'] = results_df.groupby('model_name')['status'].apply(
        lambda x: (x == 'fail').sum() + (x == 'error').sum() + (x == 'timeout').sum()
    ).values

    # Apply sorting and limiting
    model_stats = model_stats.sort_values('Pass Rate', ascending=(sort_order == 'ascending'))
    if limit_n:
        model_stats = model_stats.head(limit_n) # head() after sorting ascending gives worst N, after sorting descending gives best N
    
    fig = px.bar(
        model_stats,
        x='Pass Rate',
        y='Model',
        orientation='h',
        title='Data Quality Pass Rate by Table/Model',
        color='Pass Rate', # Color by Pass Rate
        color_continuous_scale='RdYlGn', # Red to Green
        range_color=[0, 100],
        hover_data={'Fail Count': True, 'Pass Rate': ':.2f'} # Show fail count on hover
    )
    
    # Dynamically adjust height based on number of bars
    bar_height = 35 # Height per bar
    min_height = 300 # Minimum chart height
    fig_height = max(min_height, len(model_stats) * bar_height)

    fig.update_layout(
        height=fig_height,
        margin=dict(t=50, b=50, l=50, r=50),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        title_font_color='#2c3e50',
        title_font_size=18,
        xaxis_title_text='Pass Rate (%)',
        yaxis_title_text='' # Remove Y-axis title for cleaner look
    )
    
    return fig

def create_test_type_chart(results_df):
    """Create a chart showing test type distribution."""
    if results_df.empty:
        return go.Figure()
    
    test_stats = results_df.groupby(['test_type', 'status']).size().reset_index(name='count')
    
    fig = px.bar(
        test_stats,
        x='test_type',
        y='count',
        color='status',
        title='Test Results by Test Type',
        color_discrete_map={
            'pass': '#28a745',
            'fail': '#dc3545',
            'error': '#ffc107',
            'timeout': '#0c5460'
        },
        barmode='stack' # Stack bars to show total tests per type
    )
    
    fig.update_layout(
        height=400,
        margin=dict(t=50, b=50, l=50, r=50),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        title_font_color='#2c3e50',
        title_font_size=18,
        xaxis_title='Test Type',
        yaxis_title='Number of Tests',
        legend_title='Status'
    )
    
    return fig

# --- Main Dashboard UI ---

# Header
st.markdown("""
<div class="main-header">
    <h1>🛡️ Data Quality Control Dashboard</h1>
    <p>Monitor and track data quality metrics across all your Snowflake tables</p>
</div>
""", unsafe_allow_html=True)

# --- Load Data ---
results_df = load_results()

if results_df.empty:
    st.markdown("""
    <div class="alert alert-warning">
        <strong>⚠️ No Data Available</strong><br>
        No data quality results found. Please run <code>python run_automatic_dq_checks.py</code> first to generate test results.
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# --- Sidebar Filters ---
st.sidebar.image("https://via.placeholder.com/200x80/667eea/white?text=DQ+Dashboard", width=200) # Placeholder image
st.sidebar.markdown("## 🔍 Filters & Controls")

# Auto-refresh toggle
auto_refresh = st.sidebar.checkbox("🔄 Auto-refresh (60s)", value=False, help="Automatically refresh the dashboard every 60 seconds.")
if auto_refresh:
    st.rerun()

# Date range filter (moved up for better UX)
if not results_df.empty:
    min_overall_date = results_df['timestamp'].min().date()
    max_overall_date = results_df['timestamp'].max().date()
    
    date_range_selection = st.sidebar.date_input(
        "📅 Select Date Range",
        value=(min_overall_date, max_overall_date), # Default to full range
        min_value=min_overall_date,
        max_value=max_overall_date,
        help="Filter results by the timestamp of the test run."
    )
    if len(date_range_selection) == 2:
        start_date, end_date = date_range_selection
    else: # Handle the case where only one date is selected (e.g., initial state)
        start_date, end_date = min_overall_date, max_overall_date

st.sidebar.markdown("---") # Separator

# Status filter
status_options = sorted(results_df['status'].unique())
status_filter = st.sidebar.multiselect(
    "📊 Filter by Status",
    options=status_options,
    default=[s for s in ['fail', 'error', 'timeout'] if s in status_options],
    help="Select which test statuses to display in the detailed table and critical issues."
)

# Model filter
model_options = sorted(results_df['model_name'].unique())
model_filter = st.sidebar.multiselect(
    "📋 Filter by Table/Model",
    options=model_options,
    default=[],
    help="Select specific tables/models to focus on."
)

# Test type filter
test_type_options = sorted(results_df['test_type'].unique())
test_type_filter = st.sidebar.multiselect(
    "🧪 Filter by Test Type",
    options=test_type_options,
    default=[],
    help="Select specific test types to display."
)

# Apply filters
filtered_df = results_df.copy()

# Date filter applied first
if len(date_range_selection) == 2:
    filtered_df = filtered_df[
        (filtered_df['timestamp'].dt.date >= start_date) & 
        (filtered_df['timestamp'].dt.date <= end_date)
    ]

if status_filter:
    filtered_df = filtered_df[filtered_df['status'].isin(status_filter)]
if model_filter:
    filtered_df = filtered_df[filtered_df['model_name'].isin(model_filter)]
if test_type_filter:
    filtered_df = filtered_df[filtered_df['test_type'].isin(test_type_filter)]

# --- Key Metrics ---
last_run_time = results_df['timestamp'].max()
health_score = create_health_score(results_df)

# Time since last run
time_since_last = datetime.now() - last_run_time.to_pydatetime()
if time_since_last.total_seconds() < 3600:  # Less than 1 hour
    last_run_display = f"{int(time_since_last.total_seconds() // 60)} minutes ago"
elif time_since_last.total_seconds() < 86400:  # Less than 1 day
    last_run_display = f"{int(time_since_last.total_seconds() // 3600)} hours ago"
else:
    last_run_display = f"{int(time_since_last.total_seconds() // 86400)} days ago"

# Status indicator for overall health alert
if health_score >= 95:
    status_indicator = "🟢 Excellent"
    alert_class = "alert-success"
elif health_score >= 85:
    status_indicator = "🟡 Good"
    alert_class = "alert-warning"
elif health_score >= 70:
    status_indicator = "🟠 Needs Attention"
    alert_class = "alert-warning"
else:
    status_indicator = "🔴 Critical"
    alert_class = "alert-danger"

st.markdown(f"""
<div class="alert {alert_class}">
    <strong>Overall Data Health Status: {status_indicator}</strong><br>
    <small>Health Score: {health_score}% | Last Updated: {last_run_time.strftime('%Y-%m-%d %H:%M:%S')} ({last_run_display})</small>
</div>
""", unsafe_allow_html=True)


# --- Metrics Row & Health Gauge ---
col1, col2, col3, col4, col5, col6 = st.columns([1.5, 1, 1, 1, 1, 1]) # Adjust column ratios

with col1:
    st.subheader("Data Health Overview")
    fig_gauge = create_health_gauge_chart(health_score)
    st.plotly_chart(fig_gauge, use_container_width=True, config={'displayModeBar': False}) # Hide toolbar

with col2:
    st.metric(label="Total Tests Run", value=len(results_df), help="Total number of data quality tests executed.")
with col3:
    st.metric(label="Passed Tests", value=len(results_df[results_df['status'] == 'pass']), help="Number of tests that passed successfully.")
with col4:
    st.metric(label="Failed Tests", value=len(results_df[results_df['status'] == 'fail']), help="Number of tests that found data quality issues.", delta=f"-{len(results_df[results_df['status'] == 'fail'])}" if len(results_df[results_df['status'] == 'fail']) > 0 else None, delta_color="inverse")
with col5:
    st.metric(label="Tests with Errors", value=len(results_df[results_df['status'] == 'error']), help="Number of tests that failed due to execution errors (e.g., SQL syntax, permissions).", delta=f"-{len(results_df[results_df['status'] == 'error'])}" if len(results_df[results_df['status'] == 'error']) > 0 else None, delta_color="inverse")
with col6:
    st.metric(label="Tests Timed Out", value=len(results_df[results_df['status'] == 'timeout']), help="Number of tests that exceeded the execution time limit.", delta=f"-{len(results_df[results_df['status'] == 'timeout'])}" if len(results_df[results_df['status'] == 'timeout']) > 0 else None, delta_color="inverse")

st.markdown("---")

# --- Charts Section ---
st.markdown("## 📊 Data Quality Analytics")

chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    fig_distribution = create_status_distribution_chart(results_df)
    st.plotly_chart(fig_distribution, use_container_width=True)

with chart_col2:
    fig_test_types = create_test_type_chart(results_df)
    st.plotly_chart(fig_test_types, use_container_width=True)


st.markdown("---")
# --- Model Performance Chart (New Controls) ---
st.subheader("Table/Model Performance Overview")

model_chart_display_option = st.radio(
    "Show:",
    ("Worst N Models by Pass Rate", "Best N Models by Pass Rate", "All Models"),
    horizontal=True,
    help="Select how to visualize model performance."
)

if model_chart_display_option != "All Models":
    num_models_to_show = st.slider(
        "Number of models to display (N):",
        min_value=5,
        max_value=min(25, results_df['model_name'].nunique()), # Max 25 or total unique models
        value=10,
        step=1,
        help="Select the number of models to show in the chart."
    )
    
    if model_chart_display_option == "Worst N Models by Pass Rate":
        fig_model_perf = create_model_performance_chart(results_df, limit_n=num_models_to_show, sort_order='ascending')
        st.plotly_chart(fig_model_perf, use_container_width=True)
    elif model_chart_display_option == "Best N Models by Pass Rate":
        fig_model_perf = create_model_performance_chart(results_df, limit_n=num_models_to_show, sort_order='descending')
        st.plotly_chart(fig_model_perf, use_container_width=True)
else:
    # Use an expander for "All Models" to avoid clutter by default
    with st.expander("Show Pass Rate for All Models"):
        fig_model_perf = create_model_performance_chart(results_df, limit_n=None) # No limit
        st.plotly_chart(fig_model_perf, use_container_width=True)

st.markdown("---")

# --- Critical Issues Section ---
st.markdown("## ⚠️ Critical Issues & Recent Failures")

critical_issues_df = results_df[(results_df['status'].isin(['fail', 'error', 'timeout']))].sort_values(by='timestamp', ascending=False)

if critical_issues_df.empty:
    st.info("🎉 Good news! No critical issues (failures, errors, or timeouts) found based on current filters.")
else:
    st.warning(f"Found {len(critical_issues_df)} critical issues.")
    
    # Apply styling to the critical issues table (using st.dataframe built-in styling)
    def highlight_status(s):
        if s == 'fail':
            return 'background-color: #f8d7da; color: #721c24'
        elif s == 'error':
            return 'background-color: #fff3cd; color: #856404'
        elif s == 'timeout':
            return 'background-color: #cfe2f3; color: #0c5460'
        return ''

    st.dataframe(
        critical_issues_df[['timestamp', 'model_name', 'column_name', 'test_type', 'status', 'failing_rows', 'description']]
        .rename(columns={'timestamp': 'Last Run', 'model_name': 'Table', 'column_name': 'Column', 'test_type': 'Test Type', 'status': 'Status', 'failing_rows': 'Failing Rows', 'description': 'Description'})
        .style.applymap(highlight_status, subset=['Status']),
        use_container_width=True,
        height=300
    )


st.markdown("---")

# --- Detailed Results Table ---
st.markdown("## 🔍 Detailed Test Results (All Filters Applied)")

if filtered_df.empty:
    st.info("🔍 No tests match the current filter criteria. Try adjusting your filters.")
else:
    # Summary of filtered results
    passed_filtered = len(filtered_df[filtered_df['status'] == 'pass'])
    total_filtered = len(filtered_df)
    filtered_pass_rate = (passed_filtered / total_filtered * 100) if total_filtered > 0 else 0
    
    st.markdown(f"""
    **Showing {total_filtered} of {len(results_df)} tests** | 
    Pass Rate for filtered results: {filtered_pass_rate:.1f}%
    """)
    
    # Prepare display dataframe for full table
    display_df = filtered_df.copy()
    display_df['Timestamp'] = display_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M')
    
    # Select and rename columns for display in the main table
    display_df = display_df[[
        'model_name', 'column_name', 'test_type', 'status', 'failing_rows', 'description', 'Timestamp'
    ]].rename(columns={
        'model_name': 'Table',
        'column_name': 'Column',
        'test_type': 'Test Type',
        'status': 'Status', # Keep original status column for styling
        'failing_rows': 'Failing Rows',
        'description': 'Description'
    })
    
    # Apply styling using .applymap for status column
    st.dataframe(
        display_df.style.applymap(highlight_status, subset=['Status']),
        use_container_width=True,
        height=500 # Set a fixed height for consistency
    )
    
    # Download button for filtered results
    csv = filtered_df.to_csv(index=False)
    st.download_button(
        label="📥 Download Filtered Results (CSV)",
        data=csv,
        file_name=f"dq_results_filtered_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

# --- Additional Information ---
with st.expander("🔧 System Information & Raw Data"):
    col_info, col_types = st.columns(2)
    
    with col_info:
        st.markdown("### 📋 System Info")
        st.info(f"""
        **Total Tables Monitored:** {results_df['model_name'].nunique()}  
        **Total Test Types:** {results_df['test_type'].nunique()}  
        **Data Freshness:** {last_run_time.strftime('%Y-%m-%d %H:%M:%S')} ({last_run_display})  
        **Report Location:** `{RESULTS_FILE}`
        """)
    
    with col_types:
        st.markdown("### 🏷️ Test Types Breakdown")
        test_type_counts = results_df['test_type'].value_counts().reset_index()
        test_type_counts.columns = ['Test Type', 'Number of Tests']
        st.dataframe(test_type_counts, use_container_width=True, hide_index=True)
    
    st.markdown("### 📊 Raw Data Export (All Results)")
    st.dataframe(results_df, use_container_width=True)
    
    # Full data download
    full_csv = results_df.to_csv(index=False)
    st.download_button(
        label="📥 Download All Results (CSV)",
        data=full_csv,
        file_name=f"dq_results_complete_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

# --- Footer ---
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #7f8c8d; font-size: 0.9rem;">
    <p>🛡️ Data Quality Dashboard | Built with Streamlit | Last Updated: {}</p>
</div>
""".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), unsafe_allow_html=True)
from CFPInitialiseMySQLConnection import cursor
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import plotly.express as px
import plotly.graph_objs as go
from dash.exceptions import PreventUpdate

# Fetch data from MySQL and create the referrals DataFrame
cursor.execute(
    'SELECT LocalPatientId, ServiceRequestId, ServiceRequestIDIN, ReferralRequestReceivedDate, ServDischDate FROM referral')
ref = pd.DataFrame(cursor.fetchall(), columns=['LocalPatientId', 'ServiceRequestId', 'ServiceRequestIDIN',
                                               'Date Referred To CFP', 'ServDischDate'])
cursor.execute('SELECT LocalPatientId, ClientTabGMCCodeReg, GMPCodeReg FROM centre_for_psychology.gp;')
gp = pd.DataFrame(cursor.fetchall(), columns=['LocalPatientId', 'CCG', 'GP Surgery'])
cursor.execute(
    'SELECT LocalPatientId, Gender, PersonBirthDate, ExBAFIndicator, PerinatalInd, Postcode FROM centre_for_psychology.mpi;')
mpi = pd.DataFrame(cursor.fetchall(),
                   columns=['LocalPatientId', 'Gender', 'PersonBirthDate', 'ExBAFIndicator', 'PerinatalInd',
                            'Postcode'])
cursor.execute('SELECT GP_PRACTICE_CODE, SURGERY_NAME FROM centre_for_psychology.gp_practice;')
gpsurgery = pd.DataFrame(cursor.fetchall(), columns=['GP Surgery', 'GP Practice'])
cursor.execute(
    'SELECT ServiceRequestIDIN, apptype, attendordnacode, carecontdate, StepNumber FROM centre_for_psychology.appointment;')

ref = pd.merge(gp, ref, on='LocalPatientId', how='inner')
referrals = pd.merge(mpi, ref, on='LocalPatientId', how='inner')
referrals = pd.merge(gpsurgery, referrals, on='GP Surgery', how='right')
referrals['Date Referred To CFP'] = pd.to_datetime(referrals['Date Referred To CFP'])
current_date = datetime.now().date()
referrals['PersonBirthDate'] = pd.to_datetime(referrals['PersonBirthDate'])
referrals['Age'] = referrals['PersonBirthDate'].apply(lambda dob: (current_date - dob.date()).days / 365)
referrals = referrals[
    ['LocalPatientId', 'ServiceRequestIDIN', 'GP Practice', 'Gender', 'Age', 'ExBAFIndicator', 'PerinatalInd',
     'Postcode', 'CCG', 'Date Referred To CFP']]
referrals['Gender'] = referrals['Gender'].map({'1': 'Male', '2': 'Female'}).fillna('NULL')
referrals['Postcode'] = referrals['Postcode'].str.slice(stop=-3)
ex_baf_mapping = {'02': 'Ex-Services Member', '03': 'Not Ex-Services', '05': 'Dependant Ex-Services Member'}
referrals['ExBAFIndicator'] = referrals['ExBAFIndicator'].map(ex_baf_mapping).fillna('Not Ex-Services')
perinatal_mapping = {1: 'Prenatal (Pregnant)', 2: 'Post-Partum'}
referrals['PerinatalInd'] = referrals['PerinatalInd'].map(perinatal_mapping).fillna('Neither Prenatal Or Post-Partum')
referrals['Age'] = referrals['Age'].astype(float)
unique_years = referrals['Date Referred To CFP'].dt.year.unique()
referrals[['CCG', 'GP Practice']] = referrals[['CCG', 'GP Practice']].astype(str)
referrals['Re-Referral'] = 'New Referral'
duplicates = referrals['LocalPatientId'].duplicated(keep='first')
referrals.loc[duplicates, 'Re-Referral'] = 'Re-Referral'

conditions = [
    referrals['Age'] < 22,
    (referrals['Age'] >= 22) & (referrals['Age'] < 30),
    (referrals['Age'] >= 30) & (referrals['Age'] < 40),
    (referrals['Age'] >= 40) & (referrals['Age'] < 50),
    (referrals['Age'] >= 50) & (referrals['Age'] < 60),
    (referrals['Age'] >= 60) & (referrals['Age'] < 70),
    referrals['Age'] >= 70
]
choices = [
    '17-21',
    '22-29',
    '30-39',
    '40-49',
    '50-59',
    '60-69',
    '70+'
]
referrals['Age Group'] = np.select(conditions, choices, default=None)
referrals = referrals[['ServiceRequestIDIN', 'GP Practice', 'Gender', 'ExBAFIndicator', 'PerinatalInd',
                       'Postcode', 'CCG', 'Date Referred To CFP', 'Age Group', 'Re-Referral']]

# Initialize the Dash app
app = dash.Dash(__name__)

app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'  # Add Dash CSS for basic styling
})

# Initialize an empty list to store trace names
trace_names = []
# Initialize last_time_interval to None
time_interval = None

# Initialize a global variable to store filtered traces
global filtered_traces
filtered_traces = []

# Define the layout of the app
app.layout = html.Div([
    html.H1("Referrals Dashboard", style={'font-family': 'Arial'}),  # Set font-family to Arial
    
    html.Div([
        html.Div([
            html.H3("Gender:", style={'font-family': 'Arial'}),
            dcc.Dropdown(
                id="gender-filter",
                options=[{'label': g, 'value': g} for g in referrals['Gender'].unique()],
                multi=True,
                style={'width': '100%', 'font-family': 'Arial'}  # Set the width and font-family
            )
        ], style={'display': 'inline-block', 'width': '10%', 'margin-right': '10px'}),
        
        html.Div([
            html.H3("Ex-BAF:", style={'font-family': 'Arial'}),
            dcc.Dropdown(
                id="exbaf-filter",
                options=[{'label': e, 'value': e} for e in referrals['ExBAFIndicator'].unique()],
                multi=True,
                style={'width': '100%', 'font-family': 'Arial'}  # Set the width, font-family, and margin
            )
        ], style={'display': 'inline-block', 'width': '20%', 'margin-right': '10px'}),
        
        html.Div([
            html.H3("Perinatal:", style={'font-family': 'Arial'}),
            dcc.Dropdown(
                id="perinatal-filter",
                options=[{'label': p, 'value': p} for p in referrals['PerinatalInd'].unique()],
                multi=True,
                style={'width': '100%', 'font-family': 'Arial'}  # Set the width, font-family, and margin
            )
        ], style={'display': 'inline-block', 'width': '20%', 'margin-right': '10px'}),
        
        html.Div([
            html.H3("Re-Referral:", style={'font-family': 'Arial'}),
            dcc.Dropdown(
                id="re-referral-filter",
                options=[{'label': p, 'value': p} for p in referrals['Re-Referral'].unique()],
                multi=True,
                style={'width': '100%', 'font-family': 'Arial'}  # Set the width and font-family
            )
        ], style={'display': 'inline-block', 'margin-right': '10px'}),
        
        html.Div([
            html.H3("Age:", style={'font-family': 'Arial'}),
            dcc.Dropdown(
                id="age-group-filter",
                options=[{'label': a, 'value': a} for a in referrals['Age Group'].unique()],
                multi=True,
                style={'width': '100%', 'font-family': 'Arial'}  # Set the width, font-family, and margin
            )
        ], style={'display': 'inline-block', 'width': '8%', 'margin-right': '10px'}),
        
        html.Div([
            html.H3("CCG:", style={'font-family': 'Arial'}),
            dcc.Dropdown(
                id="ccg-filter",
                options=[{'label': e, 'value': e} for e in referrals['CCG'].unique()],
                multi=True,
                style={'width': '100%', 'font-family': 'Arial'}  # Set the width, font-family, and margin
            )
        ], style={'display': 'inline-block', 'width': '7%', 'margin-right': '10px'}),
        
        html.Div([
            html.H3("GP Surgery:", style={'font-family': 'Arial'}),
            dcc.Dropdown(
                id="gp-filter",
                options=[],
                multi=True,
                style={'width': '100%', 'font-family': 'Arial'}  # Set the width, font-family, and margin
            )
        ], style={'display': 'inline-block', 'width': '22%', 'margin-right': '10px'}),
        
    ], className="filters-container"),

    # Add space around the button
    html.Div([
        html.Button("Add Filtered Class", id="add-filtered-class-button", style={
            'background-color': 'green',  # Green background
            'color': 'white',  # White font color
            'font-size': '22px',  # Larger font size
            'margin-top': '20px',
            'padding': '10px 10px'  # Increase margin to add space
        }),
    ], className="add-filter-button-container"),

    html.Hr(),

    # Referrals Over Time Graph
    html.Div([
        html.Div([
            html.H4("Time Interval:", style={'font-family': 'Arial', 'font-size': '18px'}),
            dcc.Dropdown(
                id="time-interval",
                style={'font-family': 'Arial', 'width': '120px'},
                options=[
                    {'label': 'Annual', 'value': 'A'},
                    {'label': 'Quarterly', 'value': 'Q'},
                    {'label': 'Monthly', 'value': 'M'},
                    {'label': 'Weekly', 'value': 'W'}
                ],
                value='M'
            ),
        ], className="graph-header"),
        dcc.Graph(id="referrals-over-time-graph", className="graph", figure={}),
    ], className="graph-container")
])

# Callback to update the GP Practice options based on the selected CCG
@app.callback(
    Output("gp-filter", "options"),
    Output("gp-filter", "value"),
    Input("ccg-filter", "value"),
    Input("gp-filter", "value")
)

def update_gp_options(selected_ccg, selected_gp):
    if selected_ccg:
        gp_options = [{'label': gp, 'value': gp} for gp in
                      referrals.loc[referrals['CCG'].isin(selected_ccg), 'GP Practice'].unique()]
    else:
        gp_options = [{'label': gp, 'value': gp} for gp in referrals['GP Practice'].unique()]

    if selected_gp and selected_ccg and selected_gp not in referrals.loc[
        referrals['CCG'].isin(selected_ccg), 'GP Practice'].unique():
        selected_gp = None

    return gp_options, selected_gp


import plotly.graph_objects as go

# Create a callback function to update the referrals over time graph
@app.callback(
    Output("referrals-over-time-graph", "figure"),
    Input("time-interval", "value")
)
def update_referrals_over_time_graph(selected_interval):
    if selected_interval == 'A':
        time_grouped = referrals['Date Referred To CFP'].dt.year
    elif selected_interval == 'Q':
        time_grouped = referrals['Date Referred To CFP'].dt.to_period('Q').dt.strftime('%Y-Q%q')
    elif selected_interval == 'M':
        time_grouped = referrals['Date Referred To CFP'].dt.to_period('M').dt.strftime('%Y-%m')
    elif selected_interval == 'W':
        time_grouped = referrals['Date Referred To CFP'].dt.to_period('W').dt.strftime('%Y-W%U')

    time_counts = time_grouped.value_counts().reset_index()
    time_counts.columns = ['Time Interval', 'Count']

    # Sort the DataFrame by "Time Interval" in ascending order
    time_counts.sort_values('Time Interval', inplace=True)

    figure = go.Figure()
    figure.add_trace(go.Scatter(
        x=time_counts['Time Interval'],
        y=time_counts['Count'],
        mode='lines+markers',  # Draw lines with markers
        marker=dict(color='blue'),
        line=dict(shape='linear')  # Use linear lines
    ))

    figure.update_layout(
        title="Referrals Over Time",
        xaxis=dict(title="Time Interval"),
        yaxis=dict(title="Count"),
        showlegend=False
    )

    return figure


if __name__ == "__main__":
    app.run_server(debug=True)
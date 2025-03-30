import pandas as pd
from datetime import datetime, timedelta
import os
import numpy as np
import dask.dataframe as dd
# from openpyxl import Workbook
# from openpyxl.utils.dataframe import dataframe_to_rows
# from openpyxl.styles import Border, Side, Font, PatternFill, Alignment
# from openpyxl.formatting.rule import ColorScaleRule
import xlsxwriter
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import sys
import keyring
from smtplib import SMTPAuthenticationError, SMTPConnectError, SMTPRecipientsRefused, SMTPServerDisconnected, SMTPException
import socket
import matplotlib.colors as mcolors
from bs4 import BeautifulSoup


today = datetime.today().date()
# today = '2025-02-05'

base_folder = r'C:\Users\Arc\Downloads\OneDrive\OneDrive - zp3bp\1. Tech Generated Files\10. Collection Reports'
collection_report_filename = f"Collection Resolution Report {datetime.today().strftime('%d-%b-%Y')}.xlsx"
call_and_visit_filename = f"Call and Visit {datetime.today().strftime('%d-%b-%Y')}.xlsx"

try:
    res_df = pd.read_excel((os.path.join(base_folder, collection_report_filename)), sheet_name="RawData", engine='calamine')
    cv_df = pd.read_excel((os.path.join(base_folder, call_and_visit_filename)), sheet_name="CallAndVisitReport", engine='calamine')
    risk_df = pd.read_excel(r"C:\Users\Arc\Downloads\OneDrive\OneDrive - zp3bp\8. Script Downloaded Files\Mar25_PREDICTED_FULL_BASE_Scrub.xlsx", engine='calamine')
    hr_df = pd.read_excel(r"C:\Users\Arc\Downloads\OneDrive\OneDrive - zp3bp\8. Script Downloaded Files\Collections HC Data.xlsx", engine='calamine')
    sample_df = pd.read_excel(r"C:\Users\Arc\Downloads\OneDrive\OneDrive - zp3bp\4. Working Files\8. All Lender\1. Call & Visit MIS\template\allocartion_logic_template.xlsx", engine='calamine')
    delinqunecy_df = pd.read_excel(r"C:\Users\Arc\Downloads\OneDrive\OneDrive - zp3bp\1. Tech Generated Files\3. Delinquency Reports\Current Delinquent Base Report - All lenders 31-Jan-2025.xlsx"
                                , usecols = ['ApplicationNo', 'Name', 'PendingEmiCount'], engine='calamine')
    sample_df.rename(columns={"ApplicationNo": "Application ID"}, inplace=True)
    sample_df.drop_duplicates(subset='Application ID', keep='first', inplace=True)
    print("Files loaded successfully.")
except Exception as e:
    print(f"An error occurred while loading the files: {e}")
    
export_file_name = f"Monthly Collection Coverage {today.strftime('%b-%Y')}.xlsx"    
export_file_path = fr'C:\Users\Arc\Downloads\OneDrive\OneDrive - zp3bp\4. Working Files\8. All Lender\1. Call & Visit MIS\{export_file_name}'

zero_emi = delinqunecy_df[delinqunecy_df['PendingEmiCount'] == 0].copy()
zero_emi = zero_emi.drop_duplicates(subset='ApplicationNo')
zero_emi['status'] = 'Paid'

working_df = res_df.copy()
cv_df_sorted = cv_df.sort_values(by=['Date Of Call Or Visit', 'Time of Call/Visit'], ascending=[False, False])
unique_cv_df = cv_df_sorted.drop_duplicates(subset=['Application Id'], keep='first')

working_df["name"] = pd.merge(working_df, unique_cv_df[['Application Id', "Name"]], left_on="ApplicationNo", right_on="Application Id", how="left")["Name"]
working_df = working_df.rename(columns={'name': 'customer_name', 'DpdBucket':"opening_bucket"})
working_df['customer_name'] = working_df['customer_name'].str.title()

working_df["officer_name"] = pd.merge(working_df, sample_df[['Application ID', 'Officer Name', ]], left_on="ApplicationNo", right_on="Application ID", how="left")["Officer Name"]
working_df["officer_mobile"] = pd.merge(working_df, sample_df[['Application ID', 'Officer mobile', ]], left_on="ApplicationNo", right_on="Application ID", how="left")["Officer mobile"]
working_df['officer_emp_id'] = pd.merge(working_df, sample_df[['Application ID', 'Officer EMP ID', ]], left_on="ApplicationNo", right_on='Application ID', how="left")["Officer EMP ID"]
working_df['status'] = working_df['NoOfPaidEmiInCurrentMonth'].apply(lambda x: "Unpaid" if x == 0 else "Paid")
working_df['cluster'] = pd.merge(working_df, sample_df[['Application ID', 'Cluster']], left_on="ApplicationNo", right_on="Application ID", how="left")["Cluster"]
working_df['risk_segment'] = pd.merge(working_df, risk_df[['loanaccno', 'Prediction']], left_on="ApplicationNo", right_on="loanaccno", how="left")["Prediction"]
working_df['risk_segment_1'] = working_df['risk_segment'].apply(lambda x: 'Low' if 'L' in x else 'Medium' if 'M' in x else 'High' if 'H' in x else 'No Segment Found')
pattern = r'(L\d|H\d|M\d)'
working_df['risk_segment_2'] = working_df['risk_segment'].str.extract(pattern)

working_df['allocation_logic'] = pd.merge(working_df, sample_df[['Application ID', 'Allocation Logic']], left_on="ApplicationNo", right_on="Application ID", how="left")["Allocation Logic"]
working_df['allocation_status'] = pd.merge(working_df, sample_df[['Application ID', 'Allocation Status']], left_on="ApplicationNo", right_on="Application ID", how="left")["Allocation Status"]
working_df['designation'] = pd.merge(working_df, hr_df[['Employee Id', 'Job Role']], left_on="officer_emp_id", right_on="Employee Id", how="left")["Job Role"]

working_df['allocation_logic'] = working_df['allocation_logic'].replace({'Field ' : 'Officer allocation', 'Bucket Allocation' : 'Officer allocation'})
working_df['allocation_logic'] = working_df['allocation_logic'].fillna('-')

def update_allocation_status(row):
    if row['allocation_status'] in ["Officer allocation", "Managers Allocation"]:
        if pd.notna(row['designation']) and 'Manager' in row['designation']:
            return 'Managers Allocation'
    return row['allocation_status']

working_df.loc[working_df['allocation_logic'].isin(['Officer allocation']), 'allocation_status'] = "Officer allocation"

working_df['allocation_status'] = working_df.apply(update_allocation_status, axis=1)
working_df['field_allocation'] = working_df['allocation_status'].apply(lambda x: 'Yes' if x in ['Officer allocation', 'Managers Allocation'] else 'No')
working_df['feedback'] = pd.merge(working_df, unique_cv_df[['Application Id', 'Feedback Type']], left_on="ApplicationNo", right_on="Application Id", how="left")["Feedback Type"]



# Filter and drop duplicates in one step
call_df = cv_df_sorted[cv_df_sorted['Feedback Type'] == 'Call'][['Application Id']].drop_duplicates()
visit_df = cv_df_sorted[cv_df_sorted['Feedback Type'] == 'Visit'][['Application Id']].drop_duplicates()
working_df['call_coverage'] = working_df['ApplicationNo'].isin(call_df['Application Id']).astype(int)
working_df['visit_coverage'] = working_df['ApplicationNo'].isin(visit_df['Application Id']).astype(int)
working_df['total_coverage'] = np.where((working_df['call_coverage'] > 0) | (working_df['visit_coverage'] > 0), 1, 0)


cv_df['count'] = 1
intensity_pivot = cv_df.pivot_table(index='Application Id', columns='Feedback Type', values='count', aggfunc='count', fill_value=0)
intensity_pivot.reset_index(inplace=True)

working_df['call_intensity'] = pd.merge(working_df, intensity_pivot[['Application Id', 'Call']], left_on="ApplicationNo", right_on="Application Id", how="left")["Call"]
working_df['visit_intensity'] = pd.merge(working_df, intensity_pivot[['Application Id', 'Visit']], left_on="ApplicationNo", right_on="Application Id", how="left")["Visit"]
working_df['total_intensity'] = working_df['call_intensity'] + working_df['visit_intensity']
working_df['manager_name'] = pd.merge(working_df, hr_df[['Employee Id', 'Reports To']], left_on="officer_emp_id", right_on="Employee Id", how="left")["Reports To"]

working_df['ptp_date'] = pd.merge(working_df, cv_df_sorted[['Application Id', 'Ptp Date']], left_on="ApplicationNo", right_on="Application Id", how="left")["Ptp Date"]
working_df['ptp_date'] = pd.to_datetime(working_df['ptp_date'], errors='coerce', format='mixed')
working_df['ptp_type'] = working_df.apply(
    lambda row: 'NA' if row['status'] == 'Paid' or pd.isna(row['ptp_date']) or row['ptp_date'] == pd.Timestamp('1970-01-01') else
                'Future PTP' if row['status'] == 'Unpaid' and row['ptp_date'].date() > today else
                'Today’s PTP' if row['status'] == 'Unpaid' and row['ptp_date'].date() == today else
                'Broken PTP' if row['status'] == 'Unpaid' and row['ptp_date'].date() < today else
                'NA',
    axis=1)

dialer_intensity_pivot = cv_df.pivot_table(index='Application Id', columns='Disposition', values='count', aggfunc='count', fill_value=0)
dialer_intensity_pivot = dialer_intensity_pivot[['Connected', 'Missed']]
dialer_intensity_pivot['Total_Contact'] = dialer_intensity_pivot['Connected'] + dialer_intensity_pivot['Missed']
dialer_intensity_pivot.reset_index(inplace=True)


working_df['dialer_coverage'] = (working_df['ApplicationNo'].isin(dialer_intensity_pivot.loc[dialer_intensity_pivot['Total_Contact'] > 0, 'Application Id']).fillna(0).astype(int))
working_df['dialer_contact'] = (working_df['ApplicationNo'].map(dialer_intensity_pivot.set_index('Application Id')['Connected']).fillna(0).astype(int))
working_df['dialer_intensity'] = (working_df['ApplicationNo'].map(dialer_intensity_pivot.set_index('Application Id')['Total_Contact']).fillna(0).astype(int))

cv_df_sorted['Date Of Call Or Visit'] = pd.to_datetime(cv_df_sorted['Date Of Call Or Visit'], errors='coerce', format='mixed')
disposition_temp_df = (cv_df_sorted[~cv_df_sorted['Disposition'].isin(['Missed', 'Connected'])].drop_duplicates(subset=['Application Id'], keep='first'))
working_df['disposition'] = (pd.merge(working_df, disposition_temp_df[['Application Id', 'Disposition']], left_on="ApplicationNo", right_on="Application Id", how="left")["Disposition"].fillna('No Feedback'))

yesterday = today - timedelta(days=1) 
dialer_ftp_df = cv_df_sorted[(cv_df_sorted['Date Of Call Or Visit'].dt.date == yesterday) & (cv_df_sorted['Feedback Type'] == 'Call')]
visit_ftp_df = cv_df_sorted[(cv_df_sorted['Date Of Call Or Visit'].dt.date == yesterday) & (cv_df_sorted['Feedback Type'] == 'Visit')]

dialer_ftp_grouped = dialer_ftp_df.groupby('Application Id')['Disposition'].value_counts().unstack(fill_value=0)
dialer_ftp_grouped = dialer_ftp_grouped[['Connected', 'Missed']].reset_index()

# Merge once and calculate the sum of 'Connected' and 'Missed'
merged_df = pd.merge(working_df, dialer_ftp_grouped, left_on="ApplicationNo", right_on="Application Id", how="left")
working_df['click_to_ftd_calls'] = (merged_df['Connected'] + merged_df['Missed']).fillna(0).astype(int)

# Calculate total_ftd_calls using value_counts and map
app_id_counts = dialer_ftp_df['Application Id'].value_counts()
working_df['total_ftd_calls'] = working_df['ApplicationNo'].map(app_id_counts).fillna(0).astype(int)
app_id_counts = visit_ftp_df['Application Id'].value_counts()
working_df['ftd_visits'] = working_df['ApplicationNo'].map(app_id_counts).fillna(0).astype(int)

# List of columns need to fill NA values with 0
columns_to_fill = ['click_to_ftd_calls', 'total_ftd_calls', 
                'ftd_visits', 'dialer_intensity', 'dialer_contact', 
                'dialer_coverage', 'total_intensity', 'visit_intensity', 'call_intensity']

# Fill NA values with 0 in the specified columns
working_df[columns_to_fill] = working_df[columns_to_fill].fillna(0)

columns_to_drop = ["OpeningPosPrevMonth", "NoOfPaidEmiInCurrentMonth", "LastPaymentDate", "Nachmode", "BounceStatus", "DsaCode", "Category", "MOB", "MOB_SLAB", 
                "CollectorName", "CollectorEmpID", "CollectorMobileNo", "collectordesignation", "Unnamed: 20", "risk_segment", "designation"]
working_df.drop(columns=columns_to_drop, inplace=True)

sort_order = ["ApplicationNo", "Nbfc", "customer_name", "opening_bucket", "officer_name", "officer_mobile", "officer_emp_id", "OpeningPos", "status", "City", 
            "State", "Zone", "cluster", "risk_segment_1", "risk_segment_2", "allocation_logic", "allocation_status", "field_allocation", "call_coverage", "visit_coverage", 
            "total_coverage", "disposition", "call_intensity", "visit_intensity", "total_intensity", "manager_name", "ptp_date", "ptp_type", "dialer_coverage", "dialer_contact", 
            "dialer_intensity", "click_to_ftd_calls", "total_ftd_calls", "ftd_visits"]
working_df = working_df[sort_order]


working_df.set_index('ApplicationNo', inplace=True)
zero_emi.set_index('ApplicationNo', inplace=True)
working_df.update(zero_emi['status'], overwrite=True)

working_df.reset_index(inplace=True)
unpaid_and_field_df = working_df[(working_df['status'] == 'Unpaid') & (working_df['field_allocation'] == 'Yes')]

def pivot_bucket_and_zone_lvl(unpaid_and_field_df):
    # Convert pandas DataFrame to Dask DataFrame
    dask_df = dd.from_pandas(unpaid_and_field_df, npartitions=4)  # Adjust npartitions based on your dataset size

    # Define aggregation functions
    aggfunc = {
        'ApplicationNo': 'count',
        'call_coverage': 'sum',
        'visit_coverage': 'sum',
        'total_coverage': 'sum',
        'call_intensity': 'mean',
        'visit_intensity': 'mean',
        'total_intensity': 'mean',
    }

    # Group by 'opening_bucket' and 'Zone'
    grouped_bucket_zone = dask_df.groupby(['opening_bucket', 'Zone']).agg(aggfunc).reset_index()

    # Round intensity columns
    for col in ['call_intensity', 'visit_intensity', 'total_intensity']:
        grouped_bucket_zone[col] = grouped_bucket_zone[col].round(1)

    # Calculate coverage percentages
    for attempt, coverage in zip(['call_coverage', 'visit_coverage', 'total_coverage'],
                                ['call_coverage%', 'visit_coverage%', 'total_coverage%']):
        grouped_bucket_zone[coverage] = (np.ceil((grouped_bucket_zone[attempt] / grouped_bucket_zone['ApplicationNo']) * 100)).astype(int)

    # Group by 'opening_bucket' only
    grouped_bucket = dask_df.groupby('opening_bucket').agg(aggfunc).reset_index()

    # Round intensity columns
    for col in ['call_intensity', 'visit_intensity', 'total_intensity']:
        grouped_bucket[col] = grouped_bucket[col].round(1)

    # Calculate coverage percentages
    for attempt, coverage in zip(['call_coverage', 'visit_coverage', 'total_coverage'],
                                ['call_coverage%', 'visit_coverage%', 'total_coverage%']):
        grouped_bucket[coverage] = (np.ceil((grouped_bucket[attempt] / grouped_bucket['ApplicationNo']) * 100)).astype(int)

    # Add 'Total' label and blank 'Zone'
    grouped_bucket['opening_bucket'] = grouped_bucket['opening_bucket'].astype(str) + ' Total'
    grouped_bucket['Zone'] = ''

    # Sort both dataframes
    grouped_bucket_zone_sorted = grouped_bucket_zone.compute().sort_values(by='opening_bucket')
    grouped_bucket_sorted = grouped_bucket.compute().sort_values(by='opening_bucket')

    # Combine dataframes with 'Total' rows
    final_rows = []
    num_zones = 6  # Assuming 6 zones per bucket
    for i in range(0, len(grouped_bucket_zone_sorted), num_zones):
        chunk = grouped_bucket_zone_sorted.iloc[i:i + num_zones]
        final_rows.append(chunk)
        total_row = grouped_bucket_sorted[grouped_bucket_sorted['opening_bucket'] == f"{int(chunk['opening_bucket'].iloc[0])} Total"]
        final_rows.append(total_row)

    final_df = pd.concat(final_rows, ignore_index=True)

    # Add Grand Total row
    grand_total = grouped_bucket_zone_sorted[['ApplicationNo', 'call_coverage', 'visit_coverage', 'total_coverage']].sum()
    avg_total = grouped_bucket_zone_sorted[['call_intensity', 'visit_intensity', 'total_intensity']].mean()

    grand_total_row = pd.Series({
        'opening_bucket': 'Grand Total',
        'Zone': '',
        'ApplicationNo': grand_total['ApplicationNo'],
        'call_coverage': grand_total['call_coverage'],
        'visit_coverage': grand_total['visit_coverage'],
        'total_coverage': grand_total['total_coverage'],
        'call_intensity': avg_total['call_intensity'].round(1),
        'visit_intensity': avg_total['visit_intensity'].round(1),
        'total_intensity': avg_total['total_intensity'].round(1),
        'call_coverage%': (grand_total['call_coverage'] / grand_total['ApplicationNo'] * 100).astype(int),
        'visit_coverage%': (grand_total['visit_coverage'] / grand_total['ApplicationNo'] * 100).astype(int),
        'total_coverage%': (grand_total['total_coverage'] / grand_total['ApplicationNo'] * 100).astype(int)
    })

    final_df = pd.concat([final_df, grand_total_row.to_frame().T], ignore_index=True)

    return final_df

def pivot_zone_lvl(unpaid_and_field_df):
    

    # Define aggregation functions
    aggfunc = {
        'ApplicationNo': 'count',
        'click_to_ftd_calls': 'sum',  # Replaced lambda with np.sum
        'total_ftd_calls': 'sum',     # Replaced lambda with np.sum
        'ftd_visits': 'sum',          # Replaced lambda with np.sum
        'call_coverage': 'sum',
        'visit_coverage': 'sum',
        'total_coverage': 'sum',
        'call_intensity': 'mean',
        'visit_intensity': 'mean',
        'total_intensity': 'mean',
    }

    # Group by 'Zone'
    grouped_bucket_zone = unpaid_and_field_df.groupby(['Zone']).agg(aggfunc).reset_index()

    # Round intensity columns
    grouped_bucket_zone[['call_intensity', 'visit_intensity', 'total_intensity']] = grouped_bucket_zone[
        ['call_intensity', 'visit_intensity', 'total_intensity']
    ].round(1)

    # Calculate coverage percentages
    for attempt, coverage in zip(
        ['total_ftd_calls', 'ftd_visits', 'call_coverage', 'visit_coverage', 'total_coverage'],
        ['total_ftd_calls%', 'ftd_visits%', 'call_coverage%', 'visit_coverage%', 'total_coverage%']
    ):
        grouped_bucket_zone[coverage] = ((grouped_bucket_zone[attempt] / grouped_bucket_zone['ApplicationNo']) * 100).astype(float).round(1)

    # Select and reorder columns
    columns_list = [
        'Zone', 'ApplicationNo', 'click_to_ftd_calls', 'total_ftd_calls', 'total_ftd_calls%',
        'ftd_visits', 'ftd_visits%', 'call_coverage', 'visit_coverage', 'total_coverage',
        'call_intensity', 'visit_intensity', 'total_intensity', 'call_coverage%', 'visit_coverage%', 'total_coverage%'
    ]
    grouped_bucket_zone = grouped_bucket_zone[columns_list]

    # Add a total row
    total_row = grouped_bucket_zone.sum(numeric_only=True).astype(int).round(1)
    total_row['Zone'] = 'Grand Total'
    total_row['call_intensity'] = grouped_bucket_zone['call_intensity'].mean().round(1)
    total_row['visit_intensity'] = grouped_bucket_zone['visit_intensity'].mean().round(1)
    total_row['total_intensity'] = grouped_bucket_zone['total_intensity'].mean().round(1)

    # Calculate coverage percentages for the total row
    for attempt, coverage in zip(
        ['total_ftd_calls', 'ftd_visits', 'call_coverage', 'visit_coverage', 'total_coverage'],
        ['total_ftd_calls%', 'ftd_visits%', 'call_coverage%', 'visit_coverage%', 'total_coverage%']
    ):
        total_row[coverage] = np.round(((total_row[attempt] / total_row['ApplicationNo']) * 100), 1).astype(float).round(1)

    # Append the total row to the DataFrame
    grouped_bucket_zone = pd.concat([grouped_bucket_zone, total_row.to_frame().T], ignore_index=True)

    return grouped_bucket_zone

def pivot_zone_and_bucket_lvl(unpaid_and_field_df):
    # Convert pandas DataFrame to Dask DataFrame
    dask_df = dd.from_pandas(unpaid_and_field_df, npartitions=4)  # Adjust npartitions based on your dataset size

    # Define aggregation functions
    aggfunc = {
        'ApplicationNo': 'count',
        'click_to_ftd_calls': 'sum',
        'total_ftd_calls': 'sum',
        'ftd_visits': 'sum',
        'call_coverage': 'sum',
        'visit_coverage': 'sum',
        'total_coverage': 'sum',
        'call_intensity': 'mean',
        'visit_intensity': 'mean',
        'total_intensity': 'mean',
    }

    # Group by 'Zone' and 'opening_bucket'
    grouped_bucket_zone = dask_df.groupby(['Zone', 'opening_bucket']).agg(aggfunc).reset_index()

    # Round intensity columns
    grouped_bucket_zone[['call_intensity', 'visit_intensity', 'total_intensity']] = grouped_bucket_zone[
        ['call_intensity', 'visit_intensity', 'total_intensity']].round(1)

    # Calculate coverage percentages in a vectorized way
    coverage_columns = ['call_coverage', 'visit_coverage', 'total_coverage', 'total_ftd_calls', 'ftd_visits']
    coverage_percent_columns = [f'{col}%' for col in coverage_columns]
    
    # Vectorized percentage calculation for all coverage columns
    for attempt, coverage in zip(coverage_columns, coverage_percent_columns):
        grouped_bucket_zone[coverage] = (grouped_bucket_zone[attempt] / grouped_bucket_zone['ApplicationNo'] * 100).round(1)

    # Group by 'Zone' only for total calculations
    grouped_bucket = dask_df.groupby('Zone').agg(aggfunc).reset_index()

    # Round intensity columns
    grouped_bucket[['call_intensity', 'visit_intensity', 'total_intensity']] = grouped_bucket[
        ['call_intensity', 'visit_intensity', 'total_intensity']].round(1)

    # Calculate coverage percentages for grouped_bucket
    for attempt, coverage in zip(coverage_columns, coverage_percent_columns):
        grouped_bucket[coverage] = (grouped_bucket[attempt] / grouped_bucket['ApplicationNo'] * 100).round(1)

    # Add 'Total' label and blank 'Zone'
    grouped_bucket['Zone'] = grouped_bucket['Zone'].astype(str) + ' Total'
    grouped_bucket['opening_bucket'] = ''

    # Sort both dataframes
    grouped_bucket_zone_sorted = grouped_bucket_zone.compute().sort_values(by=['Zone', 'opening_bucket'])
    grouped_bucket_sorted = grouped_bucket.compute().sort_values(by='Zone')

    zone_frequency = grouped_bucket_zone_sorted['Zone'].value_counts().sort_values(ascending=True)
    
    final_rows = []
    
    # Iterate through each unique zone
    for zone in zone_frequency.index:
        # Filter rows for the current zone
        zone_rows = grouped_bucket_zone_sorted[grouped_bucket_zone_sorted['Zone'] == zone]
        final_rows.append(zone_rows)
        
        # Append the corresponding total row for the current zone
        total_row = grouped_bucket_sorted[grouped_bucket_sorted['Zone'] == f"{zone} Total"]
        final_rows.append(total_row)
    
    # Combine all rows into a final DataFrame
    final_df = pd.concat(final_rows, ignore_index=True)

    # # Combine dataframes with 'Total' rows
    # final_rows = []
    # num_zones = 6  # Assuming 6 zones per bucket
    # for i in range(0, len(grouped_bucket_zone_sorted), num_zones):
    #     chunk = grouped_bucket_zone_sorted.iloc[i:i + num_zones]
    #     final_rows.append(chunk)
    #     total_row = grouped_bucket_sorted[grouped_bucket_sorted['Zone'] == f"{chunk['Zone'].iloc[0]} Total"]
    #     final_rows.append(total_row)

    # final_df = pd.concat(final_rows, ignore_index=True)

    # Add Grand Total row in a vectorized manner
    grand_total = grouped_bucket_zone_sorted[['ApplicationNo', 'call_coverage', 'visit_coverage', 'total_coverage', 
                                            'click_to_ftd_calls', 'total_ftd_calls', 'ftd_visits']].sum()
    avg_total = grouped_bucket_zone_sorted[['call_intensity', 'visit_intensity', 'total_intensity']].mean()

    grand_total_row = pd.Series({
        'Zone': 'Grand Total',
        'opening_bucket': '',
        'ApplicationNo': grand_total['ApplicationNo'],
        'call_coverage': grand_total['call_coverage'],
        'visit_coverage': grand_total['visit_coverage'],
        'total_coverage': grand_total['total_coverage'],
        'call_intensity': avg_total['call_intensity'].round(1),
        'visit_intensity': avg_total['visit_intensity'].round(1),
        'total_intensity': avg_total['total_intensity'].round(1),
        'call_coverage%': (grand_total['call_coverage'] / grand_total['ApplicationNo'] * 100).round(1),
        'visit_coverage%': (grand_total['visit_coverage'] / grand_total['ApplicationNo'] * 100).round(1),
        'total_coverage%': (grand_total['total_coverage'] / grand_total['ApplicationNo'] * 100).round(1),
        'click_to_ftd_calls': grand_total['click_to_ftd_calls'],
        'total_ftd_calls': grand_total['total_ftd_calls'],
        'ftd_visits': grand_total['ftd_visits'],
        'total_ftd_calls%': (grand_total['total_ftd_calls'] / grand_total['ApplicationNo'] * 100).round(1),
        'ftd_visits%': (grand_total['ftd_visits'] / grand_total['ApplicationNo'] * 100).round(1)
    })

    final_df = pd.concat([final_df, grand_total_row.to_frame().T], ignore_index=True)

    # Sorting the final dataframe
    sort_order = ['Zone', 'opening_bucket', 'ApplicationNo', 'click_to_ftd_calls', 'total_ftd_calls', 
                'total_ftd_calls%', 'ftd_visits', 'ftd_visits%', 'call_coverage', 'visit_coverage', 
                'total_coverage', 'call_intensity', 'visit_intensity', 'total_intensity', 'call_coverage%', 
                'visit_coverage%', 'total_coverage%']
    final_df = final_df[sort_order]

    return final_df

def pivot_zone_and_x_lvl(working_df):
    # Filter the DataFrame
    unpaid_and_man_df = working_df[(working_df['status'] == 'Unpaid') & (working_df['opening_bucket'] == 0)]

    # Define aggregation functions
    aggfunc = {
        'ApplicationNo': 'count',
        'click_to_ftd_calls': 'sum',  # Replaced lambda with np.sum
        'total_ftd_calls': 'sum',     # Replaced lambda with np.sum
        'ftd_visits': 'sum',          # Replaced lambda with np.sum
        'call_coverage': 'sum',
        'visit_coverage': 'sum',
        'total_coverage': 'sum',
        'call_intensity': 'mean',
        'visit_intensity': 'mean',
        'total_intensity': 'mean',
    }

    # Group by 'Zone'
    grouped_bucket_zone = unpaid_and_man_df.groupby(['Zone']).agg(aggfunc).reset_index()

    # Round intensity columns
    grouped_bucket_zone[['call_intensity', 'visit_intensity', 'total_intensity']] = grouped_bucket_zone[
        ['call_intensity', 'visit_intensity', 'total_intensity']
    ].round(1)

    # Calculate coverage percentages
    for attempt, coverage in zip(
        ['click_to_ftd_calls', 'ftd_visits', 'call_coverage', 'visit_coverage', 'total_coverage'],
        ['click_to_ftd_calls%', 'ftd_visits%', 'call_coverage%', 'visit_coverage%', 'total_coverage%']
    ):
        grouped_bucket_zone[coverage] = ((grouped_bucket_zone[attempt] / grouped_bucket_zone['ApplicationNo']) * 100).astype(float).round(1)

    # Select and reorder columns
    columns_list = [
        'Zone', 'ApplicationNo', 'click_to_ftd_calls', 'total_ftd_calls', 'click_to_ftd_calls%',
        'ftd_visits', 'ftd_visits%', 'call_coverage', 'visit_coverage', 'total_coverage',
        'call_intensity', 'visit_intensity', 'total_intensity', 'call_coverage%', 'visit_coverage%', 'total_coverage%'
    ]
    grouped_bucket_zone = grouped_bucket_zone[columns_list]

    # Add a total row
    total_row = grouped_bucket_zone.sum(numeric_only=True).astype(int).round(1)
    total_row['Zone'] = 'Grand Total'
    total_row['call_intensity'] = round(grouped_bucket_zone['call_intensity'].mean(), 1)
    total_row['visit_intensity'] = round(grouped_bucket_zone['visit_intensity'].mean(), 1)
    total_row['total_intensity'] = round(grouped_bucket_zone['total_intensity'].mean(), 1)

    # Calculate coverage percentages for the total row
    for attempt, coverage in zip(
        ['click_to_ftd_calls', 'ftd_visits', 'call_coverage', 'visit_coverage', 'total_coverage'],
        ['click_to_ftd_calls%', 'ftd_visits%', 'call_coverage%', 'visit_coverage%', 'total_coverage%']
    ):
        # total_row[coverage] = ((total_row[attempt] / total_row['ApplicationNo']) * 100).astype(float).round(1)
        total_row[coverage] = np.round(((total_row[attempt] / total_row['ApplicationNo']) * 100), 1).astype(float).round(1)

    # Convert all numeric columns to integers except intensity columns

    # Append the total row to the DataFrame
    grouped_bucket_zone = pd.concat([grouped_bucket_zone, total_row.to_frame().T], ignore_index=True)

    return grouped_bucket_zone

def pivot_zone_and_30_lvl(working_df):
    # Filter the DataFrame
    unpaid_and_man_df = working_df[(working_df['status'] == 'Unpaid') & (working_df['opening_bucket'] == 30)]

    # Define aggregation functions
    aggfunc = {
        'ApplicationNo': 'count',
        'click_to_ftd_calls': 'sum',  
        'total_ftd_calls': 'sum',     
        'ftd_visits': 'sum',
        'call_coverage': 'sum',          
        'visit_coverage': 'sum',
        'total_coverage': 'sum',
        'call_intensity': 'mean',
        'visit_intensity': 'mean',
        'total_intensity': 'mean',
    }

    # Group by 'Zone'
    grouped_bucket_zone = unpaid_and_man_df.groupby(['Zone']).agg(aggfunc).reset_index()

    # Round intensity columns
    grouped_bucket_zone[['call_intensity', 'visit_intensity', 'total_intensity']] = grouped_bucket_zone[
        ['call_intensity', 'visit_intensity', 'total_intensity']
    ].round(1)

    # Calculate coverage percentages
    for attempt, coverage in zip(
        ['click_to_ftd_calls', 'ftd_visits', 'call_coverage', 'visit_coverage', 'total_coverage'],
        ['click_to_ftd_calls%', 'ftd_visits%', 'call_coverage%', 'visit_coverage%', 'total_coverage%']
    ):
        grouped_bucket_zone[coverage] = ((grouped_bucket_zone[attempt] / grouped_bucket_zone['ApplicationNo']) * 100).astype(float).round(1)

    # Select and reorder columns
    columns_list = [
        'Zone', 'ApplicationNo', 'click_to_ftd_calls', 'total_ftd_calls', 'click_to_ftd_calls%',
        'ftd_visits', 'ftd_visits%', 'call_coverage', 'visit_coverage', 'total_coverage',
        'call_intensity', 'visit_intensity', 'total_intensity', 'call_coverage%', 'visit_coverage%', 'total_coverage%'
    ]
    grouped_bucket_zone = grouped_bucket_zone[columns_list]

    # Add a total row
    total_row = grouped_bucket_zone.sum(numeric_only=True).astype(int).round(1)
    total_row['Zone'] = 'Grand Total'
    total_row['call_intensity'] = round(grouped_bucket_zone['call_intensity'].mean(), 1)
    total_row['visit_intensity'] = round(grouped_bucket_zone['visit_intensity'].mean(), 1)
    total_row['total_intensity'] = round(grouped_bucket_zone['total_intensity'].mean(), 1)

    # Calculate coverage percentages for the total row
    for attempt, coverage in zip(
        ['click_to_ftd_calls', 'ftd_visits', 'call_coverage', 'visit_coverage', 'total_coverage'],
        ['click_to_ftd_calls%', 'ftd_visits%', 'call_coverage%', 'visit_coverage%', 'total_coverage%']
    ):
        total_row[coverage] = (np.round((total_row[attempt] / total_row['ApplicationNo']) * 100)).astype(float).round(1)

    # Append the total row to the DataFrame
    grouped_bucket_zone = pd.concat([grouped_bucket_zone, total_row.to_frame().T], ignore_index=True)

    return grouped_bucket_zone

def pivot_state_and_bucket_lvl(unpaid_and_field_df):
    # Convert pandas DataFrame to Dask DataFrame
    dask_df = dd.from_pandas(unpaid_and_field_df, npartitions=4)  # Adjust npartitions based on your dataset size

    # Define aggregation functions
    aggfunc = {
        'ApplicationNo': 'count',
        'click_to_ftd_calls': 'sum',
        'total_ftd_calls': 'sum',
        'ftd_visits': 'sum',
        'call_coverage': 'sum',
        'visit_coverage': 'sum',
        'total_coverage': 'sum',
        'call_intensity': 'mean',
        'visit_intensity': 'mean',
        'total_intensity': 'mean',
    }

    # Group by 'State' and 'opening_bucket'
    grouped_bucket_zone = dask_df.groupby(['State', 'opening_bucket']).agg(aggfunc).reset_index()

    
    # Round intensity columns
    for col in ['call_intensity', 'visit_intensity', 'total_intensity']:
        grouped_bucket_zone[col] = grouped_bucket_zone[col].round(1)

    # Calculate coverage percentages
    for attempt, coverage in zip(['call_coverage', 'visit_coverage', 'total_coverage', 'total_ftd_calls', 'ftd_visits'],
                                ['call_coverage%', 'visit_coverage%', 'total_coverage%', 'total_ftd_calls%', 'ftd_visits%']):
        grouped_bucket_zone[coverage] = ((grouped_bucket_zone[attempt] / grouped_bucket_zone['ApplicationNo']) * 100).astype(float).round(1)

    # Group by 'State' only
    grouped_bucket = dask_df.groupby('State').agg(aggfunc).reset_index()

    # Round intensity columns
    for col in ['call_intensity', 'visit_intensity', 'total_intensity']:
        grouped_bucket[col] = grouped_bucket[col].round(1)

    # Calculate coverage percentages
    for attempt, coverage in zip(['call_coverage', 'visit_coverage', 'total_coverage', 'total_ftd_calls', 'ftd_visits'],
                                ['call_coverage%', 'visit_coverage%', 'total_coverage%', 'total_ftd_calls%', 'ftd_visits%']):
        grouped_bucket[coverage] = ((grouped_bucket[attempt] / grouped_bucket['ApplicationNo']) * 100).astype(float).round(1)

    # Add 'Total' label and blank 'State'
    grouped_bucket['State'] = grouped_bucket['State'].astype(str) + ' Total'
    grouped_bucket['opening_bucket'] = ''

    # Sort both dataframes
    grouped_bucket_zone_sorted = grouped_bucket_zone.compute().sort_values(by='State')
    grouped_bucket_sorted = grouped_bucket.compute().sort_values(by='State')
    
    #---------------------------------------------------------------test -------------------------------------------
    
    # Calculate the frequency of each 'State' and sort by frequency
    final_rows = []
    # Calculate the frequency of each 'State' and sort by frequency
    state_counts = grouped_bucket_zone_sorted['State'].value_counts().reset_index()
    state_counts.columns = ['State', 'Count']

    # Sort the states by frequency
    sorted_state_counts = state_counts.sort_values(by='Count', ascending=False)

    # Iterate through each state in sorted order
    for state, count in zip(sorted_state_counts['State'], sorted_state_counts['Count']):
        # Get the rows corresponding to this state
        state_rows = grouped_bucket_zone_sorted[grouped_bucket_zone_sorted['State'] == state]
        
        # Sort the rows for this state by 'opening_bucket'
        state_rows_sorted = state_rows.sort_values(by='opening_bucket')
        
        # Append the sorted rows for this state to final_rows
        final_rows.append(state_rows_sorted)
        
        # Find the "Total" row for this state
        total_row = grouped_bucket_sorted[grouped_bucket_sorted['State'] == f"{state} Total"]
        
        # Append the "Total" row to final_rows
        final_rows.append(total_row)

    # Combine all the final rows into one DataFrame
    final_df = pd.concat(final_rows, ignore_index=True)

    #-------------------------------------- old sorting logic -----------------------------------------------------
    # # Combine dataframes with 'Total' rows
    # final_rows = []
    # num_zones = 6  # Assuming 6 zones per bucket
    # for i in range(0, len(grouped_bucket_zone_sorted), num_zones):
    #     chunk = grouped_bucket_zone_sorted.iloc[i:i + num_zones]
    #     final_rows.append(chunk)
    #     total_row = grouped_bucket_sorted[grouped_bucket_sorted['State'] == f"{chunk['State'].iloc[0]} Total"]
    #     final_rows.append(total_row)

    # final_df = pd.concat(final_rows, ignore_index=True)

    # Add Grand Total row
    grand_total = grouped_bucket_zone_sorted[['ApplicationNo', 'call_coverage', 'visit_coverage', 'total_coverage', 'click_to_ftd_calls', 'total_ftd_calls', 'ftd_visits']].sum()
    avg_total = grouped_bucket_zone_sorted[['call_intensity', 'visit_intensity', 'total_intensity']].mean()

    grand_total_row = pd.Series({
        'State': 'Grand Total',
        'opening_bucket': '',
        'ApplicationNo': grand_total['ApplicationNo'],
        'call_coverage': grand_total['call_coverage'],
        'visit_coverage': grand_total['visit_coverage'],
        'total_coverage': grand_total['total_coverage'],
        'call_intensity': avg_total['call_intensity'].round(1),
        'visit_intensity': avg_total['visit_intensity'].round(1),
        'total_intensity': avg_total['total_intensity'].round(1),
        'call_coverage%': (grand_total['call_coverage'] / grand_total['ApplicationNo'] * 100).astype(float).round(1),
        'visit_coverage%': (grand_total['visit_coverage'] / grand_total['ApplicationNo'] * 100).astype(float).round(1),
        'total_coverage%': (grand_total['total_coverage'] / grand_total['ApplicationNo'] * 100).astype(float).round(1),
        'click_to_ftd_calls': grand_total['click_to_ftd_calls'],
        'total_ftd_calls' : grand_total['total_ftd_calls'],
        'ftd_visits': grand_total['ftd_visits'],
        'total_ftd_calls%': (grand_total['total_ftd_calls'] / grand_total['ApplicationNo'] * 100).astype(float).round(1),
        'ftd_visits%' : (grand_total['ftd_visits'] / grand_total['ApplicationNo'] * 100).astype(float).round(1)
    })

    final_df = pd.concat([final_df, grand_total_row.to_frame().T], ignore_index=True)
    sort_order = ['State', 'opening_bucket', 'ApplicationNo', 'click_to_ftd_calls', 'total_ftd_calls', 'total_ftd_calls%', 'ftd_visits', 'ftd_visits%', 'call_coverage', 'visit_coverage', 'total_coverage', 'call_intensity', 'visit_intensity', 'total_intensity', 'call_coverage%', 'visit_coverage%', 'total_coverage%']
    final_df = final_df[sort_order]
    return final_df

def pivot_call_intensity_and_zone_lvl(unpaid_and_field_df):
    unpaid_and_field_df['call_coverage_1'] = unpaid_and_field_df['call_coverage'].copy()
    unpaid_and_field_df = unpaid_and_field_df[unpaid_and_field_df['call_intensity'].isin([0,1])].copy()
    
    # Convert pandas DataFrame to Dask DataFrame
    dask_df = dd.from_pandas(unpaid_and_field_df, npartitions=4)  # Adjust npartitions based on your dataset size

    # Define aggregation functions
    aggfunc = {
        'ApplicationNo': 'count',
        'call_coverage': 'sum',
        'visit_coverage': 'sum',
        'total_coverage': 'sum',
        'call_intensity': 'mean',
        'visit_intensity': 'mean',
        'total_intensity': 'mean',
    }

    # Group by 'call_coverage_1' and 'Zone'
    grouped_bucket_zone = dask_df.groupby(['call_coverage_1', 'Zone']).agg(aggfunc).reset_index()

    # Round intensity columns
    for col in ['call_intensity', 'visit_intensity', 'total_intensity']:
        grouped_bucket_zone[col] = grouped_bucket_zone[col].round(1)

    # Calculate coverage percentages
    for attempt, coverage in zip(['call_coverage', 'visit_coverage', 'total_coverage'],
                                ['call_coverage%', 'visit_coverage%', 'total_coverage%']):
        grouped_bucket_zone[coverage] = ((grouped_bucket_zone[attempt] / grouped_bucket_zone['ApplicationNo']) * 100).astype(float).round(1)

    # Group by 'call_coverage_1' only
    grouped_bucket = dask_df.groupby('call_coverage_1').agg(aggfunc).reset_index()

    # Round intensity columns
    for col in ['call_intensity', 'visit_intensity', 'total_intensity']:
        grouped_bucket[col] = grouped_bucket[col].round(1)

    # Calculate coverage percentages
    for attempt, coverage in zip(['call_coverage', 'visit_coverage', 'total_coverage'],
                                ['call_coverage%', 'visit_coverage%', 'total_coverage%']):
        grouped_bucket[coverage] = ((grouped_bucket[attempt] / grouped_bucket['ApplicationNo']) * 100).astype(float).round(1)

    # Add 'Total' label and blank 'Zone'
    grouped_bucket['call_coverage_1'] = grouped_bucket['call_coverage_1'].astype(str) + ' Total'
    grouped_bucket['Zone'] = ''

    # Sort both dataframes
    grouped_bucket_zone_sorted = grouped_bucket_zone.compute().sort_values(by=['call_coverage_1', 'Zone'])
    grouped_bucket_sorted = grouped_bucket.compute().sort_values(by='call_coverage_1')

    # Calculate the number of zones dynamically
    num_zones_0 = grouped_bucket_zone_sorted[grouped_bucket_zone_sorted['call_coverage_1'] == 0]['Zone'].nunique()
    num_zones_1 = grouped_bucket_zone_sorted[grouped_bucket_zone_sorted['call_coverage_1'] == 1]['Zone'].nunique()

    # Combine dataframes with 'Total' rows
    final_rows = []
    for call_coverage_value, num_zones in zip([0, 1], [num_zones_0, num_zones_1]):
        chunk = grouped_bucket_zone_sorted[grouped_bucket_zone_sorted['call_coverage_1'] == call_coverage_value]
        final_rows.append(chunk)
        total_row = grouped_bucket_sorted[grouped_bucket_sorted['call_coverage_1'] == f"{call_coverage_value} Total"]
        final_rows.append(total_row)

    final_df = pd.concat(final_rows, ignore_index=True)

    # Add Grand Total row
    grand_total = grouped_bucket_zone_sorted[['ApplicationNo', 'call_coverage', 'visit_coverage', 'total_coverage']].sum()
    avg_total = grouped_bucket_zone_sorted[['call_intensity', 'visit_intensity', 'total_intensity']].mean()

    grand_total_row = pd.Series({
        'call_coverage_1': 'Grand Total',
        'Zone': '',
        'ApplicationNo': grand_total['ApplicationNo'],
        'call_coverage': grand_total['call_coverage'],
        'visit_coverage': grand_total['visit_coverage'],
        'total_coverage': grand_total['total_coverage'],
        'call_intensity': avg_total['call_intensity'].round(1),
        'visit_intensity': avg_total['visit_intensity'].round(1),
        'total_intensity': avg_total['total_intensity'].round(1),
        'call_coverage%': (grand_total['call_coverage'] / grand_total['ApplicationNo'] * 100).astype(float).round(1),
        'visit_coverage%': (grand_total['visit_coverage'] / grand_total['ApplicationNo'] * 100).astype(float).round(1),
        'total_coverage%': (grand_total['total_coverage'] / grand_total['ApplicationNo'] * 100).astype(float).round(1)
    })

    final_df = pd.concat([final_df, grand_total_row.to_frame().T], ignore_index=True)

    return final_df

def pivot_visit_intensity_and_zone_lvl(unpaid_and_field_df):
    unpaid_and_field_df['visit_coverage_1'] = unpaid_and_field_df['visit_coverage'].copy()
    
    unpaid_and_field_df = unpaid_and_field_df[unpaid_and_field_df['visit_intensity'].isin([0,1])]
    # Convert pandas DataFrame to Dask DataFrame
    dask_df = dd.from_pandas(unpaid_and_field_df, npartitions=4)  # Adjust npartitions based on your dataset size

    # Define aggregation functions
    aggfunc = {
        'ApplicationNo': 'count',
        'call_coverage': 'sum',
        'visit_coverage': 'sum',
        'total_coverage': 'sum',
        'call_intensity': 'mean',
        'visit_intensity': 'mean',
        'total_intensity': 'mean',
    }

    # Group by 'visit_coverage_1' and 'Zone'
    grouped_bucket_zone = dask_df.groupby(['visit_coverage_1', 'Zone']).agg(aggfunc).reset_index()

    # Round intensity columns
    for col in ['call_intensity', 'visit_intensity', 'total_intensity']:
        grouped_bucket_zone[col] = grouped_bucket_zone[col].round(1)

    # Calculate coverage percentages
    for attempt, coverage in zip(['call_coverage', 'visit_coverage', 'total_coverage'],
                                ['call_coverage%', 'visit_coverage%', 'total_coverage%']):
        grouped_bucket_zone[coverage] = ((grouped_bucket_zone[attempt] / grouped_bucket_zone['ApplicationNo'] * 100)).astype(float).round(1)

    # Group by 'visit_coverage_1' only
    grouped_bucket = dask_df.groupby('visit_coverage_1').agg(aggfunc).reset_index()

    # Round intensity columns
    for col in ['call_intensity', 'visit_intensity', 'total_intensity']:
        grouped_bucket[col] = grouped_bucket[col].round(1)

    # Calculate coverage percentages
    for attempt, coverage in zip(['call_coverage', 'visit_coverage', 'total_coverage'],
                                ['call_coverage%', 'visit_coverage%', 'total_coverage%']):
        grouped_bucket[coverage] = ((grouped_bucket[attempt] / grouped_bucket['ApplicationNo']) * 100).astype(float).round(1)

    # Add 'Total' label and blank 'Zone'
    grouped_bucket['visit_coverage_1'] = grouped_bucket['visit_coverage_1'].astype(str) + ' Total'
    grouped_bucket['Zone'] = ''

    # Sort both dataframes
    grouped_bucket_zone_sorted = grouped_bucket_zone.compute().sort_values(by=['visit_coverage_1', 'Zone'])
    grouped_bucket_sorted = grouped_bucket.compute().sort_values(by='visit_coverage_1')

    # Calculate the number of zones dynamically
    num_zones_0 = grouped_bucket_zone_sorted[grouped_bucket_zone_sorted['visit_coverage_1'] == 0]['Zone'].nunique()
    num_zones_1 = grouped_bucket_zone_sorted[grouped_bucket_zone_sorted['visit_coverage_1'] == 1]['Zone'].nunique()

    # Combine dataframes with 'Total' rows
    final_rows = []
    for call_coverage_value, num_zones in zip([0, 1], [num_zones_0, num_zones_1]):
        chunk = grouped_bucket_zone_sorted[grouped_bucket_zone_sorted['visit_coverage_1'] == call_coverage_value]
        final_rows.append(chunk)
        total_row = grouped_bucket_sorted[grouped_bucket_sorted['visit_coverage_1'] == f"{call_coverage_value} Total"]
        final_rows.append(total_row)

    final_df = pd.concat(final_rows, ignore_index=True)

    # Add Grand Total row
    grand_total = grouped_bucket_zone_sorted[['ApplicationNo', 'call_coverage', 'visit_coverage', 'total_coverage']].sum()
    avg_total = grouped_bucket_zone_sorted[['call_intensity', 'visit_intensity', 'total_intensity']].mean()

    grand_total_row = pd.Series({
        'visit_coverage_1': 'Grand Total',
        'Zone': '',
        'ApplicationNo': grand_total['ApplicationNo'],
        'call_coverage': grand_total['call_coverage'],
        'visit_coverage': grand_total['visit_coverage'],
        'total_coverage': grand_total['total_coverage'],
        'call_intensity': avg_total['call_intensity'].round(1),
        'visit_intensity': avg_total['visit_intensity'].round(1),
        'total_intensity': avg_total['total_intensity'].round(1),
        'call_coverage%': (grand_total['call_coverage'] / grand_total['ApplicationNo'] * 100).astype(float).round(1),
        'visit_coverage%': (grand_total['visit_coverage'] / grand_total['ApplicationNo'] * 100).astype(float).round(1),
        'total_coverage%': (grand_total['total_coverage'] / grand_total['ApplicationNo'] * 100).astype(float).round(1)
    })

    final_df = pd.concat([final_df, grand_total_row.to_frame().T], ignore_index=True)

    return final_df

def pivot_officer_lvl(working_df):
    # Filter the DataFrame
    unpaid_and_man_df = working_df.copy()

    # Define aggregation functions
    aggfunc = {
        'ApplicationNo': 'count',
        'click_to_ftd_calls': 'sum',  # Replaced lambda with np.sum
        'total_ftd_calls': 'sum',     # Replaced lambda with np.sum
        'ftd_visits': 'sum',          # Replaced lambda with np.sum
        'call_coverage': 'sum',
        'visit_coverage': 'sum',
        'total_coverage': 'sum',
        'call_intensity': 'mean',
        'visit_intensity': 'mean',
        'total_intensity': 'mean',
    }

    # Group by 'officer_name'
    grouped_bucket_zone = unpaid_and_man_df.groupby(['officer_name']).agg(aggfunc).reset_index()

    # Round intensity columns
    grouped_bucket_zone[['call_intensity', 'visit_intensity', 'total_intensity']] = grouped_bucket_zone[
        ['call_intensity', 'visit_intensity', 'total_intensity']
    ].round(1)

    # Calculate coverage percentages
    for attempt, coverage in zip(
        ['click_to_ftd_calls', 'ftd_visits', 'call_coverage', 'visit_coverage', 'total_coverage'],
        ['click_to_ftd_calls%', 'ftd_visits%', 'call_coverage%', 'visit_coverage%', 'total_coverage%']
    ):
        grouped_bucket_zone[coverage] = ((grouped_bucket_zone[attempt] / grouped_bucket_zone['ApplicationNo']) * 100).astype(float).round(0)

    # Select and reorder columns
    columns_list = [
        'officer_name', 'ApplicationNo', 'click_to_ftd_calls', 'total_ftd_calls', 'click_to_ftd_calls%',
        'ftd_visits', 'ftd_visits%', 'call_coverage', 'visit_coverage', 'total_coverage',
        'call_intensity', 'visit_intensity', 'total_intensity', 'call_coverage%', 'visit_coverage%', 'total_coverage%'
    ]
    grouped_bucket_zone = grouped_bucket_zone[columns_list]

    # Add a total row
    total_row = grouped_bucket_zone.sum(numeric_only=True)
    total_row['officer_name'] = 'Grand Total'
    total_row['call_intensity'] = grouped_bucket_zone['call_intensity'].mean().round(1)
    total_row['visit_intensity'] = grouped_bucket_zone['visit_intensity'].mean().round(1)
    total_row['total_intensity'] = grouped_bucket_zone['total_intensity'].mean().round(1)

    # Calculate coverage percentages for the total row
    for attempt, coverage in zip(
        ['click_to_ftd_calls', 'ftd_visits', 'call_coverage', 'visit_coverage', 'total_coverage'],
        ['click_to_ftd_calls%', 'ftd_visits%', 'call_coverage%', 'visit_coverage%', 'total_coverage%']
    ):
        total_row[coverage] = (np.round((total_row[attempt] / total_row['ApplicationNo']) * 100)).astype(float).round(0)

    # Convert all numeric columns to integers except intensity columns
    for col in total_row.index:
        if col not in ['call_intensity', 'visit_intensity', 'total_intensity', 'officer_name']:
            total_row[col] = int(total_row[col])

    # Append the total row to the DataFrame
    grouped_bucket_zone = pd.concat([grouped_bucket_zone, total_row.to_frame().T], ignore_index=True)

    return grouped_bucket_zone

def pivot_disposition(unpaid_and_field_df):
    # Filter the dataframe for 'opening_bucket' == 0
    unpaid_and_field_df = unpaid_and_field_df[unpaid_and_field_df['opening_bucket'] == 0]

    # Group by 'disposition' and 'Zone', then count occurrences
    grouped = unpaid_and_field_df.groupby(['disposition', 'Zone']).size().unstack(fill_value=0)
    zone_totals = grouped.sum(axis=0)
    percentage_grouped = (grouped.div(zone_totals, axis=1) * 100)

    # Adjust percentages to ensure they sum to 100 for each Zone
    for zone in percentage_grouped.columns:
        diff = 100 - percentage_grouped[zone].sum()
        if diff != 0:
            # Add the difference to the largest value to ensure the total is 100
            max_index = percentage_grouped[zone].idxmax()
            percentage_grouped.at[max_index, zone] += diff

    # Reset the index to make 'disposition' a column
    percentage_grouped.reset_index(inplace=True)

    # Add a "Total" row for the zone percentages
    total_row = percentage_grouped.sum(numeric_only=True)
    total_row['disposition'] = 'Total'
    percentage_grouped = pd.concat([percentage_grouped, total_row.to_frame().T], ignore_index=True)
    total_count = unpaid_and_field_df['ApplicationNo'].nunique()
    disposition_count = unpaid_and_field_df.groupby('disposition').size()

    # Vectorized Total calculation
    percentage_grouped['Total'] = (percentage_grouped['disposition'].map(disposition_count) / total_count) * 100
    
    percentage_grouped.loc[percentage_grouped['disposition'] == 'Total', 'Total'] = 100.0
    # Round the values in all specified columns to 2 decimal places
    percentage_grouped[["East", "North 1", "North 2", "South", "West 1", "West 2", "Total"]] = \
        percentage_grouped[["East", "North 1", "North 2", "South", "West 1", "West 2", "Total"]].astype(float).round(2)

    return percentage_grouped



call_intensity_0_1 = unpaid_and_field_df[unpaid_and_field_df['call_intensity'].isin([0,1])].copy()
visit_intensity_0_1 = unpaid_and_field_df[unpaid_and_field_df['visit_intensity'].isin([0,1])].copy()

# Column mapping and sheet mapping remain the same

column_mapping = {
    "Zone": "Zone",
    "opening_bucket": "Opening Bucket",
    "ApplicationNo": "Allocated Accounts",
    "click_to_ftd_calls": "FTD Click to Call",
    "total_ftd_calls": "FTD Total Call",
    "total_ftd_calls%": "FTD Total Calls%",
    "ftd_visits": "FTD Visit",
    "ftd_visits%": "FTD Visit%",
    "call_coverage": "Unique Call Attempt",
    "visit_coverage": "Unique Visit Attempt",
    "total_coverage": "Unique Total Attempt",
    "call_intensity": "Call Intensity",
    "visit_intensity": "Visit Intensity",
    "total_intensity": "Total Intensity",
    "call_coverage%": "Call Coverage%",
    "visit_coverage%": "Visit Coverage %",
    "total_coverage%": "Total Coverage%",
    "call_coverage_1" : "Call Coverage",
    "visit_coverage_1" : "Visit Coverage",
    "officer_name" : "Officer Name",
    "disposition" : "Disposition",
    "status" : "Status",
    "customer_name": "Customer Name",
    "officer_mobile": "Officer Mobile",
    "officer_emp_id": "Officer Emp Id",
    "cluster": "Cluster",
    "risk_segment_1": "Risk Segmentation 1",
    "risk_segment_2": "Risk Segmentation 2",
    "allocation_logic": "Allocation Logic",
    "allocation_status": "Allocation Status",
    "field_allocation": "Field Allocation",
    "manager_name": "Manager Name",
    "ptp_date": "Ptp Date",
    "ptp_type": "Ptp Type",
    "dialer_coverage": "Dialer Coverage",
    "dialer_contact": "Dialer Contact",
    "dialer_intensity": "Dialer Intensity",
    "East": "East %",
    "North 1": "North 1 %",
    "North 2": "North 2 %",
    "South": "South %",
    "West 1": "West 1 %",
    "West 2": "West 2 %",
    "Total": "Total %"
}

sheet_map = {
    'pivot_bucket_and_zone_lvl': 'Bucket and Zone Level',
    'pivot_zone_lvl': 'Zone Level',
    'pivot_zone_and_bucket_lvl': 'Zone and Bucket Level',
    'pivot_zone_and_x_lvl': 'Stress Pool X',
    'pivot_zone_and_30_lvl': 'Stress Pool 30',
    'pivot_state_and_bucket_lvl': 'State Level',
    'pivot_call_intensity_and_zone_lvl': 'Call Intensity Lvl Zero & One',
    'pivot_visit_intensity_and_zone_lvl': 'Visit Intensity Lvl Zero & One',
    'pivot_officer_lvl': 'Allocated Officer Level',
    'pivot_disposition': 'Disposition Report',
    'working_df' : 'Jan-24 Data',
    'call_intensity_0_1': 'Call Intensity Lvl 0 & 1',
    'visit_intensity_0_1': 'Visit Intensity Lvl 0 & 1',
}

def rename_columns(df):
    """
    Renames the columns of a DataFrame based on the provided column mapping.
    Only renames columns that are present in the DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame to rename columns in.

    Returns:
    pd.DataFrame: The DataFrame with renamed columns.
    """
    present_columns = {col: column_mapping[col] for col in column_mapping if col in df.columns}
    return df.rename(columns=present_columns)

def apply_formatting(worksheet, df, formatting_type="both"):
    """
    Applies formatting to the worksheet based on the specified formatting type.

    Parameters:
    worksheet (xlsxwriter.worksheet.Worksheet): The worksheet to apply formatting to.
    df (pd.DataFrame): The DataFrame to determine which columns to format.
    formatting_type (str): The type of formatting to apply. Options are "basic", "conditional", or "both".
    """
    # Define styles
    header_format = workbook.add_format({
        'bold': True,
        'font_name': 'Calibri',
        'font_size': 10,
        'bg_color': '#d9e1f2',
        'align': 'center',
        'valign': 'vcenter',
        'border': 1
    })

    data_format = workbook.add_format({
        'font_name': 'Calibri',
        'font_size': 10,
        'align': 'center',
        'valign': 'vcenter',
        'border': 1
    })

    total_format = workbook.add_format({
        'bold': True,
        'font_name': 'Calibri',
        'font_size': 10,
        'bg_color': '#d9e1f2',
        'align': 'center',
        'valign': 'vcenter',
        'border': 1
    })

    # Replace NaN values with "-"
    df = df.infer_objects()
    df = df.fillna("-")

    # Track column widths for auto-fit
    col_widths = {}

    # Apply basic formatting
    if formatting_type in ["basic", "both"]:
        # Write header
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
            # Update column width for header
            col_widths[col_num] = max(col_widths.get(col_num, 0), len(str(value)))

        # Write data
        for row_num, row_data in enumerate(df.itertuples(index=False), start=1):
            for col_num, value in enumerate(row_data):
                worksheet.write(row_num, col_num, value, data_format)
                # Update column width for data
                col_widths[col_num] = max(col_widths.get(col_num, 0), len(str(value)))

        # Apply total row formatting
        if 'Total' in df.columns:
            total_row = df.columns.get_loc('Total')
            for col_num in range(df.shape[1]):
                worksheet.write(total_row, col_num, df.iloc[total_row, col_num], total_format)
                # Update column width for total row
                col_widths[col_num] = max(col_widths.get(col_num, 0), len(str(df.iloc[total_row, col_num])))

    # Apply conditional formatting
    if formatting_type in ["conditional", "both"]:
        columns_to_format = ['FTD Total Calls%', 'FTD Visit%', 'Call Intensity', 'Visit Intensity', 'Call Coverage%',
                            'Visit Coverage%', "East %", "North 1 %", "North 2 %", "South %", "West 1 %", "West 2 %", "Total %"]

        existing_columns = [col for col in columns_to_format if col in df.columns]

        for col_name in existing_columns:
            col_index = df.columns.get_loc(col_name)
            worksheet.conditional_format(1, col_index, df.shape[0], col_index, {
                'type': '3_color_scale',
                'min_color': '#f8696b',
                'mid_color': '#ffeb84',
                'max_color': '#63be7b'
            })

    # Auto-fit column widths
    for col_num, width in col_widths.items():
        worksheet.set_column(col_num, col_num, width + 1)  # Add padding

def export_to_excel(file_path, sheet_map, dataframes):
    """
    Exports multiple DataFrames to an Excel file with specified sheet names.

    Parameters:
    file_path (str): The file path to save the Excel file.
    sheet_map (dict): A dictionary mapping DataFrame names to sheet names.
    dataframes (dict): A dictionary of DataFrames to export.
    """
    global workbook
    workbook = xlsxwriter.Workbook(file_path)

    for df_name, df in dataframes.items():
        sheet_name = sheet_map.get(df_name, df_name)
        worksheet = workbook.add_worksheet(sheet_name)
        apply_formatting(worksheet, df, formatting_type="both")

    workbook.close()


# Assuming unpaid_and_field_df and working_df are already defined
dataframes = {
    'pivot_bucket_and_zone_lvl': pivot_bucket_and_zone_lvl(unpaid_and_field_df),
    'pivot_zone_lvl': pivot_zone_lvl(unpaid_and_field_df),
    'pivot_zone_and_bucket_lvl': pivot_zone_and_bucket_lvl(unpaid_and_field_df),
    'pivot_state_and_bucket_lvl': pivot_state_and_bucket_lvl(unpaid_and_field_df),
    'pivot_call_intensity_and_zone_lvl': pivot_call_intensity_and_zone_lvl(unpaid_and_field_df),
    'pivot_visit_intensity_and_zone_lvl': pivot_visit_intensity_and_zone_lvl(unpaid_and_field_df),
    'pivot_officer_lvl': pivot_officer_lvl(unpaid_and_field_df),
    'pivot_disposition': pivot_disposition(unpaid_and_field_df),
    'working_df' : working_df,
}

for df_name in dataframes:
    dataframes[df_name] = rename_columns(dataframes[df_name])

export_to_excel(export_file_path, sheet_map, dataframes)

def simple_export_to_excel(file_path, df):
    """
    Exports a DataFrame to an Excel file and applies basic formatting using xlsxwriter.

    Parameters:
    file_path (str): The file path to save the Excel file.
    df (pd.DataFrame): The DataFrame to export.
    """
    # Step 1: Rename the columns based on the column mapping
    df = rename_columns(df)
    df = df.fillna("-")

    # Step 2: Create a new workbook and add a worksheet
    workbook = xlsxwriter.Workbook(file_path)
    worksheet = workbook.add_worksheet("Sheet1")  # Adjust the name if needed

    # Step 3: Define the header and data formats
    header_format = workbook.add_format({
        'bold': True,
        'font_name': 'Calibri',
        'font_size': 10,
        'bg_color': '#d9e1f2',
        'align': 'center',
        'valign': 'vcenter',
        'border': 1
    })

    data_format = workbook.add_format({
        'font_name': 'Calibri',
        'font_size': 10,
        'align': 'center',
        'valign': 'vcenter',
        'border': 1
    })

    # Step 4: Write the DataFrame data to the worksheet
    # Write header
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, value, header_format)  # Write the header row

    # Write data
    for row_num, row_data in enumerate(df.itertuples(index=False), start=1):
        for col_num, value in enumerate(row_data):
            worksheet.write(row_num, col_num, value, data_format)  # Write the data rows

    # Step 5: Apply formatting (auto-fit columns, etc.)
    # Auto-size columns based on the content width
    col_widths = {}
    for col_num, value in enumerate(df.columns.values):
        col_widths[col_num] = len(str(value))

    for row_num, row_data in enumerate(df.itertuples(index=False), start=1):
        for col_num, value in enumerate(row_data):
            col_widths[col_num] = max(col_widths.get(col_num, 0), len(str(value)))

    for col_num, width in col_widths.items():
        worksheet.set_column(col_num, col_num, width + 2)  # Adding some padding

    # Step 6: Save the workbook
    workbook.close()

simple_export_to_excel(fr"C:\Users\Arc\Downloads\OneDrive\OneDrive - zp3bp\4. Working Files\8. All Lender\1. Call & Visit MIS\Zero and One Call Intensity {today.strftime('%b-%Y')}.xlsx", call_intensity_0_1 )
simple_export_to_excel(fr"C:\Users\Arc\Downloads\OneDrive\OneDrive - zp3bp\4. Working Files\8. All Lender\1. Call & Visit MIS\Zero and One Visit Intensity {today.strftime('%b-%Y')}.xlsx", visit_intensity_0_1)


def formatting_to_html(html_table):
    """
    Applies basic formatting and conditional formatting to the HTML table.

    Parameters:
    html_table (str): The HTML table as a string.

    Returns:
    str: The formatted HTML table as a string.
    """
    # Parse the HTML table
    soup = BeautifulSoup(html_table, 'html.parser')

    # Define CSS styles
    border_style = "border: 1px solid black;"
    bold_font_style = "font-weight: bold; font-family: Calibri, sans-serif; font-size: 12pt;"
    normal_font_style = "font-family: Calibri, sans-serif; font-size: 12pt;"
    bg_color_style = "background-color: #d9e1f2;"
    center_alignment_style = "text-align: center; vertical-align: middle;"
    no_wrap_style = "white-space: nowrap;"
    padding_style = "padding: 2px 6px;"  # Adjust padding as needed

    # Apply border-collapse to the table
    table = soup.find('table')
    table['style'] = "border-collapse: collapse;"

    # Apply border, bold header, background color, font settings, center alignment, no-wrap, and padding
    for row in soup.find_all('tr'):
        for cell in row.find_all(['td', 'th']):
            cell['style'] = f"{border_style} {normal_font_style} {center_alignment_style} {no_wrap_style} {padding_style}"
            if row == soup.find_all('tr')[0]:  # Header row
                cell['style'] += f" {bold_font_style} {bg_color_style}"
            if any("total" in str(c.text).lower() for c in row.find_all(['td', 'th'])):
                cell['style'] += f" {bold_font_style} {bg_color_style}"

    # Add conditional formatting
    columns_to_format = ['FTD Total Calls%', 'FTD Visit%', 'Call Intensity', 'Visit Intensity', 'Call Coverage%',
                        'Total Coverage%', "East %", "North 1 %", "North 2 %", "South %", "West 1 %", "West 2 %", "Total %"]

    # Get the header row to find the column indices
    header_row = soup.find_all('tr')[0]
    header_cells = header_row.find_all(['th', 'td'])

    # Map column names to their indices
    col_name_to_index = {cell.text.strip(): idx for idx, cell in enumerate(header_cells)}

    # Define the color gradient
    colors = ['#f8696b', '#ffeb82', '#63be7b']  # Red, Yellow, Green
    cmap = mcolors.LinearSegmentedColormap.from_list('custom', colors)

    # Apply conditional formatting based on the column names
    for col_name in columns_to_format:
        if col_name in col_name_to_index:
            col_index = col_name_to_index[col_name]
            # Collect all values in the column to determine the min and max
            values = []
            for row in soup.find_all('tr')[1:-1]:  # Skip header and bottom row
                cell = row.find_all(['td', 'th'])[col_index]
                try:
                    value = float(cell.text.strip('%'))
                    values.append(value)
                except ValueError:
                    pass

            min_value = min(values)
            max_value = max(values)

            for row in soup.find_all('tr')[1:-1]:  # Skip header and bottom row
                cell = row.find_all(['td', 'th'])[col_index]
                try:
                    value = float(cell.text.strip('%'))
                    # Normalize the value to the range [0, 1]
                    normalized_value = (value - min_value) / (max_value - min_value)
                    # Get the color from the colormap
                    color = cmap(normalized_value)
                    # Convert the color to a hex string
                    hex_color = mcolors.rgb2hex(color)
                    cell['style'] += f" background-color: {hex_color};"
                except ValueError:
                    pass

    # Return the formatted HTML table as a string
    return str(soup)

formatted_html = formatting_to_html(rename_columns(pivot_zone_lvl(unpaid_and_field_df)).to_html(index=False))
formatted_html_2 = formatting_to_html(rename_columns(pivot_zone_and_bucket_lvl(unpaid_and_field_df)).to_html(index=False))


# Sample email body template
body_template = """
<html>
<body>
    <div>Hi All,</div><br>
    <div>Kindly find the detailed summary of the coverage reports below:</div> <br>
    <h3><b>Zone Level:</b></h3>
    <br>
    {pivot_html}
    <br>
    <h3><b>Zone and Bucket Level:</b></h3>
    <br>
    {pivot_html_2}
    <br>
    <div>Thanks & Regards,</div>
    <div style="color: #01437c;">Collection Ops</div>
    <div><strong>Finnable</strong></div>
</body>
</html>
"""

# Email configuration
email_address = "collections.mis@finnable.com"
smtp_server = "smtppro.zoho.com"
smtp_port = 465
password = 'Finnable@123'

# Example usage
mail_dict = {
    'Collection Coverage': {
        'to': ['hocollections@finnable.com', 'collections@finnable.com'],
        'cc': ['balaji.ashwin@finnable.com', 'sandeep.satsangi@finnable.com', 'saurabh.agrawal@finnable.com', 'sohaib.ansari@finnable.com', 'mohdimran.ali@finnable.com', 'naresh.sharma@finnable.com',
            'nitin.gupta@finnable.com', 'vaibhav.bhardwaj@finnable.com', 'anupam.vyas@finnable.com', 'khushboo.gupta@finnable.com', 'shivam.ashiwal@finnable.com'],
        'name': 'All',
        'pivot_html': formatted_html,
        'attachment_paths': [
            export_file_path,
            fr"C:\Users\Arc\Downloads\OneDrive\OneDrive - zp3bp\4. Working Files\8. All Lender\1. Call & Visit MIS\Zero and One Call Intensity {today.strftime('%b-%Y')}.xlsx",
            fr"C:\Users\Arc\Downloads\OneDrive\OneDrive - zp3bp\4. Working Files\8. All Lender\1. Call & Visit MIS\Zero and One Visit Intensity {today.strftime('%b-%Y')}.xlsx"
        ],
        'pivot_html_2': formatted_html_2
    },
}

universal_cc = ['']

def get_password():
    while True:
        password = 'Finnable@123'
        if password.lower() == 'exit':
            print("Exiting the program.")
            sys.exit()  # Exit the program if the user types 'exit'
        return password  # Return the entered password if not 'exit'
    
    

def send_email(partner_name, to, cc, name, pivot_html, attachment_paths, pivot_html_2):
    global password  # Access the global password variable

    # Prepare email body with dynamic content for name and pivot_html
    email_body = body_template.format(name=name, pivot_html=pivot_html, pivot_html_2=pivot_html_2)

    # Prepare the subject with partner name and current date
    subject = f"{partner_name} Report {datetime.now().date()}"

    # Create message container
    msg = MIMEMultipart()
    msg['From'] = email_address
    msg['To'] = ", ".join(to)
    msg['CC'] = ", ".join(cc + universal_cc)  # Add universal CC
    msg['Subject'] = subject

    # Attach the HTML body
    msg.attach(MIMEText(email_body, 'html'))

    # Attach the files if paths exist
    for attachment_path in attachment_paths:
        if attachment_path and os.path.exists(attachment_path):
            with open(attachment_path, 'rb') as attachment_file:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment_file.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(attachment_path)}')
                msg.attach(part)

    # Loop to handle login retries on authentication error
    while True:
        try:
            print(f"Attempting to connect to SMTP server for {partner_name}...")
            with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
                if password is None:  # If password is not set, prompt for it
                    password = get_password()  # Get the password with exit option

                print("Connected. Attempting to log in...")
                server.login(email_address, password)
                print("Logged in. Sending the email...")

                server.sendmail(email_address, to + cc + universal_cc, msg.as_string())  # Send to, cc, and universal cc
                print(f"Email sent to {name} ({', '.join(to)})")
                break  # Exit after sending the email successfully

        except SMTPAuthenticationError:
            print("Error: Incorrect password. Please try again.")
            password = get_password()  # Prompt for the password again
            continue  # Retry the connection
        except SMTPConnectError:
            print("Error: Could not connect to the SMTP server. Please check your internet connection and SMTP server address.")
            break  # Exit after this error
        except socket.gaierror:
            print("Error: Network issue. Unable to resolve the SMTP server address.")
            break  # Exit after this error
        except SMTPRecipientsRefused:
            print("Error: One or more recipient's email address was refused. Please verify the recipient's address.")
            break  # Exit after this error
        except SMTPServerDisconnected as e:
            print(f"Error: SMTP server disconnected unexpectedly: {e}")
            break  # Exit after this error
        except KeyboardInterrupt:
            print("\nProcess interrupted by user. Exiting...")
            sys.exit()  # Gracefully exit if the user presses Ctrl+C
        except SMTPException as e:
            print(f"SMTP Error: {e}")
            break  # Exit after this error
        except Exception as e:  # Catch any other unexpected exceptions
            print(f"An unexpected error occurred: {e}")
            break  # Exit after this error

# Loop through mail_dict and send emails
for partner, details in mail_dict.items():
    send_email(
        partner_name=partner,  # Use the partner's name (HDB, DMI, Piramal)
        to=details['to'],
        cc=details['cc'],
        name=details['name'],
        pivot_html=details['pivot_html'],
        attachment_paths=details['attachment_paths'],
        pivot_html_2=details['pivot_html_2']
    )

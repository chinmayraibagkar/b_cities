import streamlit as st
import pandas as pd
import numpy as np
from google.ads.googleads.client import GoogleAdsClient
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.credentials import Credentials
import tempfile
import json
import chardet

def get_google_ads_client():
    # Get credentials from Streamlit secrets
    credentials = {
        'developer_token': st.secrets['google_ads']['developer_token'],
        'client_id': st.secrets['google_ads']['client_id'],
        'client_secret': st.secrets['google_ads']['client_secret'],
        'refresh_token': st.secrets['google_ads']['refresh_token'],
        'login_customer_id': st.secrets['google_ads']['login_customer_id'],
        'use_proto_plus': st.secrets['google_ads']['use_proto_plus'],
    }
    
    # Create a temporary yaml file
    with open('temp_credentials.yaml', 'w') as f:
        import yaml
        yaml.dump(credentials, f)
    
    # Load client from temporary file
    client = GoogleAdsClient.load_from_storage('temp_credentials.yaml')
    
    # Remove temporary file
    import os
    os.remove('temp_credentials.yaml')
    
    return client


# Get dashboard data
def get_kw_data(client, customer_id, start_date, end_date):
    ga_service = client.get_service("GoogleAdsService", version="v17")

    # Constructing the query
    query = f"""
    SELECT
        campaign.name,
        campaign.id,
        segments.date,
        metrics.cost_micros
    FROM
        campaign
    WHERE
        segments.date BETWEEN '{start_date}' AND '{end_date}'
    """

    response = ga_service.search_stream(customer_id=customer_id, query=query)
    
    data = []
    for batch in response:
        for row in batch.results:
            data.append({
                "Date": row.segments.date if hasattr(row.segments, 'date') else 'NA',
                "Campaign Name": row.campaign.name if hasattr(row.campaign, 'name') else 'NA',
                "Campaign ID": row.campaign.id if hasattr(row.campaign, 'id') else 'NA',
                "Cost": row.metrics.cost_micros / 1e6 if hasattr(row.metrics, 'cost_micros') else 0,
            })
    
    return pd.DataFrame(data)


def update_google_sheet(dataframe, sheet_id, worksheet_title, d_range):
    try:
        # Get credentials from Streamlit secrets
        credentials = {
            "type": st.secrets["gcp_service_account"]["type"],
            "project_id": st.secrets["gcp_service_account"]["project_id"],
            "private_key_id": st.secrets["gcp_service_account"]["private_key_id"],
            "private_key": st.secrets["gcp_service_account"]["private_key"],
            "client_email": st.secrets["gcp_service_account"]["client_email"],
            "client_id": st.secrets["gcp_service_account"]["client_id"],
            "auth_uri": st.secrets["gcp_service_account"]["auth_uri"],
            "token_uri": st.secrets["gcp_service_account"]["token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_service_account"]["auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_service_account"]["client_x509_cert_url"],
            "universe_domain": st.secrets["gcp_service_account"]["universe_domain"]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp:
            json.dump(credentials, temp)
            temp_path = temp.name

        # Use the temporary file for authentication
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        from oauth2client.service_account import ServiceAccountCredentials
        creds = ServiceAccountCredentials.from_json_keyfile_name(temp_path, scope)
        client = gspread.authorize(creds)
        st.success("Google Sheets credentials loaded successfully.")
        
        # Logging the sheet ID and worksheet title
        st.write(f"Attempting to open Google Sheet with ID: {sheet_id} and Worksheet: {worksheet_title}")

        # Open the Google Sheet using its ID
        sheet = client.open_by_key(sheet_id)

        # Open the specific worksheet by title
        worksheet = sheet.worksheet(worksheet_title)
        st.success(f"Opened the worksheet: {worksheet_title}")

        # Handle NaN values in the DataFrame
        dataframe = dataframe.fillna('')  # Replace NaN with empty string

        # Convert DataFrame to list of lists
        data = [dataframe.columns.values.tolist()] + dataframe.values.tolist()

        # Clear specific range (e.g., A1:Z1000)
        worksheet.batch_clear(d_range)

        # Update worksheet with new data
        worksheet.update("A1", data)
        st.success("Worksheet updated successfully.")
    except gspread.SpreadsheetNotFound:
        st.error(f"Spreadsheet not found with ID: {sheet_id}. Please check the sheet ID and service account permissions.")
    except gspread.WorksheetNotFound:
        st.error(f"Worksheet not found with title: {worksheet_title}. Please check the worksheet title.")
    except gspread.GSpreadException as e:
        st.error(f"GSpreadException: {e}")
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")


def get_google_sheet_data(sheet_id, worksheet_title):
    try:
        # Get credentials from Streamlit secrets
        credentials = {
            "type": st.secrets["gcp_service_account"]["type"],
            "project_id": st.secrets["gcp_service_account"]["project_id"],
            "private_key_id": st.secrets["gcp_service_account"]["private_key_id"],
            "private_key": st.secrets["gcp_service_account"]["private_key"],
            "client_email": st.secrets["gcp_service_account"]["client_email"],
            "client_id": st.secrets["gcp_service_account"]["client_id"],
            "auth_uri": st.secrets["gcp_service_account"]["auth_uri"],
            "token_uri": st.secrets["gcp_service_account"]["token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_service_account"]["auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_service_account"]["client_x509_cert_url"],
            "universe_domain": st.secrets["gcp_service_account"]["universe_domain"]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp:
            json.dump(credentials, temp)
            temp_path = temp.name

        # Use the temporary file for authentication
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        from oauth2client.service_account import ServiceAccountCredentials
        creds = ServiceAccountCredentials.from_json_keyfile_name(temp_path, scope)
        client = gspread.authorize(creds)
        st.success("Google Sheets credentials loaded successfully.")
        
        # Open the Google Sheet using its ID
        sheet = client.open_by_key(sheet_id)
        
        # Open the specific worksheet by title
        worksheet = sheet.worksheet(worksheet_title)
        st.success(f"Opened the worksheet: {worksheet_title}")

        # Get all values from worksheet
        data = worksheet.get_all_values()
        
        # Convert to DataFrame
        df = pd.DataFrame(data[1:], columns=data[0])
        return df

    except gspread.SpreadsheetNotFound:
        st.error(f"Spreadsheet not found with ID: {sheet_id}")
        return None
    except gspread.WorksheetNotFound:
        st.error(f"Worksheet not found with title: {worksheet_title}")
        return None
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return None


geo_city_mapping_ref = {
        1: "Mumbai",
        2: "Delhi",
        3: "Bangalore",
        4: "Hyderabad",
        5: "Chennai",
        6: "Ahmedabad",
        7: "Jaipur",
        8: "Pune",
        9: "Kolkata",
        10: "Surat",
        11: "Lucknow",
        12: "Coimbatore",
        13: "Indore",
        14: "Nagpur",
        15: "Chandigarh",
        16: "Vadodara",
        17: "Ludhiana",
        18: "Kochi",
        19: "Nashik",
        20: "Kanpur",
        29: "Vizag",
        30: "Trivandrum",
        None: "Others"
    }

client = get_google_ads_client()
st.title("Bottom Cities Analysis Tool")
date_range = st.date_input("Select Date Range", [pd.to_datetime("2023-12-01"), pd.to_datetime("2025-01-31")])
st.session_state.start_date = date_range[0].strftime("%Y-%m-%d")
st.session_state.end_date = date_range[1].strftime("%Y-%m-%d")

st.session_state.campaign_data = get_kw_data(client,"9680382253", st.session_state.start_date, st.session_state.end_date)
st.session_state.campaign_data = pd.concat([st.session_state.campaign_data, get_kw_data(client,"4840834180", st.session_state.start_date, st.session_state.end_date)], ignore_index=True)

geo_acq_mapped = st.session_state.campaign_data.copy()

#map the data with mapping_ref
mapping_ref = get_google_sheet_data("1RsFcJ9NSFJTggG95zOOU9-kNQJQWwbQ8p6dc2-Ns78g", "Mapping_ref")
mapping_ref["Campaign ID"] = mapping_ref["Campaign ID"].astype(str)
st.session_state.campaign_data["Campaign Name"] = st.session_state.campaign_data["Campaign Name"].str.strip()
st.session_state.campaign_data = st.session_state.campaign_data.merge(mapping_ref, how="inner", left_on="Campaign Name", right_on="Campaign")
st.session_state.campaign_data = st.session_state.campaign_data[["Date","Campaign ID_y" ,"Campaign Name", "City", "Category", "Cost"]]
st.dataframe(st.session_state.campaign_data)

metacard_2w_spot = st.file_uploader("Upload a 2W and Spot Metacard CSV", type=["csv"])
metacard_uac = st.file_uploader("Upload a UAC Metacard CSV", type=["csv"])
metacard_uace = st.file_uploader("Upload a UACE Metacard CSV", type=["csv"])

with open(metacard_2w_spot, 'rb') as f:
    result = chardet.detect(f.read())

if metacard_2w_spot and metacard_uac and metacard_uace is not None:
    st.session_state.metacard_2w_spot = pd.read_csv(metacard_2w_spot, encoding=result['encoding'])
    st.session_state.metacard_uac = pd.read_csv(metacard_uac)
    st.session_state.metacard_uace = pd.read_csv(metacard_uace)

    # Clean and prepare data for merging
    st.session_state.metacard_2w_spot["UTM_CAMPAIGN"] = st.session_state.metacard_2w_spot["UTM_CAMPAIGN"].astype(str).str.strip()
    st.session_state.metacard_2w_spot["LEAD_DATE"] = st.session_state.metacard_2w_spot["LEAD_DATE"].str.strip()

    #keep only those rows where CUSTOMER = 1
    st.session_state.metacard_2w_spot = st.session_state.metacard_2w_spot[st.session_state.metacard_2w_spot["CUSTOMER"] == 1]

    st.session_state.campaign_data["Campaign ID_y"] = st.session_state.campaign_data["Campaign ID_y"].astype(str).str.strip()
    st.session_state.campaign_data["Date"] = st.session_state.campaign_data["Date"].astype(str).str.strip()
    st.session_state.campaign_data["Date"] = pd.to_datetime(st.session_state.campaign_data["Date"]).dt.strftime("%d-%m-%Y")

    st.session_state.metacard_2w_spot["LEAD_DATE"] = pd.to_datetime(st.session_state.metacard_2w_spot["LEAD_DATE"], format="%d-%m-%Y").dt.strftime("%d-%m-%Y")
    st.session_state.metacard_2w_spot["key"] = st.session_state.metacard_2w_spot["UTM_CAMPAIGN"] + "_" + st.session_state.metacard_2w_spot["LEAD_DATE"]

    st.session_state.metacard_2w_spot["SME"] = np.where(st.session_state.metacard_2w_spot["FREQUENCY_ENUM"] == 4, 1, 0)
    st.session_state.metacard_2w_spot["Retail"] = np.where((st.session_state.metacard_2w_spot["FREQUENCY_ENUM"] == 5) | (st.session_state.metacard_2w_spot["FREQUENCY_ENUM"] == 6), 1, 0)
    st.session_state.metacard_2w_spot["SME_2W"] = np.where((st.session_state.metacard_2w_spot["FREQUENCY_ENUM"] == 4) & (st.session_state.metacard_2w_spot["FIRST_CATEGORY"] == "2w"), 1, 0)
    st.session_state.metacard_2w_spot["SME_Trucks"] = np.where((st.session_state.metacard_2w_spot["FREQUENCY_ENUM"] == 4) & ((st.session_state.metacard_2w_spot["FIRST_CATEGORY"] == "LCV")|(st.session_state.metacard_2w_spot["FIRST_CATEGORY"] == "HCV")), 1, 0)
    
    # map geo_city_mapping_ref
    st.session_state.metacard_2w_spot["City"] = st.session_state.metacard_2w_spot["REG_GEO_ID"].map(geo_city_mapping_ref)

    geo_acq_2w_spot = st.session_state.metacard_2w_spot.groupby(["LEAD_DATE", "CAMPAIGN_NAME", "City"]).agg({
        "CUSTOMER": "sum",
        "ACQ_2W": "sum",
        "ACQ_TRUCKS": "sum",
        "ACQ_HCV": "sum",
        "ACQ_LCV": "sum",
        "PNM_CONV": "sum",
        "SME": "sum",
        "Retail": "sum",
        "SME_2W": "sum",
        "SME_Trucks": "sum"
    }).reset_index()
    
    # make this sequence : LEAD_DATE	City	CAMPAIGN_NAME	CUSTOMER	ACQ_2W	ACQ_TRUCKS	ACQ_HCV	ACQ_LCV	PNM_CONV	SME	Retail	SME_2W	SME_Trucks
    geo_acq_2w_spot = geo_acq_2w_spot[["LEAD_DATE", "City", "CAMPAIGN_NAME", "CUSTOMER", "ACQ_2W", "ACQ_TRUCKS", "ACQ_HCV", "ACQ_LCV", "PNM_CONV", "SME", "Retail", "SME_2W", "SME_Trucks"]]
    st.dataframe(geo_acq_2w_spot)
    
    # fill NA with 0
    st.session_state.metacard_2w_spot = st.session_state.metacard_2w_spot.fillna(0)

    # Group metacard by key
    st.session_state.metacard_2w_spot = st.session_state.metacard_2w_spot.groupby("key").agg({
        "ACQ_2W": "sum", 
        "ACQ_TRUCKS": "sum", 
        "ACQ_HCV": "sum", 
        "ACQ_LCV": "sum", 
        "PNM_CONV": "sum",
        "SME": "sum",
        "Retail": "sum",
        "SME_2W": "sum",
        "SME_Trucks": "sum"
    }).reset_index()

    st.session_state.campaign_data["key"] = st.session_state.campaign_data["Campaign ID_y"] + "_" + st.session_state.campaign_data["Date"]

    # Merge the datasets
    st.session_state.campaign_data = st.session_state.campaign_data.merge(st.session_state.metacard_2w_spot, how="left", on="key")

    ## Engagement Campaign Data processing

    # create vehicle mapping ref
    vehicle_mapping_ref = {
    97:"2W",
    126:"LCV",
    1:"LCV",
    9:"HCV",
    91:"LCV",
    128:"HCV",
    2:"HCV",
    10:"HCV",
    7:"LCV",
    3:"HCV",
    110:"HCV",
    8:"HCV",
    14:"LCV",
    101:"LCV",
    104:"LCV",
    133:"0",
    109:"HCV",
    103:"LCV",
    111:"HCV",
    114:"HCV",
    106:"HCV",
    88:"LCV",
    107:"HCV",
    132:"0",
    105:"LCV",
    100:"HCV",
    108:"HCV",
    112:"HCV",
    0:"0"}

    # Map Vehicles according to vehicle_mapping_ref in uace metacard
    st.session_state.metacard_uace["Vehicle"] = st.session_state.metacard_uace["VEHICLE_ID"].map(vehicle_mapping_ref)

    # create columns 2W_acq, LCV_acq, HCV_acq, Trucks_acq, Total_acq
    # Where vehicle is 2W, LCV, HCV, make the value of ACQ as 1, else 0
    st.session_state.metacard_uace["2W_UACe"] = np.where(st.session_state.metacard_uace["Vehicle"] == "2W", 1, 0)
    st.session_state.metacard_uace["LCV_UACe"] = np.where(st.session_state.metacard_uace["Vehicle"] == "LCV", 1, 0)
    st.session_state.metacard_uace["HCV_UACe"] = np.where(st.session_state.metacard_uace["Vehicle"] == "HCV", 1, 0)
    # fill na with 0
    st.session_state.metacard_uace = st.session_state.metacard_uace.fillna(0)

    st.session_state.metacard_uace["Trucks_UACe"] = st.session_state.metacard_uace["LCV_UACe"] + st.session_state.metacard_uace["HCV_UACe"]
    st.session_state.metacard_uace["Total_UACe"] = st.session_state.metacard_uace["2W_UACe"] + st.session_state.metacard_uace["Trucks_UACe"]
    st.session_state.metacard_uace["SME_UACe"] = np.where(st.session_state.metacard_uace["FREQ"] == 4, 1, 0)
    st.session_state.metacard_uace["Retail_UACe"] = np.where((st.session_state.metacard_uace["FREQ"] == 5) | (st.session_state.metacard_uace["FREQ"] == 6), 1, 0)
    st.session_state.metacard_uace["SME_2W_UACe"] = np.where((st.session_state.metacard_uace["FREQ"] == 4) & (st.session_state.metacard_uace["Vehicle"] == "2W"), 1, 0)
    st.session_state.metacard_uace["SME_Trucks_UACe"] = np.where((st.session_state.metacard_uace["FREQ"] == 4) & ((st.session_state.metacard_uace["Vehicle"] == "LCV")|(st.session_state.metacard_uace["Vehicle"] == "HCV")), 1, 0)

    # merge with geo_city_mapping_ref
    st.session_state.metacard_uace["City"] = st.session_state.metacard_uace["GEO_REGION_ID"].map(geo_city_mapping_ref)

    # extract campaign name
    st.session_state.metacard_uace["Campaign"] = st.session_state.metacard_uace["CAMPAIGN_NAME"].str.extract(r'^(.*?)(?=\()')

    # remove extra space at the end from campaign name
    st.session_state.metacard_uace["Campaign"] = st.session_state.metacard_uace["Campaign"].str.strip()

    geo_acq_uace = st.session_state.metacard_uace.groupby(["ORDER_DATE", "Campaign", "City"]).agg({
        "CUSTOMER_ID": "count",
        "2W_UACe": "sum",
        "LCV_UACe": "sum",
        "HCV_UACe": "sum",
        "Trucks_UACe": "sum",
        "Total_UACe": "sum",
        "SME_UACe": "sum",
        "Retail_UACe": "sum",
        "SME_2W_UACe": "sum",
        "SME_Trucks_UACe": "sum"
    }).reset_index()

    geo_acq_uace = geo_acq_uace[["ORDER_DATE", "City", "Campaign", "CUSTOMER_ID", "2W_UACe", "LCV_UACe", "HCV_UACe", "Trucks_UACe", "Total_UACe", "SME_UACe", "Retail_UACe", "SME_2W_UACe", "SME_Trucks_UACe"]]
    st.dataframe(geo_acq_uace)

    # #sort by Newest to oldest according to ORDER_DATE
    # st.session_state.metacard_uace = st.session_state.metacard_uace.sort_values(by="ORDER_DATE", ascending=False)

    # # remove newest duplicates and keep the last one
    # st.session_state.metacard_uace = st.session_state.metacard_uace.drop_duplicates(subset=["CUSTOMER_ID"], keep="last")
    st.session_state.metacard_uace["Campaign ID"] = st.session_state.metacard_uace["CAMPAIGN_NAME"].str.extract(r'\((\d+)\)$')
    

    # create key & Group data by key
    st.session_state.metacard_uace["ORDER_DATE"] = pd.to_datetime(st.session_state.metacard_uace["ORDER_DATE"], format="%d-%m-%Y")
    st.session_state.metacard_uace["key"] = st.session_state.metacard_uace["Campaign ID"] + "_" + st.session_state.metacard_uace["ORDER_DATE"].dt.strftime("%d-%m-%Y")
    st.session_state.metacard_uace = st.session_state.metacard_uace.groupby("key").agg({
        "2W_UACe": "sum", 
        "LCV_UACe": "sum", 
        "HCV_UACe": "sum", 
        "Trucks_UACe": "sum", 
        "Total_UACe": "sum",
        "SME_UACe": "sum",
        "Retail_UACe": "sum",
        "SME_2W_UACe": "sum",
        "SME_Trucks_UACe": "sum"
    }).reset_index()

    # Merge with campaign data
    st.session_state.campaign_data = st.session_state.campaign_data.merge(st.session_state.metacard_uace, how="left", on="key")
    
    # UAC Metacard Data processing

    # create columns 2W_acq, LCV_acq, HCV_acq, Trucks_acq, Total_acq in uac metacard
    # Where VEHICLE_TYPE is 2W, LCV, HCV, make the value of ACQ as 1, else 0
    st.session_state.metacard_uac["2W_UAC"] = np.where(st.session_state.metacard_uac["VEHICLE_TYPE"] == "2W", 1, 0)
    st.session_state.metacard_uac["LCV_UAC"] = np.where(st.session_state.metacard_uac["VEHICLE_TYPE"] == "LCV", 1, 0)
    st.session_state.metacard_uac["HCV_UAC"] = np.where(st.session_state.metacard_uac["VEHICLE_TYPE"] == "HCV", 1, 0)
    
    #fill na with 0
    st.session_state.metacard_uac = st.session_state.metacard_uac.fillna(0)

    st.session_state.metacard_uac["Trucks_UAC"] = st.session_state.metacard_uac["LCV_UAC"] + st.session_state.metacard_uac["HCV_UAC"]
    st.session_state.metacard_uac["Total_UAC"] = st.session_state.metacard_uac["2W_UAC"] + st.session_state.metacard_uac["Trucks_UAC"]
    st.session_state.metacard_uac["SME_UAC"] = np.where(st.session_state.metacard_uac["FREQ"] == 4, 1, 0)
    st.session_state.metacard_uac["Retail_UAC"] = np.where((st.session_state.metacard_uac["FREQ"] == 5) | (st.session_state.metacard_uac["FREQ"] == 6), 1, 0)
    st.session_state.metacard_uac["SME_2W_UAC"] = np.where((st.session_state.metacard_uac["FREQ"] == 4) & (st.session_state.metacard_uac["VEHICLE_TYPE"] == "2W"), 1, 0)
    st.session_state.metacard_uac["SME_Trucks_UAC"] = np.where((st.session_state.metacard_uac["FREQ"] == 4) & ((st.session_state.metacard_uac["VEHICLE_TYPE"] == "LCV")|(st.session_state.metacard_uac["VEHICLE_TYPE"] == "HCV")), 1, 0)


    # merge with mapping_ref
    st.session_state.metacard_uac = st.session_state.metacard_uac.merge(mapping_ref, how="inner", left_on="CAMPAIGN_NAME", right_on="Campaign")

    # keep only thows rows where campaign contains 'UAC_tCPA'
    st.session_state.metacard_uac = st.session_state.metacard_uac[st.session_state.metacard_uac["Campaign"].str.contains("UAC_ROI_tCPA")]

    # map geo_city_mapping_ref
    st.session_state.metacard_uac["City"] = st.session_state.metacard_uac["GEO_REGION_ID"].map(geo_city_mapping_ref)

    geo_acq_uac = st.session_state.metacard_uac.groupby(["REG_DATE_FORMATED", "Campaign", "City"]).agg({
        "MOBILE_NUMBER": "count",
        "2W_UAC": "sum",
        "LCV_UAC": "sum",
        "HCV_UAC": "sum",
        "Trucks_UAC": "sum",
        "Total_UAC": "sum",
        "SME_UAC": "sum",
        "Retail_UAC": "sum",
        "SME_2W_UAC": "sum",
        "SME_Trucks_UAC": "sum"
    }).reset_index()

    geo_acq_uac = geo_acq_uac[["REG_DATE_FORMATED", "City", "Campaign", "MOBILE_NUMBER", "2W_UAC", "LCV_UAC", "HCV_UAC", "Trucks_UAC", "Total_UAC", "SME_UAC", "Retail_UAC", "SME_2W_UAC", "SME_Trucks_UAC"]]
    st.dataframe(geo_acq_uac)

    # create key & Group data by key
    st.session_state.metacard_uac["REG_DATE_FORMATED"] = pd.to_datetime(st.session_state.metacard_uac["REG_DATE_FORMATED"], format="%d-%m-%Y")
    st.session_state.metacard_uac["key"] = st.session_state.metacard_uac["Campaign ID"] + "_" + st.session_state.metacard_uac["REG_DATE_FORMATED"].dt.strftime("%d-%m-%Y")
    st.session_state.metacard_uac = st.session_state.metacard_uac.groupby("key").agg({
        "2W_UAC": "sum", 
        "LCV_UAC": "sum", 
        "HCV_UAC": "sum", 
        "Trucks_UAC": "sum", 
        "Total_UAC": "sum",
        "SME_UAC": "sum",
        "Retail_UAC": "sum",
        "SME_2W_UAC": "sum",
        "SME_Trucks_UAC": "sum"
    }).reset_index()

    # Merge with campaign data
    st.session_state.campaign_data = st.session_state.campaign_data.merge(st.session_state.metacard_uac, how="left", on="key")

    # create total columns in campaign data named 2W_acq_total, LCV_acq_total, HCV_acq_total, Trucks_acq_total, PNM_acq, all_acq_total
    # fill na
    st.session_state.campaign_data = st.session_state.campaign_data.fillna(0)
    st.session_state.campaign_data["2W_acq_total"] = st.session_state.campaign_data["ACQ_2W"] + st.session_state.campaign_data["2W_UAC"] + st.session_state.campaign_data["2W_UACe"]
    st.session_state.campaign_data["LCV_acq_total"] = st.session_state.campaign_data["ACQ_LCV"] + st.session_state.campaign_data["LCV_UAC"] + st.session_state.campaign_data["LCV_UACe"]
    st.session_state.campaign_data["HCV_acq_total"] = st.session_state.campaign_data["ACQ_HCV"] + st.session_state.campaign_data["HCV_UAC"] + st.session_state.campaign_data["HCV_UACe"]
    st.session_state.campaign_data["Trucks_acq_total"] = st.session_state.campaign_data["ACQ_TRUCKS"] + st.session_state.campaign_data["Trucks_UAC"] + st.session_state.campaign_data["Trucks_UACe"]
    st.session_state.campaign_data["PNM_acq"] = st.session_state.campaign_data["PNM_CONV"]
    st.session_state.campaign_data["all_acq_total"] = st.session_state.campaign_data["2W_acq_total"] + st.session_state.campaign_data["Trucks_acq_total"] + st.session_state.campaign_data["PNM_acq"]
    st.session_state.campaign_data["SME_total"] = st.session_state.campaign_data["SME"] + st.session_state.campaign_data["SME_UAC"] + st.session_state.campaign_data["SME_UACe"]
    st.session_state.campaign_data["Retail_total"] = st.session_state.campaign_data["Retail"] + st.session_state.campaign_data["Retail_UAC"] + st.session_state.campaign_data["Retail_UACe"]
    st.session_state.campaign_data["SME_2W_total"] = st.session_state.campaign_data["SME_2W"] + st.session_state.campaign_data["SME_2W_UAC"] + st.session_state.campaign_data["SME_2W_UACe"]
    st.session_state.campaign_data["SME_Trucks_total"] = st.session_state.campaign_data["SME_Trucks"] + st.session_state.campaign_data["SME_Trucks_UAC"] + st.session_state.campaign_data["SME_Trucks_UACe"]
    
    st.session_state.campaign_data = st.session_state.campaign_data[["Date", "Campaign ID_y", "Campaign Name", "City", "Category", "2W_acq_total", "HCV_acq_total", "LCV_acq_total", "Trucks_acq_total", "PNM_acq", "all_acq_total", "ACQ_2W", "ACQ_HCV", "ACQ_LCV", "ACQ_TRUCKS","2W_UAC", "HCV_UAC", "LCV_UAC", "Trucks_UAC", "Total_UAC", "2W_UACe", "HCV_UACe", "LCV_UACe", "Trucks_UACe", "Total_UACe", "SME_total", "Retail_total","SME_2W_total","SME_Trucks_total", "SME", "Retail", "SME_UAC", "Retail_UAC", "SME_UACe", "Retail_UACe"]]
    
    st.dataframe(st.session_state.campaign_data)

    # Update Google Sheet if button is clicked
    if st.button("Update Google Sheet"):
        update_google_sheet(st.session_state.campaign_data, "1RsFcJ9NSFJTggG95zOOU9-kNQJQWwbQ8p6dc2-Ns78g", "Trial", ["A:AI"])
        update_google_sheet(geo_acq_2w_spot, "1RsFcJ9NSFJTggG95zOOU9-kNQJQWwbQ8p6dc2-Ns78g" ,"Geo_acq_2w_spot", ["A:M"])
        update_google_sheet(geo_acq_uace, "1RsFcJ9NSFJTggG95zOOU9-kNQJQWwbQ8p6dc2-Ns78g" ,"Geo_acq_uace", ["A:M"])
        update_google_sheet(geo_acq_uac, "1RsFcJ9NSFJTggG95zOOU9-kNQJQWwbQ8p6dc2-Ns78g" ,"Geo_acq_uac", ["A:M"])

import geopandas as gpd
import pandas as pd
import json
from shapely import wkt


gsa_account_file = '/Users/indirakasichhwa/Desktop/RT-12/gsa.account.csv'
farm_unit_file = '/Users/indirakasichhwa/Desktop/RT-12/2024_farm_unit_data_2024-01-01_2024-11-30.xlsx'
parcel_geometries_file = '/Users/indirakasichhwa/Desktop/RT-12/gsa.parcel (1).csv'
parcel_etaw_file = '/Users/indirakasichhwa/Desktop/RT-12/2024_parcel_etaw_data_2024-01-01_2024-11-30.xlsx'

gsa_account_data = pd.read_csv(gsa_account_file)
farm_unit_data = pd.read_excel(farm_unit_file, sheet_name='Sheet1')
parcel_geometry_data = gpd.read_file(parcel_geometries_file)
parcel_etaw_data = pd.read_excel(parcel_etaw_file, sheet_name='Sheet1')

data = {}

for index, row in gsa_account_data.iterrows():
    mailing_address = ", ".join(str(row[col]) for col in ["contact_street", "contact_city", "contact_state", "contact_zip"] if pd.notna(row[col]))
    
    record = {
        "account_id" :row["account_id"],
        "account_name": row["account_name"],
        "mailing_address" : mailing_address,
        "start_date": "2024-01-01", 
        "end_date": "2024-11-30",
        "msmt_method": row["msmt_method.msmt_method"],
        "report_creation_date": "01-02-2025", 
        "report_revision_date": "-" ,
        "geojson_parcels": {},
        "farm_units": [] 
    }
    farm_units_for_account = farm_unit_data[farm_unit_data['account_id'] == row['account_id']]

    for _, farm_row in farm_units_for_account.iterrows():
        farm_unit ={
            "farm_unit_zone": farm_row["farm_unit_zone"],
            "fu_sy_ac": farm_row["fu_sy_ac"],
            "fu_tw_ac": farm_row["fu_tw_ac"],
            "fu_alloc_af": farm_row["fu_alloc_af"],
            "fu_carryover_af": farm_row["fu_carryover_af"],
            "adjustment": farm_row.get("adjustment", 0),
            "fu_total_adjustment_af": farm_row["fu_total_adjustment_af"],
            "fu_etaw_af": farm_row["fu_etaw_af"],
            "fu_remain_af": farm_row["fu_remain_af"],
            "remaining_%": (farm_row["fu_remain_af"] / farm_row["fu_total_adjustment_af"]) * 100 if farm_row["fu_total_adjustment_af"] else 0,
            "parcels": [parcel.strip("[]' ") for parcel in farm_row["parcel_id"].split(",")] if pd.notna(farm_row["parcel_id"]) else [],
            "parcel_geometries": {} 
        }
        for parcel_id in farm_unit["parcels"]:
            parcel_geometry_row = parcel_geometry_data[parcel_geometry_data['apn'] == parcel_id]
            if parcel_geometry_row.empty:
                continue

            geometry_wkt = parcel_geometry_row['geometry'].values[0].split(';')[-1]  
            parcel_geometry = wkt.loads(geometry_wkt)  

            if parcel_geometry.geom_type == "Polygon":
                farm_unit["parcel_geometries"][parcel_id] = [[x, y] for x, y in parcel_geometry.exterior.coords]
            elif parcel_geometry.geom_type == "MultiPolygon":
                farm_unit["parcel_geometries"][parcel_id] = [[[x, y] for x, y in poly.exterior.coords] for poly in parcel_geometry.geoms]


        record["farm_units"].append(farm_unit)

    data[row["account_id"]] = record

with open('data.json','w') as f:
    json.dump(data , f , indent=4)
    print("done")

 





import geopandas as gpd
import pandas as pd
import numpy as np
import json

class FarmData:
    def __init__(self,gsa_account_file,farm_unit_file,parcel_geometries_file):
        self.gsa_account_file = gsa_account_file
        self.farm_unit_file = farm_unit_file
        self.parcel_geometries_file = parcel_geometries_file
        self.required_columns_farm_unit = ['account_id', 'farm_unit_zone', 'fu_sy_ac', 'fu_tw_ac','fu_alloc_af', 'fu_carryover_af', 'fu_total_adjustment_af', 'fu_etaw_af', 'fu_remain_af', 'parcel_id']

    def load_data(self):
        gsa_account_data = pd.read_csv(self.gsa_account_file)
        gsa_account_data = gsa_account_data[['account_name','account_id','msmt_method.msmt_method','contact_street', 'contact_city', 'contact_state', 'contact_zip']].fillna("")

        farm_unit_data = pd.read_excel(self.farm_unit_file, sheet_name='Sheet1')
        farm_unit_data = farm_unit_data[self.required_columns_farm_unit]
        farm_unit_data['parcels'] = farm_unit_data['parcel_id']

        parcel_geometry_data = gpd.read_file(parcel_geometries_file, ignore_geometry=True)
        parcel_geometry_data['geometry'] = gpd.GeoSeries.from_wkt(parcel_geometry_data['geometry'])
        parcel_geometry_data = parcel_geometry_data[['apn', 'geometry']]
        parcel_geometry_data = gpd.GeoDataFrame(parcel_geometry_data)
        parcel_geometry_data['polygon_coords'] = [
            [list(coord)[:2][::-1] for coord in geom.exterior.coords] if geom.geom_type == "Polygon" else None
            for geom in parcel_geometry_data.geometry
        ]
        return gsa_account_data,farm_unit_data,parcel_geometry_data

    def generate_data(self, gsa_account_data,farm_unit_data,parcel_geometry_data):
        account_farm_unit = pd.merge(gsa_account_data, farm_unit_data, on='account_id', how='inner')
        account_farm_unit['remaining_%'] = (account_farm_unit["fu_remain_af"] / account_farm_unit["fu_total_adjustment_af"]) * 100
        account_farm_unit['parcels'] = account_farm_unit['parcels'].apply(lambda x: [i.strip() for i in x.strip("[]' ").replace("'", "").split(',')])
        account_farm_unit.replace([np.inf, -np.inf], 0, inplace=True)

        data = {}
        farm_unit_group_by_account = account_farm_unit.groupby('account_id')

        for account_id, farm_unit_frame in farm_unit_group_by_account:
            parcels = [parcel for parcels in farm_unit_frame['parcels'] for parcel in parcels]
            farm_parcels = parcel_geometry_data[parcel_geometry_data['apn'].isin(parcels)]

            data[account_id] = {
                "account_id" :farm_unit_frame["account_id"].iloc[0],
                "account_name": farm_unit_frame["account_name"].iloc[0],
                "mailing_address": ", ".join(
                    str(farm_unit_frame[col].iloc[0]) for col in ["contact_street", "contact_city", "contact_state", "contact_zip"]
                    if col in farm_unit_frame and pd.notna(farm_unit_frame[col].iloc[0])
                ),
                "start_date": "2024-01-01",
                "end_date": "2024-11-30",
                "msmt_method": farm_unit_frame["msmt_method.msmt_method"].iloc[0],
                "report_creation_date": "01-02-2025",
                "report_revision_date": "-" ,
                "geojson_parcels": farm_parcels.to_json(),
                "farm_units": farm_unit_frame[self.required_columns_farm_unit+['remaining_%', 'parcels']].to_dict('records'),
                "parcel_geometries": dict(zip(farm_parcels['apn'], farm_parcels['polygon_coords']))
            }
        return data
        

    def save_data(self, data, output_file):
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
        print("done")

gsa_account_file = '/Users/indirakasichhwa/Desktop/RT-12/gsa.account.csv'
farm_unit_file = '/Users/indirakasichhwa/Desktop/RT-12/2024_farm_unit_data_2024-01-01_2024-11-30.xlsx'
parcel_geometries_file = '/Users/indirakasichhwa/Desktop/RT-12/gsa.parcel.csv'
output_file = 'data3.json'

data_processing = FarmData(gsa_account_file, farm_unit_file, parcel_geometries_file)
gsa_account_data, farm_unit_data, parcel_geometry_data = data_processing.load_data()
processed_data = data_processing.generate_data(gsa_account_data, farm_unit_data, parcel_geometry_data)
data_processing.save_data(processed_data, output_file)
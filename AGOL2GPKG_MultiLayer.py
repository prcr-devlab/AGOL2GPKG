
# coding: utf-8

# # AGOL to GeoPackage

# In[1]:


from arcgis.gis import GIS
from arcgis.features import FeatureSet
from shapely.geometry import (Polygon, MultiPolygon,
                             LineString, MultiLineString,
                             Point, MultiPoint,
                             GeometryCollection,
                             mapping)
import fiona
from fiona.crs import from_epsg
import pandas as pd
import sqlite3
import json
import datetime
import config


# ## Helpful stuff
# 
# List of items on AGOL to back up to a Geopackage

# In[2]:


items = [{
            "name":"parks_master",
            "item":"e891be88e3ea4e58824b76e1db80b126",
            "layer":0
         },
         {
            "name":"park_access_points",
            "item":"97ad22f4c1d74db7b6b64c3c0c95e3b0",
            "layer":0
         },
         {
             "name":"park_analysis_tiers",
             "item":"da7ab59d8303415b91d520f417d83537",
             "layer":0
         },
         {
             "name":"greenway_trails_master",
              "item":"d1e1f46354954d94b8600042533e27dc",
              "layer":0
         },
         {
             "name":"greenways_master",
              "item":"7b01cd380dc34f6bbc64e8215c95015e",
              "layer":0
         }]


# Function to classify Esri data types into generic data types

# In[3]:


def convert_esri_data_type(field_type):
    if field_type in ["esriFieldTypeOID", "esriFieldTypeGUID", "esriFieldTypeGlobalID"]:
        return None
    elif field_type in ["esriFieldTypeString", "esriFieldTypeDate"]:
        return "str"
    elif field_type in ["esriFieldTypeSmallInteger", "esriFieldTypeInteger"]:
        return "int"
    elif field_type in ["esriFieldTypeSingle", "esriFieldTypeDouble"]:
        return "float"
    else:
        return field_type


# Function to classify Esri geometries as shapely geometries

# In[4]:


def convert_esri_geometry_type(geometry_type):
    if geometry_type == "esriGeometryPoint":
        return "Point"
    elif geometry_type == "esriGeometryMultipoint":
        return "MultiPoint"
    elif geometry_type == "esriGeometryPolyline":
        return "MultiLineString"
    elif geometry_type == "esriGeometryPolygon":
        return "MultiPolygon"
    else:
        return "GeometryCollection"


# ## Login to AGOL

# In[5]:


gis = GIS(url = config.AGOL_URL,
          username = config.AGOL_USERNAME,
          password = config.AGOL_PASSWORD)


# ## Backup the data

# In[6]:


process_datetime = datetime.datetime.now()
gpkg_suffix = process_datetime.strftime("%Y%m%d_%H%m%S")
tables_list = []
for i in items:
    print("Processing {}...".format(i["name"]))
    # Get item layer as json
    item = gis.content.get(i["item"])
    if "Table" in dict(item.items())["typeKeywords"]:
        print("{} is a table. It will be processed after layers.\n".format(i["name"]))
        i["table_item"] = item
        tables_list.append(i)
    else:    
        item_json = json.loads(item.layers[i["layer"]].query().to_json)

        if item_json["geometryType"]:
            # Get CRS
            crs = from_epsg(item_json["spatialReference"]["latestWkid"])

            # Get Esri geometry type and convert to Shapely geometry type
            geometry_type = convert_esri_geometry_type(item_json["geometryType"])

        # Get fields and data types to setup schema
        properties_list = []
        for f in item_json["fields"]:
            field_type = convert_esri_data_type(f["type"])
            if field_type is not None:
                properties_list.append((f["name"], field_type))


        schema = {"geometry": geometry_type,
                  "properties": properties_list}


        geopackage_name = "parks_{}.gpkg".format(gpkg_suffix)
        feature_list = []

        for item_feature in item_json["features"]:
            multigeom_list = []
            geom = ""
            if schema["geometry"] == "MultiPolygon":
                for ring in item_feature["geometry"]["rings"]:
                    multigeom_list.append(Polygon(ring))
                geom = MultiPolygon(multigeom_list)
            elif schema["geometry"] == "MultiLineString":
                for path in item_feature["geometry"]["paths"]:
                    multigeom_list.append(LineString(path))
                geom = MultiLineString(multigeom_list)
            elif schema["geometry"] == "Point":
                item_feature_x = item_feature["geometry"]["x"]
                item_feature_y = item_feature["geometry"]["y"]
                geom = Point(item_feature_x, item_feature_y)

            properties_dict = {}
            for p in schema["properties"]:
                properties_dict[p[0]] = item_feature["attributes"][p[0]]
                
            feature = {"geometry": mapping(geom),
                           "properties": properties_dict
                          }
            feature_list.append(feature)

        print("Writing layer {} to {}".format(i["name"], geopackage_name))    
        with fiona.open(geopackage_name, "w", layer=i["name"], driver="GPKG",
                schema=schema, crs=crs) as dst:
            for f in feature_list:
                dst.write(f)
        print("Writing layer {} to {} complete!\n".format(i["name"], geopackage_name))

print("Processing {} table(s)...\n".format(len(tables_list)))
for t in tables_list:
    print("Processing {}".format(t["name"]))
    table = t["table_item"]
    table_df = table.tables[t["layer"]].query().df
    connection = sqlite3.connect(geopackage_name)
    print("Writing table {} to {}".format(t["name"], geopackage_name))
    table_df.to_sql(t["name"], connection)
    connection.close()
    print("Writing table {} to {} complete!\n".format(t["name"], geopackage_name))
    
print("Process complete.")


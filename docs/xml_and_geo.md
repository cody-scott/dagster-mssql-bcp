# XML/Geography/Geometry Types

If you would like to load XML, geometry or Geography types, these need to be cast to the hex format. 
BCP will load these if in that format, otherwise it will throw an error.

These will be loaded as VARBINARY types in SQL Server before being cast into the final type post load.

## XML

For XML types these simply need to be cast to the hex format. 
For example in polars we simply encode it and call `hex` to convert.

```python
schema = [
    {'name': 'xml_data', 'type': 'XML'}
]

pl.DataFrame(
    {'xml_data': [
        """<?xml version="1.0" encoding="UTF-8"?>
            <note>
            <to>Tove</to>
            <from>Jani</from>
            <heading>Reminder</heading>
            <body>Don't forget me this weekend!</body>
            </note>""".encode('utf-8').hex()
        ]})
```

## Geography/Geometry

For geography and geometry types, these also need to be cast to the hex format for their WKB representation. 
You may also provide an SRID, otherwise it will default to 4326, aka, web mercator.

This example uses geopandas to convert the data to hex.
```python
schema = [
    {'name': 'name_col', 'type': 'NVARCHAR', 'length': 20},
    {'name': 'geo_col', 'type': 'GEOGRAPHY', 'srid': 4326}
]

gdf = gpd.GeoDataFrame({
    "name_col": ["name1"],
    "geo_col": [
        Polygon(
            [
                [-80.54962058626626, 43.45142912346685],
                [-80.54962058626626, 43.39711241629678],
                [-80.41053208968418, 43.39711241629678],
                [-80.41053208968418, 43.45142912346685],
                [-80.54962058626626, 43.45142912346685],
            ]
        )
    ]
})
gdf["geo_col"] = gdf.set_geometry('geo_col').to_wkb(True)['geo_col']

```
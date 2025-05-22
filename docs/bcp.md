# BCP Loader

classes inheriting from `BCPCore` capture the generic behaviour of using the BCP loader. IO Managers also rely on this base class.
When creating an instance of this the arguments mirror the IOManager 1-1.

Primary use to using the loader is to not require the need to use/create the full IO manager for loading data. 
An example of this might be to collect data and insert it into a table from something other then dagster.

`PolarsBCP(...).load_bcp(dataframe, schema, table, asset_schema)` would load the data to the target table using the same bcp loader as the io manager.
In this case the data is first loaded to a staging table then inserted into the target table. This is effectively an `append` operation on your target table.


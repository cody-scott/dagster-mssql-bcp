# Development

The IO manager is a wrapper around the generic BCP loader. This splits the logic from the IO out from the BCP loader itself. This allows for easier testing and development of the BCP loader, known as `BCPCore`.

## BCP Core

This is the core abstract base class that the IO manager uses. This is the class that should be inherited and implemented to create a new BCP loader.

implementing this class requires the following methods:

* `_reorder_columns`

    Should reorder the columns to match the order in the list passed in.

* `_add_meta_columns`

    Should add the metadata columns to the dataframe using the properties `row_hash_column_name`, `load_uuid_column_name` and `load_datetime_column_name`.

* `_process_datetime`

    Should process the datetime columns in the dataframe to the accepted format. Removing the `+Z` and replacing with `+00:00`. and `T` between the date and time.

* `_replace_values`
    
    Replace the values in the dataframe with the replacements in the `replacements` dictionary. This includes NULLS with empty spaces. and new lines and tabs with the properties `_new_line_character` and `_tab_character`.

* `_rename_columns`
    
    Rename the columns in the data with the the values from the `columns` dict. Keys being current, values being new.

* `_get_frame_columns`

    return a list of the columns in the data.

* `_filter_columns`

    Remove any columns that are not in the `columns` list.

* `_save_csv`

    Save the dataframe to a CSV file.

## BCP IO Manager

The IO manager is a wrapper around the BCP core. This is the class that should be used in the dagster pipeline. This class should be inherited and the `bcp` method should be implemented. This method should return the data to be loaded.

implementing this class requires the following methods:

* `load_input`: Load the data from the input manager.
* `check_empty`: Check if the data is empty. return bool if empty
* `get_bcp`: Return the BCP core class to use.

If you need to connect to something like azure for auth, this is a good place to override the `setup_for_execution` function to process your authentication logic before passing the arguments down to the `BCPCore` using the `query_props` and `bcp_arguments`.

```python
# example
from dagster_mssql_bcp import PandasBCPIOManager
from pydantic import PrivateAttr

class PandasIOWithSomeOtherAuth(PandasBCPIOManager):
    def setup_for_execution(self):
        result = my_auth_function()
        self.bcp_arguments = {
            '-G': result.token
        }
        self.query_params = {
            'token': result.token
        }
        yield self
```
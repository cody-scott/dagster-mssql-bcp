import dagster as dg
from dagster_mssql_bcp.bcp_core.bcp_core import (
    BCPConnectionConfig,
    BCPCore,
    AssetSchema,
)


class BCPResource(BCPConnectionConfig, dg.ConfigurableResource):
    def load_bcp(
        self,
        data,
        schema: str,
        table: str,
        asset_schema: list[dict] | AssetSchema | None = None,
        add_row_hash: bool | None = None,
        add_load_datetime: bool | None = None,
        add_load_uuid: bool | None = None,
        add_identity_column: bool | None = None,
        uuid: str | None = None,
        process_datetime: bool | None = None,
        process_replacements: bool | None = None,
    ):
        return self.get_engine().load_bcp(
            data=data,
            schema=schema,
            table=table,
            asset_schema=asset_schema,
            add_row_hash=add_row_hash,
            add_load_datetime=add_load_datetime,
            add_load_uuid=add_load_uuid,
            add_identity_column=add_identity_column,
            uuid=uuid,
            process_datetime=process_datetime,
            process_replacements=process_replacements,
        )

    def get_engine(self) -> BCPCore:
        raise NotImplementedError("")

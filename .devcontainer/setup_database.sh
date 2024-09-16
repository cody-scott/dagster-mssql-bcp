# /bin/bash

DBSTATUS=1
ERRCODE=1
i=0

while [[ $DBSTATUS -ne 0 ]] && [[ $i -lt 60 ]] && [[ $ERRCODE -ne 0 ]]; do
	i=$i+1
	DBSTATUS=$(/opt/mssql-tools18/bin/sqlcmd -C -S dagster_sql_server_bcp,1433 -U sa -P yourStrong_Password -Q "SET NOCOUNT ON; Select SUM(state) from sys.databases")
	ERRCODE=$?
	sleep 1
done

if [[ $DBSTATUS -ne 0 ]] || [[ $ERRCODE -ne 0 ]]; then
	echo "SQL Server took more than 60 seconds to start up or one or more databases are not in an ONLINE state"
	exit 1
fi

echo "Database is up and running"

/opt/mssql-tools18/bin/sqlcmd -C -S dagster_sql_server_bcp,1433 -U sa -P yourStrong_Password -Q "CREATE DATABASE dagster_bcp"

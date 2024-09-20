# /bin/bash

sudo apt-get update
sudo apt-get -f install

# Install SQL Server Drivers/SQLCMD
echo "Installing MSSQL"
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | sudo tee /etc/apt/sources.list.d/msprod.list

sudo apt-get update
sudo ACCEPT_EULA=Y apt-get -y install mssql-tools18 unixodbc-dev msodbcsql18

source ~/.bashrc

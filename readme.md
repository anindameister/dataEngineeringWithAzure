#### This is data engineering with azure Project 2
# Task 1: Create your Azure resources
- Create an Azure Database for PostgreSQL.
- Create an Azure Synapse workspace. Note that if you've previously created a Synapse Workspace, you do not need to create a second one specifically for the project.
- Use the built-in serverless SQL pool and database within the Synapse workspace

# Solution
- Task 1 is just easy peesy, considering what have I been doing all this while

# Task 2: Design a star schema
- You are being provided a relational schema that describes the data as it exists in PostgreSQL. 
![relational schema that describes the data as it exists in PostgreSQL](https://github.com/anindameister/dataEngineeringWithAzure/blob/main/divvy-erd.png)
    - Tables:
        - Rider: DimRider
            - rider_id (INT PRIMARY KEY)
            - address (VARCHAR)
            - first (VARCHAR)
            - last (VARCHAR)
            - birthday (date)
            - account_number (INT FOREIGN KEY REFERENCES Account(account_number))
        - Account: DimAccount
            - account_number (INT PRIMARY KEY)
            - member (bool)
            - start_date (date)
            - end_date (date)
        - Payment: DimPayment
            - payment_id (INT PRIMARY KEY)
            - date (date)
            - amount (decimal)
            - account_number (INT FOREIGN KEY REFERENCES Account(account_number)) 
        - Station: DimStation
            - station_id (varchar PRIMARY KEY)
            - name (varchar)
            - latitude (float)
            - longitude (float)
        - Trip: DimTrip
            - trip_id (varchar PRIMARY KEY)
            <!-- There's a minor type mismatch for the rideable_type field in the Trip table, which should be VARCHAR to match the diagram instead of string. -->
            - rideable_type (string)
            - started_at (datetime)
            - ended_at (datetime)
            <!-- The Trip table's foreign keys start_station_id and end_station_id are mentioned as INT in your text, but given they reference Station(station_id) which is a VARCHAR, the data types should match. Therefore, the foreign keys should also be of type VARCHAR to correctly reference the primary key of the Station table. -->
            - start_station_id (varchar FOREIGN KEY REFERENCES Station(station_id)) 
            - end_station_id (varchar FOREIGN KEY REFERENCES Station(station_id)) 
            <!-- The foreign key in the Trip table should be rider_id, not member_id, to match the rider_id primary key in the Rider table according to the diagram. -->
            - member_id (INT FOREIGN KEY REFERENCES Rider(rider_id)) 

 - DimRider(rider_id),DimAccount(account_number),DimPayment(payment_id),DimStation(station_id),DimTrip(trip_id)
 - rider_id, account_number, payment_id, station_id, and trip_id would serve as primary keys in their respective dimension tables. These primary keys uniquely identify rows in each dimension table and would then be used as foreign keys in a central fact table, linking the fact table to each of the dimension tables. This setup allows for efficient querying and analysis of the data across different dimensions.
- my dimension tables are

        - Rider: DimRider
            - rider_id (INT PRIMARY KEY)
            - address (VARCHAR)
            - first (VARCHAR)
            - last (VARCHAR)
            - birthday (date)

        - Account: DimAccount
            - account_number (INT PRIMARY KEY)
            - member (bool)
            - start_date (date)
            - end_date (date)

        - Payment: DimPayment
            - payment_id (INT PRIMARY KEY)
            - date (date)
            - amount (decimal)

        - Station: DimStation
            - station_id (varchar PRIMARY KEY)
            - name (varchar)
            - latitude (float)
            - longitude (float)

        - Trip: DimTrip
            - trip_id (varchar PRIMARY KEY)
            <!-- There's a minor type mismatch for the rideable_type field in the Trip table, which should be VARCHAR to match the diagram instead of string. -->
            - rideable_type (string)
            - started_at (datetime)
            - ended_at (datetime)
- my fact table
    - FactRental
        - rental_id (INT PRIMARY KEY)
        - rider_id (INT)
        - account_number (INT)
        - payment_id (INT)
        - trip_id (INT)
        <!-- Ensure the data types for start_station_id and end_station_id in FactRental match the data type of station_id in DimStation (which should be VARCHAR, not INT as currently listed) for proper relational integrity. -->
        - start_station_id (INT)
        - end_station_id (INT)

Based on your descriptions, Task 2, which involves designing a star schema using fact and dimension tables, appears to have been conceptually addressed with the definition of your dimension tables (DimRider, DimAccount, DimPayment, DimStation, DimTrip) and your fact table (FactRental). This schema aligns with the principles of a star schema for data warehousing, focusing on a central fact table related to multiple dimension tables that describe various entities.

-need to take care of pgadmin (done)
# For Task 3, here's a concise guide:

- Download Python Script: Obtain ProjectDataToPostgres.py from the provided GitHub link. (done)
- Download Data Files: Get the data files from classroom resources. (done)
- Edit Script in VS Code: (done)
- Open the ProjectDataToPostgres.py script in Visual Studio Code (VS Code). = done
- Fill in your PostgreSQL database's host, username, and password information within the script. (done)
- Run the Script:
    - `# sslmode = "require" sslmode='disable'`
    - SSL mode determines the level of security for a connection to a PostgreSQL database, specifying whether the connection should use SSL encryption and how strictly to enforce it.
- Execute the script in a command line or terminal to load data into your PostgreSQL database.
- Ensure Python is installed on your machine and you're in the directory containing the script.
- Verify Data Upload:(done)
    - Use pgAdmin or another PostgreSQL tool to check that the data from all four files has been correctly uploaded to your PostgreSQL database.
- This task simulates setting up an OLTP (Online Transaction Processing) system environment in PostgreSQL, replicating a production scenario for data usage.

# Task 4
- Task 4: EXTRACT the data from PostgreSQL

- In your Azure Synapse workspace, you will use the ingest wizard to create a one-time pipeline that ingests the data from PostgreSQL into Azure Blob Storage. This will result in all four tables being represented as text files in Blob Storage, ready for loading into the data warehouse.

Certainly! Let's go through the steps one by one:

1. **Create an Azure Synapse Workspace**:
   - Go to the Azure Portal (https://portal.azure.com)
   - Click on "Create a resource"
   - Search for "Azure Synapse Analytics"
   - Click "Create"
   - Provide the required details (Resource group, Workspace name, Region, etc.)
   - Click "Review + create" and then "Create"

2. **Create a PostgreSQL Database within Azure**:
   - In the Azure Portal, click "Create a resource"
   - Search for "Azure Database for PostgreSQL"
   - Click "Create"
   - Provide the required details (Resource group, Server name, Location, etc.)
   - Set up the administrator user and password
   - Click "Review + create" and then "Create"

3. **Create an Azure Blob Storage**:
   - In the Azure Portal, click "Create a resource"
   - Search for "Storage account"
   - Click "Create"
   - Provide the required details (Resource group, Storage account name, Location, etc.)
   - Click "Review + create" and then "Create"

4. **Create a Linked Service between Blob Storage and PostgreSQL**:
   - Open your Azure Synapse Workspace
   - Go to the "Manage" hub
   - Under "External connections," click "Linked services"
   - Click "New"
   - Search for the desired connector (e.g., "Azure Blob Storage" or "PostgreSQL")
   - Select the connector and provide the required connection details
   - Click "Create"

5. **Locate the Serverless SQL Pool in Azure Synapse and Upload the Data Files**:
   - In your Azure Synapse Workspace, go to the "Develop" hub
   - Expand the "SQL script" section on the left
   - Click on the "+" icon to create a new SQL script
   - In the script editor, use the following command to create a new database:
     ```sql
     CREATE DATABASE YourDatabaseName;
     ```
   - Switch to the newly created database using:
     ```sql
     USE YourDatabaseName;
     ```
   - Use the `OPENROWSET` function to import data from the PostgreSQL database into your Synapse SQL tables. For example:
     ```sql
     CREATE EXTERNAL TABLE YourTableName
     WITH (
         LOCATION = 'https://yourblobstorage.blob.core.windows.net/path/to/data/',
         DATA_SOURCE = YourLinkedServiceName,
         FILE_FORMAT = TextFileFormat
     )
     AS
     SELECT *
     FROM OPENROWSET(
         BULK 'path/to/data/file.txt',
         DATA_SOURCE = 'YourLinkedServiceName',
         FORMAT = 'CSV'
     ) AS DataFile;
     ```
   - Replace `YourTableName`, `YourLinkedServiceName`, and the file paths with your actual values.

Make sure to replace the placeholders (e.g., `YourDatabaseName`, `YourTableName`, `YourLinkedServiceName`, file paths) with your actual values throughout the process.

After completing these steps, you should have your PostgreSQL data ingested into Azure Blob Storage and loaded into the Serverless SQL Pool within your Azure Synapse Workspace.

In the SQL statement you provided:

```sql
CREATE EXTERNAL TABLE YourTableName
WITH (
    LOCATION = 'https://yourblobstorage.blob.core.windows.net/path/to/data/',
    DATA_SOURCE = YourLinkedServiceName,
    FILE_FORMAT = TextFileFormat
)
AS
SELECT *
FROM OPENROWSET(
    BULK 'path/to/data/file.txt',
    DATA_SOURCE = 'YourLinkedServiceName',
    FORMAT = 'CSV'
) AS DataFile;
```

The data files are not being uploaded directly. Instead, this statement is creating an external table in your Serverless SQL Pool that points to the data files already stored in your Azure Blob Storage.

To upload the data files to Azure Blob Storage, you need to follow these steps:

1. **Create a container in your Azure Blob Storage**:
   - In the Azure Portal, go to your Azure Blob Storage account.
   - Click on "Containers" and then "+ Container" to create a new container.
   - Provide a name for the container and set the appropriate access level.

2. **Upload the data files to the container**:
   - Once the container is created, click on it to open it.
   - Click "Upload" to upload your data files to the container.
   - You can upload files individually or in bulk.

3. **Get the path to the data files**:
   - After uploading the files, you need to get the full path to each file.
   - The path will be in the format: `https://yourstorageaccountname.blob.core.windows.net/containername/filename.txt`

4. **Update the SQL statement**:
   - In the SQL statement you provided, replace `'path/to/data/file.txt'` with the actual path to your data file in Azure Blob Storage.
   - Also, replace `'YourLinkedServiceName'` with the name of the linked service you created between Azure Synapse and Azure Blob Storage.

5. **Execute the SQL statement**:
   - Run the SQL statement in your Serverless SQL Pool to create the external table pointing to the data files in Azure Blob Storage.

After executing the SQL statement, you should have an external table in your Serverless SQL Pool that can query and access the data files stored in Azure Blob Storage.

Note: If you have multiple data files, you'll need to create an external table for each file or use a wildcard to match multiple files (e.g., `'path/to/data/*.txt'`).


# Task 5
Task 5: LOAD the data into external tables in the data warehouse
Once in Blob storage, the files will be shown in the data lake node in the Synapse Workspace. From here, you can use the script-generating function to load the data from blob storage into external staging tables in the data warehouse you created using the serverless SQL Pool.

Sure, here are the steps to load the data from Azure Blob Storage into external tables in the data warehouse using the serverless SQL pool in Azure Synapse Analytics:

1. **Open the Synapse Studio**:
   - In the Azure Portal, navigate to your Azure Synapse Analytics workspace.
   - Click on "Open Synapse Studio" to launch the Synapse Studio.

2. **Create a database in the serverless SQL pool**:
   - In the Synapse Studio, go to the "Develop" hub.
   - Expand the "SQL script" section on the left.
   - Click on the "+" icon to create a new SQL script.
   - In the script editor, use the following command to create a new database:
     ```sql
     CREATE DATABASE YourDatabaseName;
     ```
   - Switch to the newly created database using:
     ```sql
     USE YourDatabaseName;
     ```

3. **Create external data sources**:
   - In the Synapse Studio, go to the "Manage" hub.
   - Under "External connections," click "Linked services."
   - Create a new linked service to connect to your Azure Blob Storage account.

4. **Create external file formats**:
   - In the "Develop" hub, expand the "External resources" section on the left.
   - Right-click on "File formats" and select "New file format."
   - Provide a name for the file format (e.g., `TextFileFormat`).
   - Configure the file format settings based on your data files (delimiter, row terminator, etc.).

5. **Create external tables**:
   - In the "Develop" hub, expand the "External resources" section on the left.
   - Right-click on "Data sources" and select "New data source."
   - Select the linked service you created for Azure Blob Storage.
   - Right-click on "External tables" and select "Create external table."
   - Provide a name for the external table.
   - In the "Select objects" dialog, browse to the location of your data files in Azure Blob Storage.
   - Select the file(s) you want to load and click "Import."
   - In the "Create external table" dialog, configure the external table settings:
     - Select the appropriate file format.
     - Provide a table schema or use the "Import schema" option if available.
     - Click "Create and wait" to create the external table.

6. **Load data into the external tables**:
   - In the "Develop" hub, create a new SQL script.
   - Use a `SELECT` statement to query the data from the external table:
     ```sql
     SELECT * FROM YourExternalTableName;
     ```
   - Optionally, you can create internal tables in the serverless SQL pool and insert the data from the external tables:
     ```sql
     CREATE TABLE YourInternalTableName (
         -- Define the table schema here
     );

     INSERT INTO YourInternalTableName
     SELECT * FROM YourExternalTableName;
     ```

After following these steps, you will have successfully loaded the data from Azure Blob Storage into external tables in the serverless SQL pool of your Azure Synapse Analytics workspace. You can then query or transform the data as needed.

Note: Make sure to replace `YourDatabaseName`, `YourExternalTableName`, and `YourInternalTableName` with the actual names you want to use for your database, external table, and internal table, respectively.
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
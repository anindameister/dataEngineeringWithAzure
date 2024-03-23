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
    - DimRider
            - rider_id (INT PRIMARY KEY)
            - address (VARCHAR)
            - first (VARCHAR)
            - last (VARCHAR)
            - birthday (date)
            - account_number 
            - member (bool)
            - start_date (date)
            - end_date (date)
- 
Your design for the DimRider dimension table in a star schema includes both attributes specific to the rider (such as rider_id, address, first, last, birthday) and attributes related to the rider's account (account_number, member, start_date, end_date). This is somewhat correct, but typically in a star schema, dimension tables are more focused.

Combining rider and account information into one dimension table can work, especially if account_number directly relates to the rider and doesn't require a separate dimension for analysis. However, if account_number links to distinct account characteristics or behaviors that are independent of individual riders, you might consider keeping Account as a separate dimension table for clarity and analytical flexibility.
           



           





- In addition, you have been given a set of business requirements related to the data warehouse. 
    - You are being asked to design a star schema using fact and dimension tables.
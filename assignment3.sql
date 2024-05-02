-- Step 1: Copy the zip file into my own directory
-- create folder austin
gongl@msca-bdp-hadoop-m:~$ ls -l /home/gongl/
gongl@msca-bdp-hadoop-m:~$ mkdir /home/gongl/austin
gongl@msca-bdp-hadoop-m:~$ cp /home/dmitri/austin/Municipal_Court_Caseload_Information.zip /home/gongl/austin


-- Step 2: Unzip the file
gongl@msca-bdp-hadoop-m:~$ unzip /home/gongl/austin/Municipal_Court_Caseload_Information.zip -d /home/gongl/austin
gongl@msca-bdp-hadoop-m:~$ ls -l /home/gongl/austin


-- Step 3: Load the file into Hive table
-- copy from Linux into HDFS on Hadoop Cluster
gongl@msca-bdp-hadoop-m:~$ hadoop fs -ls /user/gongl
gongl@msca-bdp-hadoop-m:~$ hadoop fs -mkdir /user/gongl/austin
gongl@msca-bdp-hadoop-m:~$ hadoop fs -copyFromLocal /home/gongl/austin/Municipal_Court_Caseload_Information.csv /user/gongl/austin/Municipal_Court_Caseload_Information.csv
gongl@msca-bdp-hadoop-m:~$ hadoop fs -ls /user/gongl/austin

-- Step 3.1: Preview the data using the head command
gongl@msca-bdp-hadoop-m:~$ head /home/gongl/austin/Municipal_Court_Caseload_Information.csv

-- Step 3.2: Based on the data preview, create a Hive table with appropriate data types and handle the header record
-- Example Hive table creation script
gongl@msca-bdp-hadoop-m:~$ hive
--switch to my database;
hive> USE gongl;
hive> SHOW TABLES;

hive> DROP TABLE Municipal_Court_Caseload_Information;

hive> CREATE TABLE Municipal_Court_Caseload_Information (
    >     OffenseCaseType STRING,
    >     OffenseDate DATE,
    >     OffenseTime TIMESTAMP,
    >     OffenseChargeDescription STRING,
    >     OffenseStreetName STRING,
    >     OffenseCrossStreetCheck STRING,
    >     OffenseCrossStreet STRING,
    >     SchoolZone STRING,
    >     ConstructionZone STRING,
    >     CaseClosed STRING
    > )
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY ','
    > LINES TERMINATED BY '\n'
    > TBLPROPERTIES ("skip.header.line.count"="1"); -- You can skip the header record using the TBLPROPERTIES clause during table creation



-- Step 4: Ensure you process the header record correctly
-- In the Hive table creation script, ensure that the data types and column names match the structure of the CSV file. 
-- The skip.header.line.count property in the table properties ensures that the header record is skipped during data loading.
-- After running these commands, the data should be loaded into the Hive table "Municipal_Court_Caseload_Information" without including the header record.
hive> LOAD DATA INPATH '/user/gongl/austin/Municipal_Court_Caseload_Information.csv' INTO TABLE Municipal_Court_Caseload_Information;
hive> DESCRIBE Municipal_Court_Caseload_Information;
hive> DESCRIBE FORMATTED Municipal_Court_Caseload_Information;
-- Skip the header record using the TBLPROPERTIES clause during table creation
-- Or use the "skip.header.line.count" property in the LOAD DATA statement

gongl@msca-bdp-hadoop-m:~$ ls -l /home/gongl/austin

-- Step 5: Calculate frequency of offenses by Offense Case Type
hive> SELECT OffenseCaseType, COUNT(*) AS offense_count
    > FROM Municipal_Court_Caseload_Information
    > GROUP BY OffenseCaseType;


-- Result is shown below
-- CO      240308
-- CM      319078
-- TR      4313221
-- PK      3388981
-- RL      224188



-- Step 6: Identify the most frequent offenses by Offense Charge Description

-- This query will save the result of SELECT query to a CSV file named "offense_counts.csv" in the specified directory on my local Linux machine.
-- To save the result of a SQL query into a CSV file in a Linux directory, use the hive command along with the INSERT OVERWRITE LOCAL DIRECTORY statement.

hive> SELECT OffenseChargeDescription, COUNT(*) AS offense_count 
    > FROM Municipal_Court_Caseload_Information 
    > GROUP BY OffenseChargeDescription
    > ORDER BY offense_count DESC
    > LIMIT 20;


-- top 20 most frequent offenses by Offense Charge Description

-- PAY STATION RECEIPT NOT DISPLAYED       892013
-- EXPIRED PAY STATION RECEIPT     732605
-- SPEEDING-STATE HIGHWAYS 486576
-- NO DRIVERS LICENSE      372339
-- SPEEDING - POSTED CITY STREET   345162
-- FAILED TO MAINTAIN FINANCIAL RESPONSIBILITY     337672
-- PARKING - EXPIRED METER 310816
-- SPEEDING - STATE HIGHWAY        287570
-- FAIL TO MAINTAIN FINANCIAL RESP 260662
-- TOW AWAY ZONE NO PARKING AREA   238168
-- Ran Red Light - Photo   224213
-- EXPIRED REGISTRATION TTC:502.407        190433
-- RAN RED LIGHT   157783
-- EXPIRED INSPECTION STICKER      141525
-- PARKING - TOW AWAY ZONE 111993
-- EXPIRED REGISTRATION    96945
-- SPEEDING - SCHOOL ZONE  92559
-- RAN STOP SIGN   89296
-- INSPECTION STICKER - EXPIRED    86344
-- NO SEAT BELT-DRIVER/PASSENGER   83008

-- step below under step 6 are my personal notes - out of scope of this homework

hive> INSERT OVERWRITE LOCAL DIRECTORY "/home/gongl/austin" 
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY "," 
    > SELECT OffenseChargeDescription, COUNT(*) AS offense_count 
    > FROM Municipal_Court_Caseload_Information 
    > GROUP BY OffenseChargeDescription
    >
    > ORDER BY offense_count DESC;

gongl@msca-bdp-hadoop-m:~$ ls -l /home/gongl/austin

-- After running this command, Hive will execute the query and save the result into the /home/gongl/austin/ directory as a CSV file named 000000_0. Rename this file to offense_counts.csv using the mv command if needed:

gongl@msca-bdp-hadoop-m:~$ mv /home/gongl/austin/000000_0 /home/gongl/austin/offense_counts.csv
gongl@msca-bdp-hadoop-m:~$ head -n 5 /home/gongl/austin/offense_counts.csv

-- SFTPing the data to my laptop from Linux
-- Make sure open a new terminal !!! 
sftp gongl@34.173.32.254
sftp> get /home/gongl/austin/offense_counts.csv "C:/Users/gongl1/Desktop/Big_Data-Dmitri/Assignment3/offense_counts.csv"



-- Step 7: Delete all data in my Linux / HDFS directories and Hive tables
-- Remove files in Linux directory
gongl@msca-bdp-hadoop-m:~$ rm -r /home/gongl/austin
gongl@msca-bdp-hadoop-m:~$ ls -l /home/gongl/

-- Remove files in HDFS directory
gongl@msca-bdp-hadoop-m:~$ hadoop fs -rm -r /user/gongl/austin
gongl@msca-bdp-hadoop-m:~$ hadoop fs -ls /user/gongl/

-- Delete data from Hive table
hive> DROP TABLE Municipal_Court_Caseload_Information;


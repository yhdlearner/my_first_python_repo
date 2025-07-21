Consider yourself as an expert in dbt. Create a dbt best in class and production efficient common process to do the following
1) Insert a control table that has the start and end timestamp of the source table that should be extracted
2) Create a stage table that selects only incremental records from source table based on the start and end timestamp of the control table
3) Using the above stage table records and load into a target dimension table using slowly changing type 2.
4) The dimension table has start timestamp, end timestamp and current record indicator

Can you change this complete process to be generic so that it can be used for any tables that is provided with key column and change column as input

Can you also change the process to handle key column that could be multiple columns


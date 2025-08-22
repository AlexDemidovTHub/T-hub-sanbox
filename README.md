# airflow_task


✅ ***AD_HOC_MAINTENANCE***

This DAG is designed for maintenance operations on the de_assignment database.
It performs the following tasks:

Data Quality Validation (validate_data_quality):

 - Checks for NULL values in user_id and activity_date.
 - Ensures dates are valid (e.g., no future dates).
 - Logs the number of detected issues for monitoring.

Archiving Old Data (archive_old_data):

 - Creates an archive table user_activity_archive if it doesn’t exist.
 - Moves records older than 1 year into the archive.
 - Cleans up outdated rows from the main user_activity table.

Database Optimization (VACUUM ANALYZE):

  Runs VACUUM ANALYZE on:
   - user_activity
   - user_profiles
   - daily_activity_metrics

Design Choices:

- PythonOperator is used for data quality validation and archiving to allow flexible business logic.
- PostgresOperator is used for VACUUM tasks since they run direct SQL commands.
- Centralized failure_alert function provides consistent error handling and alerting.
- Archiving is separated into its own task for easier scaling (e.g., partitioning support in the future).


✅ ***AIRPORTS_ONE_LOVE***

This DAG is designed for ETL operations with the airports dataset.
It performs the following tasks:

Data Extraction (extract_airports_data):

 - Downloads airport dataset from a public GitHub source.
 - Converts the data into a Pandas DataFrame for further processing.
 - Passes the dataset downstream using XCom.

Schema Check & Creation (check_and_create_table):

 - Checks if the airports table exists in PostgreSQL.
 - If it doesn’t exist, the table is created automatically.

Data Loading (load_airports_data):

 - Loads the dataset into PostgreSQL using efficient bulk loading (COPY FROM STDIN).
 - Replaces or appends data depending on table state

Send Report (send_email_report):

 - Selects sample rows (e.g., first 10 records) from the airports table.
 - Sends them as a CSV attachment via email (Mail.ru SMTP).

Design Choices:

 - PostgresHook is used for database interaction, enabling efficient bulk inserts.
 - SqlSensor + PostgresOperator ensure schema is created dynamically if absent.
 - PythonOperator is used for extraction and email reporting to allow flexible business logic.
 - Centralized failure_alert function provides consistent error handling and alerting.
 - Email reporting ensures quick visibility into successful loads.
 - Retries with exponential backoff add fault tolerance for network or API issues.


✅ ***DAILY_PROCESSING_PIPELINE***

This DAG is designed for daily ETL processing of user activity data in the de_assignment database.
It performs the following tasks:

Data Extraction (extract_daily_activities):

  Queries the user_activity table to calculate:
   - The number of active users for the previous day.
   - All activities where duration_minutes > 5 and activity_date = yesterday.

Data Transformation (transform_activity_data):

 - Pulls extracted data from XCom and loads it into a Pandas DataFrame.
 - Enriches the dataset with:
   - weekday_name (day of the week for each activity).
   - duration_bucket (categorizes user activity into 3 groups: 0–30 min, 30–60 min, 60+ min).
  
 - Stores the transformed dataset into a temporary PostgreSQL table:
   - de_assignment.temp_daily_activity_metrics.

Data Aggregation & Loading (load_daily_aggregates):

 - Ensures the target table de_assignment.daily_activity_metrics exists.
 - Inserts aggregated metrics:
   - user_id, activity_date, weekday_name, duration_bucket
   - activity_count (number of activity events per group)
   - avg_minutes (average duration of activities).
 - Runs inside a transactional block (BEGIN … COMMIT) to ensure consistency.

Design Choices

 - PythonOperator is used for extraction and transformation to leverage Python (SQL + Pandas) flexibility.
 - PostgresOperator handles final aggregation and ensures transactional consistency.
 - XCom is used for passing intermediate datasets between tasks.
 - Pandas enables feature engineering (weekday and duration buckets).
 - Centralized failure_alert function sends email notifications via Mail.ru SMTP when any task fails.
 - Retries with exponential backoff improve resilience against temporary database or network issues.
 - Temporary staging table (temp_daily_activity_metrics) ensures separation between raw and aggregated data.


✅ ***DAILY_PROCESSING_PIPELINE***

This DAG is designed to perform weekly segmentation of users in the de_assignment database.
It evaluates user activity for the past week and updates user tiers (bronze, silver, gold) based on activity levels.
It performs the following tasks:

Data Extraction (extract_user_profiles):

 - Reads data from user_profiles and user_activity tables.
 - Pushes results into XCom for downstream processing.

Data Transformation (calculate_user_metrics):

 - Joins user_profiles with user_activity.
 - Filters records for the last 7 days.
 - Calculates two key metrics per user:
   - Number of activities (user_activities)
   - Total activity duration (total_user_duration)

 - Builds SQL update queries for assigning tiers:
   - Bronze: < 5 activities
   - Silver: 5–15 activities
   - Gold: > 15 activities
    
 - Pushes update queries into XCom.

Tier Updates (update_user_tiers):

 - Executes SQL updates against the user_profiles table using PostgresOperator.

Failure Handling:

 - Centralized failure_alert function:
   - Sends email notifications via Mail.ru SMTP if a task fails.
   - Includes DAG name, task ID, execution date, log URL, and error details.

Design Choices

 - PythonOperator is used for extraction and transformation to enable flexible logic with Pandas.
 - PostgresOperator is used for updating tiers since the final step is a pure SQL execution.
 - XCom is used for data passing between tasks (tables → Pandas transformation → SQL updates).
 
 - Retry Strategy:
   - 3 retries per task
   - Exponential backoff (delays increase gradually)
   - Ensures transient issues (like database connection drops) do not cause DAG failure.

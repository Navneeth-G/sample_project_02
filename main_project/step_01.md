
##  Component 1: Counting Records in Source Files (Kubernetes Pod)

###  What This Code Does

The first script's purpose is to **connect to a Kubernetes pod**, locate a specific text file for each configured environment (or "farm"), and **extract only the relevant data blocks**. These data blocks follow a legacy structure — so we ensure the parser mimics that legacy behavior precisely.

Instead of parsing and storing full data records, this function only counts how many valid records exist across all environments. Once the total count is obtained, it **connects to Snowflake** and updates an audit table with this count under a designated column (`ES_count`). This lets downstream processes (like data validation and ingestion timing) know the expected size of the dataset.

We do this before any heavy processing or file generation, because knowing the **data volume early** allows us to:

* Decide whether to proceed
* Allocate enough time for ingestion or cleanup tasks
* Detect anomalies in data generation (e.g., unusually low or high counts)

###  Why It Matters

In a pipeline where the source data is ephemeral (available only for one day), it’s risky to assume that the data is always present and well-formed. By validating the count up front, we minimize downstream surprises. It’s also our chance to **log metadata** (farm names, counts, timestamps) before any transformation.

###  High-Level Workflow

1. Load configuration values (farms list, file path template, timezone, Snowflake table name).
2. Loop through each farm and:

   * Check if the file exists.
   * Parse the file to extract valid rows (using legacy-compatible logic).
   * Count how many valid user-group records exist.
3. Aggregate the count across all farms.
4. Connect to Snowflake.
5. Update a single row in the audit table with:

   * Date
   * Index name
   * Record count (`ES_count`)
   * Status = `"in_progress"`

###  What the Output Looks Like

A row in the Snowflake audit table might look like this after running:

| date       | index\_name     | ES\_count | status       | retry\_count |
| ---------- | --------------- | --------- | ------------ | ------------ |
| 2025-06-04 | my\_index\_name | 134       | in\_progress | 0            |

###  Summary of Key Ideas (Bullet Recap):

* Count only the relevant data block (`Begin UserGroup ... End UserGroup`) that legacy code handled.
* Do not generate or store any files — just count.
* Update the `ES_count` in Snowflake audit table.
* Helps downstream tasks plan execution time and data load windows.
* Mimics legacy parsing rules to ensure consistency.

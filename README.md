## Instructions for Adding Data from New Ad Platforms to MCDM

1. **Source Table Creation**:
   - Ensure the new ad platform's raw data is available in a source table.
   - The source table should include all relevant metrics and identifiers, such as `date`, `ad_id`, `campaign_id`, `clicks`, `impressions`, `spend`, and other metrics.

2. **Update the dbt Model**:
   - Modify the existing dbt model (`vw_paid_ads__basic_performance.sql`) to include a new `UNION ALL` block for the new platform.
   - Use the following template to structure the new `SELECT` statement:
     ```sql
     SELECT 
         CAST(date AS DATE) AS date,
         CAST(metric_1 AS INT64) AS metric_1, -- Replace `metric_1` with the specific metric column
         ...
         CAST(NULL AS INT64) AS [other_columns_not_available]
     FROM {{ ref('src_<new_ad_platform>_all_data') }}
     ```
   - Ensure that each column in the `SELECT` statement matches the schema of the existing model, using `CAST(NULL AS <data_type>)` for any metrics not applicable to the new platform.

3. **Update the YAML File**:
   - Add a description for the new data source in the corresponding YAML file (`vw_paid_ads__basic_performance.yaml`).
   - Document any unique metrics or identifiers introduced by the new platform.

4. **Testing**:
   - Run `dbt run` and `dbt test` to ensure the changes are correctly implemented.
   - Verify that the new platform's data integrates seamlessly into the consolidated model.

5. **Deploy Changes**:
   - Once verified, push changes to your repository and deploy them in your dbt environment.

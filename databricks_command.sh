# HOST
databricks configure --host https://adb-3466462831393837.17.azuredatabricks.net/

# Create Clusters
databricks clusters create --json @create-compute.json --profile DEFAULT

# List Clusters
databricks clusters list --profile DEFAULT

# Delete Clusters
databricks clusters delete <cluster_id>

[DATABRICKS BUNDLE]
# Create project
databricks bundle init 

# Validating
databricks bundle validate

# Deploying
databricks bundle deploy

# Destroy
databricks bundle destroy -t <TARGET>
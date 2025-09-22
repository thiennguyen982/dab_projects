# HOST
databricks configure --host <HOST_ADDRESS>

# Setup shelll
$env:DATABRICKS_HOST=<Host>
$env:DATABRICKS_TOKEN=<Token>
$env:DATABRICKS_AUTH_TYPE='pat' # if needed

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
databricks bundle deploy -t <TARGET> --profile <AUTH_PROFILE> --var catalog=citibike_test --var existing_cluster_id=0709-053523-uls3spa0 --var dev_flag=false

# Destroy
databricks bundle destroy -t <TARGET>
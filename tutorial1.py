from utilities.athena import AthenaDAO


region = "us-east-1"
database = "google_analytics"
bucket = "playwire-analytics"
athena = AthenaDAO(region, database, bucket)

####
# Create a string Query that queries the ga_ramp_daily table in the google
# pass that query to the appropriate function in the AthenaDAO class
# make download true
# show the results of the dataframe

# Import necessary class

# Make a query that answers the following question: what is the average number of users, sessions per country, per date ?

# Download results to your script

# Convert resulting data frame to a csv file

# next we will upload csv file to s3, but let me know when you get to this step

# upload csv file to the following s3 path: s3://playwire-analytics/shuwen-dev/tutorial2/

# use the new AWSS3 class found in the aws_s3.py file in the utilities

# when specificying s3 path for the function: input schuwen-dev/tutorial2/ 
    # leave out the  bucket name

# note that the function takes in a dataframe


import numpy as np
import pandas as pd
import csv
from utilities.athena import AthenaDAO
from utilities.aws_s3 import AWSS3


region = "us-east-1"
database = "google_analytics"
bucket = "playwire-analytics"

# query Athena table
que = """
select countryisocode, date,  avg(users) as average_users, avg(sessions) as average_sessions from ga_ramp_daily
group by countryisocode, date
order by countryisocode, date

limit 1000
"""
athena = AthenaDAO(region, database, bucket)
file = athena.execute(query=que, download=True)

awss3 = AWSS3()
awss3.save_as_csv(df=file, bucket=bucket, path='shuwen-dev/fern_solution_tutorial2/')

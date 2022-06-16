import numpy as np
import pandas as pd
import csv
from athena import AthenaDAO
from AWSS3 import AWSS3


region = "us-east-1"
database = "google_analytics"
bucket = "playwire-analytics"

# query Athena table
que = "SELECT * FROM ga_ramp_daily limit 10"
athena = AthenaDAO(region, database, bucket)
file = athena.execute(query=que, download=True)

# some analysis
ave_user = file['users'].mean()
ave_se_country = file.groupby('countryisocode', as_index=False)['sessions'].mean()
ave_se_date = file.groupby('date', as_index=False)['sessions'].mean()
with open('tutorial2.csv', 'a', newline='') as file:
    writer = csv.writer(file)
    with open('tutorial2.csv','a') as f:
        reader = csv.reader(f)
        writer.writerow(['average_user',ave_user])
        writer.writerow('')
        writer.writerow(['country', 'sessions_ave'])
        for i in range(ave_se_country.shape[0]):
            writer.writerow([np.array(ave_se_country['countryisocode'])[i],np.array(ave_se_country['sessions'])[i]])
        writer.writerow('')
        writer.writerow(['date', 'sessions_ave'])
        for i in range(ave_se_date.shape[0]):
            writer.writerow([np.array(ave_se_date['date'])[i],np.array(ave_se_date['sessions'])[i]])
f.close()

# upload to s3
ana = pd.read_csv('tutorial2.csv')
awss3 = AWSS3()
awss3.save_as_csv(df=ana, path='s3://playwire-analytics/shuwen-dev/tutorial2/')

import datetime
from utilities.athena import AthenaDAO
from utilities.AWSS3 import AWSS3

start_date = datetime.date(2021, 7, 8)
end_date = datetime.date(2021, 7, 14)
gap = datetime.timedelta(days=1)

region = "us-east-1"
database = "triplelift"
bucket = "playwire-analytics"

while start_date <= end_date:
    que = f"""
    SELECT site, AVG(ad_requests) AS ave_ad_requests,
      AVG(impressions) AS ave_impressions,
      AVG(revenue) AS ave_revenue FROM triplelift.triplelift
    WHERE date = date {start_date}
    GROUP BY site
    LIMIT 1000
    """
    athena = AthenaDAO(region, database, bucket)
    file = athena.execute(query=que, download=True)
    start_date += gap

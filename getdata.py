import datetime
import os
from sodapy import Socrata
import pandas as pd

maxDays = 375

startpoint = datetime.date.today() - datetime.timedelta(days=maxDays) 

client = Socrata('data.cdc.gov', app_token=os.getenv('CDC_APP_TOKEN'))
data = client.get('9mfq-cb36', 
    limit=60000, 
    where=f"submission_date > '{startpoint}T00:00:00.000'", 
    select="submission_date, new_death")

covid = pd.DataFrame.from_records(data)

just_death = covid[['submission_date', 'new_death']]
just_death['new_death'] = just_death['new_death'].str.replace(',', '')

just_death['submission_string'] = just_death['submission_date']
just_death['submission_date'] = pd.to_datetime(just_death['submission_date'])
just_death['new_death'] = pd.to_numeric(just_death['new_death'])
just_death = just_death.sort_values(by=['submission_date'])

agg_death = just_death.groupby('submission_date').sum()
agg_death['rolling_7'] = agg_death['new_death'].rolling(7).mean()
agg_death['rolling_30'] = agg_death['new_death'].rolling(30).mean()
agg_death['rolling_90'] = agg_death['new_death'].rolling(90).mean()
agg_death['rolling_365'] = agg_death['new_death'].rolling(365).mean()
agg_death['prev_7'] = agg_death['new_death'].rolling(7).sum()
agg_death['prev_30'] = agg_death['new_death'].rolling(30).sum()
agg_death['prev_90'] = agg_death['new_death'].rolling(90).sum()
agg_death['prev_365'] = agg_death['new_death'].rolling(365).sum()

with open("data/cdc-raw-data.json", "w") as data_dump:
    data_dump.write(just_death.to_json(orient='records'))

with open("data/cdc-processed-data.json", "w") as output:
   output.write(agg_death.to_json(orient='records'))

print(agg_death.sort_values(by=['submission_date'], ascending=False).head(maxDays-365))
import pyspark as ps
from pyspark.sql.functions import udf
from pyspark.sql.types import *

from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.functions import lit

import io
import zipfile
from pprint import pprint

states = {
        'AK': 'Alaska',
        'AL': 'Alabama',
        'AR': 'Arkansas',
        'AS': 'American Samoa',
        'AZ': 'Arizona',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DC': 'District of Columbia',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'GU': 'Guam',
        'HI': 'Hawaii',
        'IA': 'Iowa',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'MA': 'Massachusetts',
        'MD': 'Maryland',
        'ME': 'Maine',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MO': 'Missouri',
        'MP': 'Northern Mariana Islands',
        'MS': 'Mississippi',
        'MT': 'Montana',
        'NA': 'National',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'NE': 'Nebraska',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NV': 'Nevada',
        'NY': 'New York',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'PR': 'Puerto Rico',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VA': 'Virginia',
        'VI': 'Virgin Islands',
        'VT': 'Vermont',
        'WA': 'Washington',
        'WI': 'Wisconsin',
        'WV': 'West Virginia',
        'WY': 'Wyoming'
}

def process(row):
    address = row.NAME
    stateStr = ""
    try:
        splitted = address.split(",")[-1].strip().split(" ")[0]
        try:
            stateStr = states[splitted]
        except:
            pass
    except:
        pass
    return row.DATE, stateStr, row.PRCP, row.TAVG

sc = ps.SparkContext()
sqlContext = SQLContext(sc)

path = "/Users/krishnasharma/Documents/BDA/gsom/*.csv"




df = df = sqlContext.read.load(path,
                          format='com.databricks.spark.csv',
                          header='true',
                          inferSchema='true')

#pprint(df.dtypes)
#pprint(df.count())
#pprint(df.take(5))

df_selected = df.select(df['DATE'], df['NAME'], df['PRCP'], df['TAVG'],df['SNOW'])#, df['SNOW'])
df_selected = df_selected.withColumn("x4", lit(0))
print("-------------------")
pprint(df_selected.show())
rdd = df_selected.rdd.map(process)
df.withColumn
pprint(rdd.collect())

from sqlalchemy import create_engine
import psycopg2
import mysql.connector
import pandas as pd
from faker import Faker
import json


## DATA GENERATION
fake = Faker()


def input_data(x):
    # pandas dataframe
    data = pd.DataFrame()
    for i in range(0, x):
        # data.loc[i, "device_id"] = fake.uuid4()
        data.loc[i, "device_id"] = str(f"{fake.random.randint(1000000, 1000100)}")
        data.loc[i, "temperature"] = fake.random_int(10, 50)
        data.loc[i, "location"] = json.dumps(
            dict(latitude=str(fake.latitude()), longitude=str(fake.longitude()))
        )
        data.loc[i, "time"] = fake.date_time()
    return data


data = input_data(20000).to_dict()
dataFrame = pd.DataFrame.from_dict(data)
print(dataFrame.head())

postgres_db = psycopg2.connect(
    host="localhost", database="postgres", user="postgres", password="Root123$"
)

cursor1 = postgres_db.cursor()
postgres_db.autocommit = True
cursor1 = postgres_db.cursor()
cursor1.execute("DROP TABLE IF EXISTS devices ;")

alchemyEngine = create_engine(
    "postgresql+psycopg2://postgres:Root123$@127.0.0.1/postgres", pool_recycle=3600
)
postgreSQLConnection = alchemyEngine.connect()
postgreSQLTable = "devices"

try:
    frame = dataFrame.to_sql(postgreSQLTable, postgreSQLConnection, if_exists="fail")
except ValueError as vx:
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("PostgreSQL Table %s has been created successfully." % postgreSQLTable)
finally:
    postgreSQLConnection.close()


### TRANSFROM

df = pd.read_sql(
    sql="SELECT DISTINCT device_id, extract(HOUR FROM TIME) hour_, \
                 MAX(temperature) AS max_temp_per_hour, \
                COUNT(device_id) nb_record_per_hour  , \
                ROUND(SUM( sqrt(power(CAST(location::jsonb->>'latitude' AS DECIMAL) \
                        - CAST(location::jsonb->>'longitude' AS DECIMAL),2) \
                    ) * (69.09) ),2) euclidean_distance \
        FROM devices \
        GROUP BY device_id, extract(HOUR FROM TIME) \
        ORDER BY device_id, extract(HOUR FROM TIME) ",
    con=postgres_db,
)
print(df.head())


# -------INSERT INTO MySQL----------------
mysql_db = mysql.connector.connect(
    host="localhost", database="python_test_db", user="root", password="Root123"
)


cursor2 = mysql_db.cursor()
cursor2.execute("DROP TABLE IF EXISTS devices_dm_agg ;")
cursor2.execute(
    "CREATE TABLE devices_dm_agg ( \
                  device_id VARCHAR(255) NOT NULL, \
                  hour_ INT, \
                  max_temp_per_hour INT, \
                  nb_record_per_hour  INT, \
                  euclidean_distance DECIMAL \
            )"
)

# creating column list for insertion
cols = ",".join([str(i) for i in df.columns.tolist()])

# Insert DataFrame recrds one by one.
for i, row in df.iterrows():
    sql = (
        "INSERT INTO devices_dm_agg ("
        + cols
        + ") VALUES ("
        + "%s," * (len(row) - 1)
        + "%s)"
    )
    cursor2.execute(sql, tuple(row))

    # the connection is not autocommitted by default, so we must commit to save our changes
    mysql_db.commit()

    ##

from sqlalchemy import Table, Column, Integer, String, Float, Date, MetaData, ForeignKey
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
import csv
from datetime import date

engine = create_engine('sqlite:///stations.db', echo=True)

meta = MetaData()

stations = Table(
    'stations', meta,
    Column('station', String, primary_key=True),
    Column('latitude', Float),
    Column('longitude', Float),
    Column('elevation', Float),
    Column('name', String),
    Column('country', String),
    Column('state', String),
)


measure = Table(
    'measure', meta,
    Column('station', String),
    Column('date', String),
    Column('precip', Float),
    Column('tobs', Integer),
)


meta.create_all(engine)
# print(engine.table_names())

insert_query_s = stations.insert()

with open('clean_stations.csv', 'r', encoding="utf-8") as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',')
    header = next(csv_reader)
    for row in csv_reader:
        row[1] = float(row[1])
        row[2] = float(row[2])
        row[3] = float(row[3])
        engine.execute(
            insert_query_s,
            [{"station": row[0], "latitude": row[1],
              "longitude": row[2], "elevation": row[3],
              "name": row[4], "country": row[5], "state": row[6]
              }])
        # print(insert_query_s)


insert_query_m = measure.insert()

with open('clean_measure.csv', 'r', encoding="utf-8") as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',')
    header = next(csv_reader)
    count_row = 0
    for row in csv_reader:
        row[2] = float(row[2])
        row[3] = int(row[3])
        count_row += 1
        if count_row <= 20:
            engine.execute(
                insert_query_m,
                [{"station": row[0], "date": row[1],
                  "recip": row[2], "tobs": row[3],
                  }]
            )
        else:
            break

conn = engine.connect()
st = stations.select().limit(5)
results = conn.execute(st)
# results = conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
for r in results:
    print(r)

conn = engine.connect()
m = measure.select().limit(5)
results = conn.execute(m)
for r in results:
    print(m)

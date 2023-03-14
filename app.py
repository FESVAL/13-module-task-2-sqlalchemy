import sqlalchemy as db
import csv
from datetime import datetime

engine = db.create_engine('sqlite:///stations.db', echo=True)
conn = engine.connect()

meta = db.MetaData()

stations = db.Table(
    'stations', meta,
    db.Column('station', db.String, primary_key=True),
    db.Column('latitude', db.Float),
    db.Column('longitude', db.Float),
    db.Column('elevation', db.Float),
    db.Column('name', db.String),
    db.Column('country', db.String),
    db.Column('state', db.String),
)

measure = db.Table(
    'measure', meta,
    db.Column('station', db.String),
    db.Column('date', db.Date),
    db.Column('precip', db.Float),
    db.Column('tobs', db.Integer),
)

meta.create_all(engine)
print(repr(meta.tables['stations']))


with open('clean_stations.csv', 'r', encoding="utf-8") as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',')
    header = next(csv_reader)
    for row in csv_reader:

        query = db.insert(stations).values(
            station=row[0],
            latitude=float(row[1]),
            longitude=float(row[2]),
            elevation=float(row[3]),
            name=row[4],
            country=row[5],
            state=row[6]
        )
        conn.execute(query)
        conn.commit()

measure_data = []
with open('clean_measure.csv', 'r', encoding="utf-8") as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',')
    header = next(csv_reader)
    for ind, row in enumerate(csv_reader):

        d = dict(
            station=row[0],
            date=datetime.strptime(row[1], '%Y-%m-%d'),
            precip=float(row[2]),
            tobs=float(row[3]),
        )
        measure_data.append(d)


query = db.insert(measure)
conn.execute(query, measure_data)
conn.commit()
conn.close()
print(len(measure_data))

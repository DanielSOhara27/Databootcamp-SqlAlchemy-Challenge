# 1. Import Flask.
from flask import Flask, jsonify


# 2. Create an app, being sure to pass __name__
app = Flask(__name__)




# Import SQL_Alchemy libraries
# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

# Import Pandas
import pandas as pd


##########################
##########################

# Preparing Sql Alchemy engine, session, reflection et la
engine = create_engine("sqlite:///../Resources/hawaii.sqlite")
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)
# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station
# Create our session (link) from Python to the DB
session = Session(engine)

##########################
##########################

# This function called `calc_temps` will accept start date and end date in the format '%Y-%m-%d'
# and return the minimum, average, and maximum temperatures for that range of dates
def calc_temps(start_date, end_date):
    """TMIN, TAVG, and TMAX for a list of dates.

    Args:
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d

    Returns:
        TMIN, TAVE, and TMAX
    """



    return session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()


# 3. Define what to do when a user hits the index route
@app.route("/")
def home():
    #print("Server received request for 'Home' page")
    return "<h1>Welcome to my SqlAlchemy Challenge (Homework) home page</h1> <h2>Here are the routes you can use:</h2><br />1) /api/v1.0/precipitation<br />2) /api/v1.0/stations<br />3) /api/v1.0/tobs<br />4) /api/v1.0/<start><br />5) /api/v1.0/<start>/<end>"

# 4. Define what to do when a user hits the /about route
@app.route("/api/v1.0/precipitation")
def precipitation():
    #Convert the query results to a Dictionary using date as the key and prcp as the value.
    #Return the JSON representation of your dictionary.

    # Getting all the precipitation data between 2017-08-23 & 2016-08-23 in a json
    q = session.query(Measurement.date, Measurement.station, Measurement.prcp).filter(Measurement.date <= '2017-08-023', Measurement.date >= '2016-08-23').all()
    dates = list()
    stations = list()
    prcp = list()

    for row in q:
        dates.append(row[0])
        stations.append(row[1])
        prcp.append(row[2])

    df = pd.DataFrame({'date': dates, 'station': stations, 'prcp': prcp})

    print(df.count())

    df.dropna(inplace=True)
    df.sort_values(by=['date'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    print(df.count())

    # df['date'].value_counts()

    dates = df['date'].tolist()
    stations = df['station'].tolist()
    prcp = df['prcp'].tolist()

    myDictionary = dict()

    for x in range(len(dates)):
        if (dates[x] in myDictionary):
            myDictionary[dates[x]][stations[x]] = {'prcp': prcp[x]}
        else:
            myDictionary[dates[x]] = dict()
            myDictionary[dates[x]][stations[x]] = {'prcp': prcp[x]}

    return jsonify(myDictionary)

@app.route("/api/v1.0/stations")
def stations():
    # Return a JSON list of stations from the dataset.

    q = session.query(Station.station).all()

    stations = list()

    for row in q:
        stations.append(row[0])

    myDictionary = {'station':stations}
    return jsonify(myDictionary)

@app.route("/api/v1.0/tobs")
def tobs():

    # query for the dates and temperature observations from a year from the last data point.
    # Return a JSON list of Temperature Observations (tobs) for the previous year.

    # Getting all the precipitation data between 2017-08-23 & 2016-08-23 in a json
    q = session.query(Measurement.date, Measurement.station, Measurement.tobs).filter(Measurement.date <= '2017-08-023', Measurement.date >= '2016-08-23').all()

    dates = list()
    stations = list()
    tobs = list()

    for row in q:
        dates.append(row[0])
        stations.append(row[1])
        tobs.append(row[2])

    df = pd.DataFrame({'date': dates, 'station': stations, 'tobs': tobs})

    print(df.count())

    df.dropna(inplace=True)
    df.sort_values(by=['date'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    print(df.count())

    # df['date'].value_counts()

    dates = df['date'].tolist()
    stations = df['station'].tolist()
    tobs = df['tobs'].tolist()

    myDictionary = dict()

    for x in range(len(dates)):
        if (dates[x] in myDictionary):
            myDictionary[dates[x]][stations[x]] = {'tobs': tobs[x]}
        else:
            myDictionary[dates[x]] = dict()
            myDictionary[dates[x]][stations[x]] = {'tobs': tobs[x]}

    return jsonify(myDictionary)

@app.route("/api/v1.0/<start>")
def temps1(start):

    # Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range.
    # When given the start only, calculate TMIN, TAVG, and TMAX for all dates greater than and equal to the start date.
    # When given the start and the end date, calculate the TMIN, TAVG, and TMAX for dates between the start and end date inclusive.

    latest_date = (session.query(Measurement.date).order_by(Measurement.date.desc()).first())[0]

    if(start == latest_date):
        return jsonify({ 'Error' : "Choose an earlier date, This function won't work if you choose a non existent date" } )

    checkStart = start.split('-')
    checkEnd = latest_date.split('-')

    if (int(checkStart[0]) <= int(checkEnd[0])):
        if (int(checkStart[1]) <= int(checkEnd[1])):
            if (int(checkStart[2]) < int(checkEnd[2])):
                temps = calc_temps(start, latest_date)[0]
                myDictionary = {"start_date": start, "end_date": latest_date, "TMIN": temps[0], "TAVG": temps[1],"TMAX": temps[2]}

                return jsonify(myDictionary)

    return jsonify({ 'Error': 'The End date has to after the start date' })

@app.route("/api/v1.0/<start>/<end>")
def temps(start, end):


    # Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range.
    # When given the start only, calculate TMIN, TAVG, and TMAX for all dates greater than and equal to the start date.
    # When given the start and the end date, calculate the TMIN, TAVG, and TMAX for dates between the start and end date inclusive.

    checkStart = start.split('-')
    checkEnd = end.split('-')

    if (int(checkStart[0]) <= int(checkEnd[0])):
        if (int(checkStart[1]) <= int(checkEnd[1])):
            if (int(checkStart[2]) < int(checkEnd[2])):
                temps = calc_temps(start, end)[0]
                myDictionary = {"start_date": start, "end_date": end, "TMIN": temps[0], "TAVG": temps[1],"TMAX": temps[2]}

                return jsonify(myDictionary)

    return jsonify({ 'Error': 'The End date has to after the start date' })


if __name__ == "__main__":
    app.run(debug=True)



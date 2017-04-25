from flask import Flask, render_template, request, redirect
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
import platform


app = Flask(__name__)
if platform.system() == 'Windows':
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:password@192.168.1.3:5432/radar'

else:
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:password@localhost:5432/radar'

db = SQLAlchemy(app)


class Radar(db.Model):
    __tablename__ = 'radar'
    id = db.Column(db.Integer, primary_key=True)
    squawk = db.Column(db.String(10))
    flight = db.Column(db.String(10))
    altitude = db.Column(db.String(10))
    lat = db.Column(db.String(10))
    lon = db.Column(db.String(10))
    timer = db.Column(db.String(20))


header = ['ID', 'FLIGHT', 'ALTITUDE', 'SQUAWK', 'LAT', 'LON', 'DATETIME']


@app.route('/')
def hello_world():

    return render_template('layout.html')


@app.route('/flight/')
def flight_search():
    flight = request.args.get('flight')
    if flight is None or len(flight) != 0:
            query = Radar.query.filter_by(flight=func.upper(flight))
            return render_template('flight.html', query=query, header=header)
    else:
        return render_template('flight.html', header=header)


@app.route('/squawk/')
def squawk_search():
    squawk = request.args.get("squawk")
    query = Radar.query.filter_by(squawk=func.upper(squawk))
    return render_template('squawk.html', query=query, header=header)


@app.route('/recent/')
def recent():
    # query = Radar.query.order_by(Radar.id.desc()).limit(10)
    query = Radar.query.order_by('id desc').limit(30)
    # query = Radar.query.distinct(Radar.flight).limit(10).order_by(Radar.id.desc())
    # print render_template('recent.html', query=query, header=header)
    return render_template('recent.html', query=query, header=header)


@app.route('/all/')
def layer_search():
    search = request.args.get('search')
    if search is None:
        return render_template('flight.html', header=header)
    else:
        query = Radar.query.filter(Radar.id.desc('%' + search + '%'))
        return render_template('flight.html', query=query, header=header)


if __name__ == '__main__':

    if platform.system() == 'Windows':
        app.run(debug=True)
    else:
        app.run(debug=False, host='0.0.0.0', processes=4)

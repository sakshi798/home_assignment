from flask import *
from flask_mysqldb import MySQL
import csv, time, os, threading
from generate_report import generate_report
from os import environ

app = Flask(__name__)

app.config['MYSQL_HOST'] = environ.get('HOST')
app.config['MYSQL_USER'] = environ.get('USER')
app.config['MYSQL_PASSWORD'] = environ.get('PASSWORD')
app.config['MYSQL_DB'] = environ.get('DATABASE')

mysql = MySQL(app)

@app.route("/trigger_report", methods = ['GET','POST'])
def trigger_report():
    
    if request.method == 'POST':
        stored_files = ['store_status.csv', 'business_hours.csv', 'timezone.csv']
        for i in stored_files:
            if os.path.exists(i):
                os.remove(i)
        
        request.files['store_status'].save('store_status.csv')
        request.files['business_hours'].save('business_hours.csv')
        request.files['timezone'].save('timezone.csv')
        
        millisec = int(round(time.time() * 1000))
        cur = mysql.connection.cursor()
        cur.execute('insert into report_status values (%s, %s)', (millisec, 0))
        mysql.connection.commit()
        cur.close()

        t1 = threading.Thread(target=generate_report, args=([millisec],))
        t1.start()

        return render_template('trigger_report_return.html', millisec=millisec, error='false')
        
    if request.method == 'GET':
        return render_template('trigger_report_form.html')
    

@app.route("/get_report", methods = ['GET','POST'])
def get_report():
    if request.method == 'GET':
        return render_template('get_report_form.html')
    if request.method == 'POST':
        report_id = request.form['report_id']

        cur = mysql.connection.cursor()
        cur.execute('select isCompleted from report_status where report_id=(%s)', (report_id,))
        status = cur.fetchone()
        mysql.connection.commit()
        cur.close()

        if status[0]==1:
            return send_file('final_report.csv', as_attachment=True)
        return render_template('get_report_form.html', status=status[0])

if __name__ == "__main__":
    app.run(debug=True)
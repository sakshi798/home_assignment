def max_time():
    import mysql.connector
    from os import environ
    cnx = mysql.connector.connect(user=environ.get('USER'), password=environ.get('PASSWORD'), host=environ.get('HOST'), database=environ.get('DATABASE'))
    cur = cnx.cursor()
    cur.execute('SELECT MAX(timestamp_utc) FROM store_status')
    max_time = cur.fetchone()
    cur.nextset()
    cur.execute('SELECT DISTINCT store_id FROM store_status')
    distinct_store =  cur.fetchall()
    cur.close()
    return max_time, distinct_store

def convert_time(time_stamp, zone1, zone2):
    from pytz import timezone

    time_zone1 = timezone(zone1)
    time_zone2 = timezone(zone2)

    pre_time = time_zone1.localize(time_stamp)
    new_time = pre_time.astimezone(time_zone2)
    
    return new_time

def generate_uptime():
    import mysql.connector, datetime, tqdm, csv, os
    from os import environ
    cur_time, stores = max_time()
    cur_time = cur_time[0]

    if os.path.exists('final_report.csv'):
        os.remove('final_report.csv')

    with open('final_report.csv', 'w', newline='') as csvfile: 
        csvwriter = csv.writer(csvfile) 
        csvwriter.writerow(['store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week', 'downtime_last_hour', 'downtime_last_day', 'downtime_last_week']) 
            
        
        for i in tqdm.tqdm(stores[:50]):
            i = i[0]
            
            cnx = mysql.connector.connect(user=environ.get('USER'), password=environ.get('PASSWORD'), host=environ.get('HOST'), database=environ.get('DATABASE'))
            cur = cnx.cursor()
            cur.execute('SELECT timezone FROM timezone WHERE store_id = (%s)', (i,))
            timezone = cur.fetchall()
            if len(timezone)==0:
                timezone = [('America/Chicago',)]
            timezone = timezone[0][0]
            
            cur.nextset()
            cur.execute('SELECT timestamp_utc, status_ FROM store_status WHERE store_id = (%s) AND DATEDIFF(timestamp_utc, %s)<9', (i,cur_time))
            status_data = cur.fetchall()
            
            cur.nextset()
            cur.execute('SELECT dayOfWeek, start_time_local, end_time_local FROM business_hours WHERE store_id = (%s)', (i,))
            business_hours_data = cur.fetchall()
            cur.close()
            cnx.close()

            if len(business_hours_data)==0:
                business_hours_data = [ (0, datetime.timedelta(seconds=0), datetime.timedelta(seconds=86399)),\
                                        (1, datetime.timedelta(seconds=0), datetime.timedelta(seconds=86399)),\
                                        (4, datetime.timedelta(seconds=0), datetime.timedelta(seconds=86399)), \
                                        (3, datetime.timedelta(seconds=0), datetime.timedelta(seconds=86399)), \
                                        (5, datetime.timedelta(seconds=0), datetime.timedelta(seconds=86399)), \
                                        (6, datetime.timedelta(seconds=0), datetime.timedelta(seconds=86399)), \
                                        (2, datetime.timedelta(seconds=0), datetime.timedelta(seconds=86399))]

            working = {
                0 : [],
                1 : [],
                2 : [],
                3 : [],
                4 : [],
                5 : [],
                6 : [],
                7 : []
            }
            
            uptime_last_hour = 0
            uptime_last_day  = 0
            uptime_last_week = 0
            downtime_last_hour = 0
            downtime_last_day  = 0
            downtime_last_week = 0

            for j in business_hours_data:
                working[j[0]].append([j[1],j[2],1])
                if j[0]==cur_time.weekday():
                    working[7].append([j[1],j[2],1])

            for j in status_data:
                diff = cur_time - j[0]
                temp_time = convert_time(j[0],'UTC',timezone)
                day = temp_time.weekday()
                temp_time = datetime.timedelta(hours=temp_time.time().hour, minutes=temp_time.time().minute, seconds=temp_time.time().second)
                if diff.days==0:
                    for k in range(len(working[7])):
                        temp = working[7][k]
                        start = temp[0]
                        end   = temp[1]
                        if start < temp_time or temp_time < end:
                            working[7][k][1] = temp_time
                            working[7].append([temp_time,end, 1 if j[1] == 'active' else 0])
                            break
                    continue
                for k in range(len(working[day])):
                    temp = working[day][k]
                    start = temp[0]
                    end   = temp[1]
                    if start < temp_time or temp_time < end:
                        working[day][k][1] = temp_time
                        working[day].append([temp_time,end, 1 if j[1] == 'active' else 0])
                        break

            day = cur_time.weekday()

            for j in working[(day-1+7)%7]:
                diff = j[1]-j[0]
                if j[2]:
                    uptime_last_day += diff.total_seconds()
                else:
                    downtime_last_day += diff.total_seconds()

            for j in range(7):
                for k in working[j]:
                    diff = k[1]-k[0]
                    if k[2]:
                        uptime_last_week += diff.total_seconds()
                    else:
                        downtime_last_week += diff.total_seconds()

            cur_time1 = convert_time(cur_time,'UTC',timezone)
            cur_time1 = datetime.timedelta(hours=cur_time1.time().hour, minutes=cur_time1.time().minute, seconds=cur_time1.time().second)
            for j in working[7]:
                diff1 = cur_time1 - j[0]
                diff2 = cur_time1 - j[1]

                x = 0
                if cur_time1>=j[0] and cur_time1<=j[1]:
                    x += min(3600,diff1.total_seconds())
                if cur_time1>=j[0] and cur_time1>=j[1]:
                    x += min(max(0,3600-diff2.total_seconds()),diff1.total_seconds()-diff1.total_seconds())
                
                if j[2]:
                    uptime_last_hour += x
                else:
                    downtime_last_hour += x

            uptime_last_hour   = int(uptime_last_hour/6)/10
            uptime_last_day    = int(uptime_last_day/360)/10
            uptime_last_week   = int(uptime_last_week/360)/10
            downtime_last_hour = int(downtime_last_hour/6)/10
            downtime_last_day  = int(downtime_last_day/360)/10
            downtime_last_week = int(downtime_last_week/360)/10
            
            csvwriter.writerow([i,uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week])

def generate_report(token):
    import mysql.connector
    import time, csv, pytz
    from pytz import timezone
    from os import environ

    def compile_store_status(cnx):
        print("Compiling store status")
        try:
            data = []
            with open("store_status.csv", 'r') as file:
                csvreader = csv.reader(file)
                header = next(csvreader)
                for row in csvreader:
                    temp = row[2]
                    row[2] = row[1]
                    row[1] = temp

                    row[1] = row[1][:-4]
                    data.append(tuple(row))

            data = tuple(data)
            cur = cnx.cursor()
            
            cur.execute('TRUNCATE TABLE store_status')
            cnx.commit()

            cur.execute('SET GLOBAL max_allowed_packet=1073741824')
            cnx.commit()

            cur.executemany('INSERT INTO store_status VALUES (%s, %s, %s)', data)
            cnx.commit()
            cur.close()
            return True
        except Exception as e:
            print(e)
            return False

    def compile_business_hours(cnx):
        print("Compiling business hours")
        try:
            data = []
            with open("business_hours.csv", 'r') as file:
                csvreader = csv.reader(file)
                header = next(csvreader)
                for row in csvreader:
                    data.append(tuple(row))

            data = tuple(data)
            cur = cnx.cursor()
            
            cur.execute('TRUNCATE TABLE business_hours')
            cnx.commit()
            
            cur.executemany('INSERT INTO business_hours VALUES (%s, %s, %s, %s)', data)
            cnx.commit()
            cur.close()
            return True
        except:
            return False

    def compile_timezone(cnx):
        print("Compiling time zone")
        try:
            data = []
            with open("timezone.csv", 'r') as file:
                csvreader = csv.reader(file)
                header = next(csvreader)
                for row in csvreader:
                    data.append(tuple(row))

            data = tuple(data)
            cur = cnx.cursor()
            
            cur.execute('TRUNCATE TABLE timezone')
            cnx.commit()
            
            cur.executemany('INSERT INTO timezone VALUES (%s, %s)', data)
            cnx.commit()
            cur.close()
            return True
        except:
            return False


    token = token[0]
    
    
    cnx = mysql.connector.connect(user=environ.get('USER'), password=environ.get('PASSWORD'), host=environ.get('HOST'), database=environ.get('DATABASE'))
    compile_timezone(cnx)
    compile_business_hours(cnx)
    compile_store_status(cnx)
    print("Input files compiled successfully.")
    cnx.close()

    generate_uptime()
    
    cnx = mysql.connector.connect(user=environ.get('USER'), password=environ.get('PASSWORD'), host=environ.get('HOST'), database=environ.get('DATABASE'))
    cur = cnx.cursor()
    cur.execute('UPDATE report_status SET isCompleted=1 WHERE report_id=%s', (token,))
    cnx.commit()
    cur.close()

    cnx.close()
    return

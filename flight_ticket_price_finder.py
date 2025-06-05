import os
import psycopg2
import requests
import datetime
import time
import logging
nr_of_adults = 1
search_results = 5
min_nights = 4
max_nights = 9
one_way = 0  # set to 1 to enable docs: https://tequila.kiwi.com/portal/docs/tequila_api/search_api
source = None
destination = None
max_price_eur = 0
reached_else = 0
reached_last_elif = 0

logging.basicConfig(format='%(asctime)s %(message)s', filename='flights.log',)

url = f'https://api.tequila.kiwi.com/v2/search?fly_from=city%3A{source}&fly_to=city%3A{destination}&date_from=09%2F06%2F2025&date_to=30%2F11%2F2026&nights_in_dst_from={min_nights}&nights_in_dst_to={max_nights}&max_fly_duration=20&ret_from_diff_city=false&ret_to_diff_city=false&one_for_city={one_way}&one_per_date=0&adults={nr_of_adults}&children=0&only_working_days=false&only_weekends=false&partner_market=de&curr=EUR&price_to={max_price_eur}&max_stopovers=2&max_sector_stopovers=2&limit={search_results}'

headers = {'accept': 'application/json', "apikey": os.environ['tequila_api_key']}


conn = psycopg2.connect(database="flight_prices",
                        user="ps_admin_user",
                        host="db.cluster.home",
                        password=os.environ['psql_pw'],
                        port=5432,)

cur = conn.cursor()

logging.warning('Connected to db.cluster.home')

cur.execute("""select * from flights;""")
data = cur.fetchall()

cur.execute("""select price,seats_left,departure_time,arrival_time, source, destination from result;""")
data_in_db = cur.fetchall()

cur.execute("""select departure_time from result;""")
dep_time = cur.fetchall()

cur.execute("""select arrival_time from result;""")
arr_time = cur.fetchall()

for row in data:
    id = row[0]
    source = row[1]
    destination = row[2]
    isroundtrip = row[3]
    max_price_eur = row[4]
    url = f'https://api.tequila.kiwi.com/v2/search?fly_from=city%3A{source}&fly_to=city%3A{destination}&date_from=09%2F06%2F2025&date_to=30%2F11%2F2026&nights_in_dst_from={min_nights}&nights_in_dst_to={max_nights}&max_fly_duration=20&ret_from_diff_city=false&ret_to_diff_city=false&one_for_city={one_way}&one_per_date=0&adults={nr_of_adults}&children=0&only_working_days=false&only_weekends=false&partner_market=de&curr=EUR&price_to={max_price_eur}&max_stopovers=2&max_sector_stopovers=2&limit={search_results}'

    response = requests.get(url, headers=headers)
    data = response.json()
    for i in data["data"]:
        source = i["cityFrom"]
        destination = i["cityTo"]
        price = i["price"]
        carrier = i["airlines"]
        fly_in_date = i["route"][0]['local_departure']
        fly_back_date = i["route"][1]['local_departure']
        seats_left = i["availability"]['seats']
        link = i["deep_link"]
        fly_back_datetime = datetime.datetime.strptime(fly_back_date, f'%Y-%m-%dT%H:%M:%S.%fZ')
        fly_in_datetime = datetime.datetime.strptime(fly_in_date, f'%Y-%m-%dT%H:%M:%S.%fZ')
        days = (fly_back_datetime-fly_in_datetime).days
        # id_to_use += 1
        if not data_in_db:
            logging.warning("adding new data to empty db")
            cur.execute("""INSERT INTO result (price, source, destination, days, link, seats_left, departure_time, arrival_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """, (price, source, destination, days, link, seats_left, fly_in_date, fly_back_date))
        else:
            for row in data_in_db:
                if (row[4] == source) and (row[5] == destination):
                    if (row[2] == fly_in_datetime) and (row[3] == fly_back_datetime) and (seats_left == row[1]) and (price == row[0]):
                        break
                    elif (row[2] == fly_in_datetime) and (row[3] == fly_back_datetime) and (price != row[0]) and (seats_left != row[1]):
                        cur.execute(
                            f"UPDATE result SET seats_left = %s WHERE departure_time = %s AND arrival_time= %s", (seats_left, fly_in_date, fly_back_date,))
                        cur.execute(
                            f"UPDATE result SET price = %s WHERE departure_time = %s AND arrival_time= %s", (price, fly_in_date, fly_back_date,))
                    elif (row[2] == fly_in_datetime) and (row[3] == fly_back_datetime) and (price == row[0]) and (seats_left != row[1]):
                        cur.execute(
                            f"UPDATE result SET seats_left = %s WHERE departure_time = %s AND arrival_time= %s", (seats_left, fly_in_date, fly_back_date,))
                    elif (row[2] == fly_in_datetime) and (row[3] == fly_back_datetime) and (price != row[0]) and (seats_left == row[1]):
                        cur.execute(
                            f"UPDATE result SET price = %s WHERE departure_time = %s AND arrival_time= %s", (price, fly_in_date, fly_back_date,))
                    elif ((fly_in_datetime,) not in dep_time) and (((fly_back_datetime,) not in dep_time)):
                        logging.warning("adding new flights to already existing data in db")
                        cur.execute("""INSERT INTO result (price, source, destination, days, link, seats_left, departure_time, arrival_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                        """, (price, source, destination, days, link, seats_left, fly_in_date, fly_back_date))
                        break
                    elif ((fly_in_datetime,) in dep_time) and (((fly_back_datetime,) in dep_time)):
                        break
                    else:
                        break

time.sleep(2)

cur.close()
conn.commit()
conn.close()
logging.warning("PostgreSQL connection is closed")

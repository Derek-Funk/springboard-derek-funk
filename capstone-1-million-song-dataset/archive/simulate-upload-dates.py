# https://www.w3schools.com/python/python_datetime.asp
import datetime
import numpy as np
import random

# local_time = datetime.datetime.now()
# utc_time = datetime.datetime.utcnow()
#
# local_time.strftime('%Y-%m-%d %H:%M:%S' + ' (CST)')
# utc_time.strftime('%Y-%m-%d %H:%M:%S' + ' (UTC)')
#
# utc_time + datetime.timedelta(days=1)

timestamp_first = datetime.datetime(2010, 1, 1)
timestamp_mid = datetime.datetime(2020, 12, 20)
timestamp_last = datetime.datetime(2021, 2, 20)

no_songs = 10000
no_songs_half_one = 5000
no_songs_half_two = 5000

upload_dates = np.array([])

# first half
duration_half_one = (timestamp_mid - timestamp_first).days
for _ in range(no_songs_half_one):
    day_diff = random.randint(0, duration_half_one)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    new_timestamp = timestamp_first + datetime.timedelta(days=day_diff, hours=hour, minutes=minute, seconds=second)
    upload_dates = np.append(upload_dates, new_timestamp)

# second half
duration_half_two = (timestamp_last - timestamp_mid).days
for _ in range(no_songs_half_two):
    day_diff = random.randint(0, duration_half_two)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    new_timestamp = timestamp_mid + datetime.timedelta(days=day_diff, hours=hour, minutes=minute, seconds=second)
    upload_dates = np.append(upload_dates, new_timestamp)
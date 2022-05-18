import numpy as np
from datetime import timedelta, datetime

import pymongo
from pymongo import MongoClient
import datetime as DT
import time
import random


def dbConnect():
    client = pymongo.MongoClient("mongodb://HotDog:HotDog@cluster0-shard-00-00.9q7j7.mongodb.net:27017,cluster0-shard-00-01.9q7j7.mongodb.net:27017,cluster0-shard-00-02.9q7j7.mongodb.net:27017/dogs?ssl=true&replicaSet=atlas-7anlxw-shard-0&authSource=admin&retryWrites=true&w=majority")

    db_hotDog = client.dogs
    return db_hotDog




def dogTempDaily(db_hotDog, dogId):
    dt_now = DT.datetime.now().replace(minute=0, second=0, microsecond=0)
    temp_avg = getDogPulseInfo(db_hotDog, dogId,dt_now)
    doc = {
        "date_created": dt_now,
        'dog_id': int(dogId),
        'temp_daily_avg': temp_avg,
    }

    # db_hotDog.dog_dist_every_minute.insert_one(doc)

def getDogTempInfo(db_hotDog, dogId, dt_now):
    temp=0
    dt_day_before = (datetime.now()).replace(day=dt_now.day-1, hour=0, minute=0, second=0, microsecond=0)
    dogAgg = db_hotDog.basic_tag_feeding_y2.aggregate([
        {'$match': {
                    'dog_id': dogId,
                    'date_created': {'$gte': dt_now, '$lt': dt_day_before},
                    }},
        {'$group':
            {'_id': {
                'dog_id': "dog_id",
                "date_created": "date_created"

            },
                "temp_avg": {"$avg": "temp"}
            }},

    ])
    for i in dogAgg:
        temp = i['temp_avg']
    return temp



def dogDistDaily(db_hotDog, dogId):
    dt_now = DT.datetime.now().replace(minute=0, second=0, microsecond=0)
    walking_min = getDogDistInfo(db_hotDog, dogId,dt_now)
    doc = {
        "date_created": dt_now,
        'dog_id': int(dogId),
        'walking_hours': walking_min/60,
    }

    # db_hotDog.dog_dist_every_minute.insert_one(doc)

def getDogDistInfo(db_hotDog, dogId, dt_now):
    distMet=0
    dt_day_before = (datetime.now()).replace(day=dt_now.day-1, hour=0, minute=0, second=0, microsecond=0)
    dogAgg = db_hotDog.basic_tag_feeding_y2.aggregate([
        {'$match': {
                    'dog_id': dogId,
                    'date_created': {'$gte': dt_now, '$lt': dt_day_before},
                    'dog_active_status':'active'
                    }},
        {'$group':
            {'_id': {
                'dog_id': "dog_id",
                "date_created": "date_created"

            },
                "walking_met": {"$sum": "$walking_met"}
            }},

    ])
    for i in dogAgg:
        distMet = i['walking_met']
    return distMet



def dogPulseDaily(db_hotDog, dogId):
    dt_now = DT.datetime.now().replace(minute=0, second=0, microsecond=0)
    pulse_avg = getDogPulseInfo(db_hotDog, dogId,dt_now)
    doc = {
        "date_created": dt_now,
        'dog_id': int(dogId),
        'pulse_daily_avg': pulse_avg,
    }

    # db_hotDog.dog_dist_every_minute.insert_one(doc)

def getDogPulseInfo(db_hotDog, dogId, dt_now):
    pulse=0
    dt_before_hour = (datetime.now()).replace(hour=dt_now.hour-1, minute=0, second=0, microsecond=0)
    dogAgg = db_hotDog.basic_tag_feeding_y2.aggregate([
        {'$match': {
                    'dog_id': dogId,
                    'date_created': {'$gte': dt_now, '$lt': dt_before_hour},
                    }},
        {'$group':
            {'_id': {
                'dog_id': "dog_id",
                "date_created": "date_created"

            },
                "pulse_avg": {"$avg": "$pulse"}
            }},

    ])
    for i in dogAgg:
        pulse = i['pulse_avg']
    return pulse





def main():
    print("main")
    # db_hotDog = dbConnect()


if __name__ == "__main__":
    db_hotDog = dbConnect()
    main()
    # dogs = db_hotDog.dogs_info.distinct("_id")
    while True:
        now = DT.datetime.now()

        if now.hour == 0:
            # for dog in dogs:


            time.sleep(86000)




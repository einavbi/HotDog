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




def dogTempHourly(db_hotDog, dogId):
    dt_now = DT.datetime.now().replace(minute=0, second=0, microsecond=0)
    temp_avg = getDogPulseInfo(db_hotDog, dogId,dt_now)
    doc = {
        "date_created": dt_now,
        'dog_id': dogId,
        'temp_hourly_avg': temp_avg,
    }

    db_hotDog.dog_temp_hourly.insert_one(doc)

def getDogTempInfo(db_hotDog, dogId, dt_now):
    temp=0
    dt_before_hour = (datetime.now()+ timedelta(hours=-1)).replace( minute=0, second=0, microsecond=0)
    dogAgg = db_hotDog.dog_temp_every_minute.aggregate([
        {'$match': {
                    'dog_id': dogId,
                    'date_created': {'$gte': dt_before_hour, '$lte': dt_now},
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



def dogDistHourly(db_hotDog, dogId):
    dt_now = DT.datetime.now().replace(minute=0, second=0, microsecond=0)
    walking_met = getDogDistInfo(db_hotDog, dogId,dt_now)
    doc = {
        "date_created": dt_now,
        'dog_id': dogId,
        'walking_met': walking_met,
    }

    db_hotDog.dog_dist_hourly.insert_one(doc)

def getDogDistInfo(db_hotDog, dogId, dt_now):
    distMet=0
    dt_before_hour = (datetime.now()+ timedelta(hours=-1)).replace( minute=0, second=0, microsecond=0)
    dogAgg = db_hotDog.dog_dist_every_minute.aggregate([
        {'$match': {
                    'dog_id': dogId,
                    'date_created': {'$gte': dt_before_hour, '$lte': dt_now},
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



def dogPulseHourly(db_hotDog, dogId):
    dt_now = DT.datetime.now().replace(minute=0, second=0, microsecond=0)
    pulse_avg = getDogPulseInfo(db_hotDog, dogId,dt_now)
    doc = {
        "date_created": dt_now,
        'dog_id': dogId,
        'pulse_hourly_avg': pulse_avg,
    }

    db_hotDog.dog_pulse_hourly.insert_one(doc)

def getDogPulseInfo(db_hotDog, dogId, dt_now):
    pulse=0
    dt_before_hour = (datetime.now()+ timedelta(hours=-1)).replace( minute=0, second=0, microsecond=0)
    dogAgg = db_hotDog.dog_pulse_every_minute.aggregate([
        {'$match': {
                    'dog_id': dogId,
                    'date_created': {'$gte': dt_before_hour, '$lte': dt_now},
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
    db_hotDog = dbConnect()
    dogs = db_hotDog.dogs_info.distinct("_id")
    while True:
        now = DT.datetime.now()

        if now.minute == 0:
            for dog in dogs:
                dogDistHourly(db_hotDog, dog)
                dogPulseHourly(db_hotDog, dog)
                dogTempHourly(db_hotDog, dog)
            time.sleep(3600)


if __name__ == "__main__":
    main()
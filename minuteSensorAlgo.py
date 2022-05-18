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


def dogTempEveryMinute(db_hotDog, dogId):
    batt_level_startRange=1
    batt_level_endRange=10
    startRange = 37.8
    endRange = 39.5

    doc = {
        "date_created": DT.datetime.now().replace(second=0, microsecond=0),
        'temp' : round(random.uniform(startRange, endRange), 2),
        'dog_id' : dogId,
        'version' : '1.0',
        'batt_level' :round(random.uniform(batt_level_startRange, batt_level_endRange), 2),
        'pkt_type' : 'T'
    }


    db_hotDog.dog_temp_every_minute.insert_one(doc)

def dogDistEveryMinute(db_hotDog, dogId):
    dtNow=DT.datetime.now()
    options = ["rest", "active"]
    walking_met = 0
    walking_met_startRange = 0
    walking_met_endRange = 15
    dogStatus=(random.choices(options, weights=[7, 3], k=1))
    if(dtNow.hour>=23 & dtNow.hour<=6):
        dogStatus[0] = 'rest'
    if dogStatus[0] == 'active':
        walking_met = round(random.uniform(walking_met_startRange, walking_met_endRange), 2)
    batt_level_startRange=1
    batt_level_endRange=10


    doc = {
        "date_created": dtNow.replace(second=0, microsecond=0),
        'dog_active_status' : dogStatus[0],
        'dog_id' : dogId,
        'walking_met' :walking_met,
        'version' : '1.0',
        'batt_level' :round(random.uniform(batt_level_startRange, batt_level_endRange), 2),
        'pkt_type' : 'D'
    }
    print(doc)


    db_hotDog.dog_dist_every_minute.insert_one(doc)

def dogPulseEveryMinute(db_hotDog, dogId):
    dogSize, dogAge, pulse = 0, 0, 0
    dogInfo = db_hotDog.dogs_info.find({"_id":dogId})
    for i in dogInfo:
        dogSize=i['dog_size']
        dogAge=i['dog_age']


    if ((dogAge>=9 )| (dogSize == 'big')):
        pulse_startRange = 60
        pulse_endRange = 140
    else:
        pulse_startRange = 100
        pulse_endRange = 180

    pulse = round(random.uniform(pulse_startRange, pulse_endRange), 2)
    batt_level_startRange=1
    batt_level_endRange=10


    doc = {
        "date_created": DT.datetime.now().replace(second=0, microsecond=0),
        'dog_id' : dogId,
        'pulse' :pulse,
        'version' : '1.0',
        'batt_level' :round(random.uniform(batt_level_startRange, batt_level_endRange), 2),
        'pkt_type' : 'P'
    }
    print(doc)


    db_hotDog.dog_pulse_every_minute.insert_one(doc)




def main():
    print("main")
    db_hotDog = dbConnect()
    dogs = db_hotDog.dogs_info.distinct("_id")
    while True:
        now = DT.datetime.now()
        if now.second == 0:
            for dog in dogs:
                dogDistEveryMinute(db_hotDog, dog)
                dogPulseEveryMinute(db_hotDog, dog)
                dogTempEveryMinute(db_hotDog, dog)
            time.sleep(60)








if __name__ == "__main__":
    main()

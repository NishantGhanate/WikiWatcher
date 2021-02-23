import os
import sys
import json
import time
import copy
import asyncio
import aiohttp
import traceback
# import logging
from datetime import datetime, timedelta
from collections import deque
from aiosseclient import aiosseclient

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
WIKI_REPORT_PATH =  os.path.join(SCRIPT_DIR,"WikiReports")

PREV_TIME = datetime.now()
CURRENT_TIME = datetime.now()

if not os.path.exists(WIKI_REPORT_PATH):
    os.makedirs(WIKI_REPORT_PATH)

TIME_WINDOW = deque()
MIN_5_REPORT = {} 
COUNTER = 0 
EVENT_DATA = []

def filter_report(json_data):
    """
    Please readme.md for filtered data structure
    """
    maps = {}
    try:
        for data in json_data:
            performer = data['performer']
            user_edit_count = 0
            if not data['performer']['user_is_bot']:
                domain = data['meta']['domain']
                if domain not in maps :
                    performer = data['performer']
                    user_text =  performer['user_text'] 
                    
                    if 'user_edit_count' in performer:
                        user_edit_count = performer['user_edit_count'] 
                    temp = {
                        'users' :   {
                            user_text :  user_edit_count,
                        },
                        'page_titles' : { data['page_title'] },
                        'page_count' : 1
                    }
                    maps[data['meta']['domain']] = temp
                else:
                    
                    maps[domain]['page_titles'].add(data['page_title']) 

                    performer = data['performer']
                    user_text = performer['user_text']
                    if 'user_edit_count' in performer:
                        user_edit_count = performer['user_edit_count']

                    users_list = maps[domain]['users']
                    if user_text in users_list:
                        edit_count = maps[domain]['users'][user_text] 
                        if user_edit_count > edit_count:
                            maps[domain]['users'][user_text]  = user_edit_count     
                    else:
                        maps[domain]['users'][user_text] = user_edit_count

                maps[domain]['page_count'] = len(maps[domain]['page_titles'])

        return maps

    except Exception as e:
        print("\n Exception occured in filter_report : {}".format(e))
        sys.exit(1)
    
def sort_report(json_data,page_rsort=True,user_rsort=True):
    keys = dict(sorted(json_data.items(), key=lambda x: (x[1]['page_count']), reverse=page_rsort))   
    for k in keys:
        users_list = json_data[k]['users']
        json_data[k]['users'] = dict( sorted(users_list.items(), key= lambda x: x[1], reverse=user_rsort) )
    return keys

def crunch_report():
    """
    For sliding window , here compressing the list of data
    collected over 5 mintue to one single dict
    """
    global TIME_WINDOW
    global MIN_5_REPORT

    try :
        for window in TIME_WINDOW:
            for domain in window:
                if domain not in MIN_5_REPORT:
                    MIN_5_REPORT[domain] = window[domain]
                else:
                    # If domain & user exists update their values
                    # or add new users in that domain
                    page_titles = window[domain]['page_titles']
                    for title in page_titles:
                        MIN_5_REPORT[domain]['page_titles'].add(title)
                        # print(type(MIN_5_REPORT[domain]['page_titles']))
                    MIN_5_REPORT[domain]['page_count'] = len(MIN_5_REPORT[domain]['page_titles'])

                    users_list = window[domain]['users']
                    for user in users_list:
                        MIN_5_REPORT[domain]['users'][user] = users_list[user]
                          
    except Exception as e :
        print("\nException occured in crunch report {}".format(e))
        print(traceback.print_exception)
        sys.exit()

def re_calucate(json_remove,json_add):
    global MIN_5_REPORT

    try :
        for domain in json_remove:
            if domain in MIN_5_REPORT:
                domain_titles = MIN_5_REPORT[domain]['page_titles'].copy()
                for title in json_remove[domain]['page_titles']:
                    if title in MIN_5_REPORT[domain]['page_titles']:
                        domain_titles.remove(title)

                MIN_5_REPORT[domain]['page_titles'] = domain_titles
                MIN_5_REPORT[domain]['page_count'] = len(MIN_5_REPORT[domain]['page_titles'])

                users_list = json_remove[domain]['users']
                for user in users_list:
                    if user in MIN_5_REPORT[domain]['users']:
                        MIN_5_REPORT[domain]['users'].pop(user)   
    except Exception as e :
        print("Exception occured in re_calculate removal {}".format(e))
        
    try :
        for domain in json_add:
            if domain in MIN_5_REPORT:
                for title in json_add[domain]['page_titles']:
                    if title not in MIN_5_REPORT[domain]['page_titles']:
                        MIN_5_REPORT[domain]['page_titles'].add(title)
                MIN_5_REPORT[domain]['page_count'] = len(MIN_5_REPORT[domain]['page_titles'])

                users_list = json_add[domain]['users']
                for user in users_list:
                    MIN_5_REPORT[domain]['users'][user] = users_list[user]    
            else:
                MIN_5_REPORT[domain] = json_add[domain]

        MIN_5_REPORT = sort_report(MIN_5_REPORT)
        return MIN_5_REPORT

    except Exception as e :
        print("Exception occured in re_calculate during addition {}".format(e))
        
async def save_report(json_data):

    for domain in json_data:
        json_data[domain]['page_titles'] = list( json_data[domain]['page_titles'])

    try:
        time_stamp =  datetime.now().strftime("%A- %d- %B %Y %I-%M-%S")
        file_path = WIKI_REPORT_PATH + os.sep + "Report_" + time_stamp + "_.json"
        ostream = open(file_path, 'w')
        json.dump(json_data, ostream, indent = 4)
    except Exception as e:
        print(e)

async def print_console(json_data,prev_time,current_time):
    print("\n{d1} Report generated from {prev_time} - {current_time} {d2}"
    .format(
            d1 = "#"*20,
            prev_time = str(prev_time),
            current_time = str(current_time),
            d2 = "#"*20
        )
    )

    for data in json_data:
        print("\n{} : {} pages updated "
        .format(
                data , 
                json_data[data]['page_count']
            )
        )
        print("users:")
        user_list = json_data[data]['users']
        for user in user_list:
            print("\t- {user} : {edit_count}"
            .format(
                    user = user,
                    edit_count = user_list[user]
                )
            )

async def generate_report(json_data):
    """ 
    The global TIME_WINDOW will store past 5 min data 
    for sliding window report i.e 1-6 , 2-7 ranges so on

    The global COUNTER is only incremented first 5 times then
    print SWR of every 5 mintues.

    TODO:
        - Use deque DS to keep window sliding 
        - After 5 min, crunch the whole data into single report
        - Then Pop left & append right in deque 
        - Recalc Report : Subtract the popped values from single report 
          and add/update append values   
        - Lastly print they see me rolling 5 min sliding report  

    """

    global TIME_WINDOW
    global COUNTER 
    global PREV_TIME
    global CURRENT_TIME 

    filtered_report = filter_report(json_data)
    sorted_report = sort_report(filtered_report)
   
    
    #TODO: save report async based on event url & timestamp
    if COUNTER > 5:
        five_minute = timedelta(minutes=5)
        PREV_TIME = CURRENT_TIME - five_minute
        json_remove = TIME_WINDOW.popleft()
        result = re_calucate(
            json_remove = json_remove,
            json_add = sorted_report
        )
        TIME_WINDOW.append(sorted_report)

        await print_console(
            json_data= result,
            prev_time=  PREV_TIME,
            current_time= CURRENT_TIME
        )
        await save_report(json_data= copy.deepcopy(result)) 
        
    else: 
        COUNTER += 1
        one_minute = timedelta(minutes=1)
        PREV_TIME = CURRENT_TIME - one_minute
        TIME_WINDOW.append(sorted_report)

        await print_console(
            json_data = sorted_report,
            prev_time=  PREV_TIME,
            current_time= CURRENT_TIME
        )
        await save_report(json_data= copy.deepcopy(sorted_report)) 
        
        if COUNTER == 5:
            crunch_report()
            
async def read_stream(session):
    """
    Store the stream data for 1 min in temp on message event
    Every 1 min call generate report 
    """
    global EVENT_DATA
    temp = []
    event_type = 'revision-create'
    api_url = 'https://stream.wikimedia.org/v2/stream/{event_type}'.format(event_type = event_type)
    
    try:
        print("\nWaiting to recieve data on api url on event {event_type} ..".format(event_type=event_type))
        print("\nPlease wait for 1 mniute while we collect data & generate report")

        async for event in aiosseclient(api_url):
            if event.event == 'message':
                try:
                    change = json.loads(event.data)   
                    EVENT_DATA.append(change) 
                except ValueError:
                    pass
                   
    except Exception as e:
        print(e)
        return await read_stream(session)

async def main():
    print("\nInitializing session ..")
    async with aiohttp.ClientSession() as session:
        never = await read_stream(session)

async def timer():
    global CURRENT_TIME
    global EVENT_DATA
    start = time.time()

    while True:
        CURRENT_TIME = datetime.now()
        done = time.time()
        elapsed = done - start
        if elapsed >= 5:
            print("\nTime is {} ".format(CURRENT_TIME))
            start = done
            if EVENT_DATA:
                await generate_report(json_data= EVENT_DATA)
                EVENT_DATA = []
            else:
                print("\n Did not recive any data past 1 minute , trying again ....")
        else:
            await asyncio.sleep(1)


loop = asyncio.get_event_loop()
try:
    task1 = loop.create_task(main())
    task2 = loop.create_task(timer())
    loop.run_until_complete(asyncio.gather(task1, task2))
except :
    pass
finally:
    print("\nProgram closed")
    exit()
    
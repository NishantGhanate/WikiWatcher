import os
import sys
import json
import time
import asyncio
import aiohttp
import traceback

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

def filter_report(json_data):
    """
    Please readme.md for filtered data structure
    """
    maps = {}
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
                    'page_title' : [ data['page_title'] ]
                }
                maps[data['meta']['domain']] = temp
            else:
                if data['page_title'] not in maps[domain]['page_title']:
                    maps[domain]['page_title'].append(data['page_title'] ) 

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

            
            maps[domain]['page_edit_count'] = len(maps[domain]['page_title'])

    for m in maps:
        maps[m].pop('page_title')  
               
    return maps

def sort_report(json_data,page_rsort=True,user_rsort=True):
    keys = dict(sorted(json_data.items(), key=lambda x: (x[1]['page_edit_count']), reverse=page_rsort))   
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
                    MIN_5_REPORT[domain]['page_edit_count'] += window[domain]['page_edit_count']
                    users_list = window[domain]['users']
                    for user in users_list:
                        if user in MIN_5_REPORT[domain]['users']:
                            MIN_5_REPORT[domain]['users'][user] +=  users_list[user]
                        else:
                            MIN_5_REPORT[domain]['users'][user] = users_list[user]
    except Exception as e :
        print("\n Exception occured in crunch report {}".format(e))
        print(traceback.print_exc)
        sys.exit()

def re_calucate(json_remove,json_add):
    global MIN_5_REPORT

    try :
        for key in json_remove:
            if key in MIN_5_REPORT:
                MIN_5_REPORT[key]['page_edit_count'] -= json_remove[key]['page_edit_count']

                users_list = json_remove[key]['users']
                for user in users_list:
                    if user in MIN_5_REPORT[key]['users']:
                        MIN_5_REPORT[key]['users'][user] -= users_list[user]    
    except Exception as e :
        print("Exception occured in re_calculate removal {}".format(e))
        
    
    try :
        for key in json_add:
            if key in MIN_5_REPORT:
                MIN_5_REPORT[key]['page_edit_count'] += json_add[key]['page_edit_count']
                users_list = json_add[key]['users']
                for user in users_list:
                    if user in MIN_5_REPORT[key]['users']:
                        MIN_5_REPORT[key]['users'][user] += users_list[user]
                    else:
                        MIN_5_REPORT[key]['users'][user] = users_list[user]
            
            else:
                MIN_5_REPORT[key] = json_add[key]

        MIN_5_REPORT = sort_report(MIN_5_REPORT)
        return MIN_5_REPORT

    except Exception as e :
        print("Exception occured in re_calculate during addition {}".format(e))
        
async def save_report(json_data):
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
                json_data[data]['page_edit_count']
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
        # print("\nShowing report over last 5 minutes")
        five_minute = timedelta(minutes=5)
        PREV_TIME = CURRENT_TIME - five_minute
        json_remove = TIME_WINDOW.popleft()
        result = re_calucate(
            json_remove = json_remove,
            json_add = sorted_report
        )

        await print_console(
            json_data= result,
            prev_time=  PREV_TIME,
            current_time= CURRENT_TIME
        ) 
        TIME_WINDOW.append(sorted_report)
    else: 
        COUNTER += 1
        one_minute = timedelta(minutes=1)
        PREV_TIME = CURRENT_TIME - one_minute
        await print_console(
            json_data = sorted_report,
            prev_time=  PREV_TIME,
            current_time= CURRENT_TIME
        )
        TIME_WINDOW.append(sorted_report)

        if COUNTER == 5:
            crunch_report()
    # await save_report(sorted_report)
            
async def read_stream(session):
    """
    Store the stream data for 1 min in temp on message event
    Every 1 min call generate report 
    """
    global CURRENT_TIME 

    temp = []
    start = time.time()
    url = 'https://stream.wikimedia.org/v2/stream/revision-create'
    try:
        print("\nFetching url..... ")
        print("\nPlease wait for 1 mniute while we collect data & generate report")

        async for event in aiosseclient(url):
            if event.event == 'message':
                try:
                    change = json.loads(event.data)    
                except ValueError:
                    pass
                temp.append(change)
                done = time.time()
                elapsed = done - start

                if elapsed >= 60:
                    start = done
                    CURRENT_TIME = datetime.now()
                    await generate_report(json_data= temp)
            
    except Exception as e:
        print(e)
        return await read_stream(session)

async def main():
    print("\nInitializing session ..")
    async with aiohttp.ClientSession() as session:
        never = await read_stream(session)


loop = asyncio.get_event_loop()
try:
   loop.run_until_complete(main())
except :
    pass
finally:
    print("\nProgram closed")
    exit()
    
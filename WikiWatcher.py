import json
import time
import asyncio
import aiohttp
from aiosseclient import aiosseclient

from datetime import datetime
import os

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

WIKI_REPORT_PATH =  os.path.join(SCRIPT_DIR,"WikiReports")

if not os.path.exists(WIKI_REPORT_PATH):
    os.makedirs(WIKI_REPORT_PATH)

def filter_report(json_data):
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

async def save_report(json_data):
    try:
        time_stamp =  datetime.now().strftime("%A- %d- %B %Y %I-%M-%S")
        file_path = WIKI_REPORT_PATH + os.sep + "Report_" + time_stamp + "_.json"
        ostream = open(file_path, 'w')
        json.dump(json_data, ostream, indent = 4)
    except Exception as e:
        print(e)

async def print_console(json_data):
    for data in json_data:
        print("\nDomain : {} , {} pages updated "
        .format(
                data , 
                json_data[data]['page_edit_count']
            )
        )
        print("Users who made changes to {} ".format(data))
        user_list = json_data[data]['users']
        for user in user_list:
            print("\t - {user} : {edit_count}"
            .format(
                    user = user,
                    edit_count = user_list[user]
                )
            )

async def generate_report_1(json_data):
    filtered_report = filter_report(json_data)
    sorted_report  = sort_report(filtered_report)
    #TODO: save report async based on event url & timestamp
    await save_report(sorted_report)
    await print_console(sorted_report)

async def read_stream(session):
    temp = []
    start_1 = start_5 = time.time()
    try:
        async for event in aiosseclient('https://stream.wikimedia.org/v2/stream/revision-create'):
            if event.event == 'message':
                try:
                    change = json.loads(event.data)    
                except ValueError:
                    pass
                temp.append(change)
                done = time.time()
                elapsed = done - start_1
                if elapsed >= 5:
                    start_1 = done
                    await generate_report_1(json_data= temp)

                # elapsed = done - start_5
                # if elapsed >= 300:
                #     start_5 = time.time()
                    
    except Exception as e:
        print(e)
        return await read_stream(session)

async def main():
    async with aiohttp.ClientSession() as session:
        never = await read_stream(session)


loop = asyncio.get_event_loop()
try:
   loop.run_until_complete(main())
except KeyboardInterrupt:
    pass
finally:
    print("Closing Program")
    loop.close()
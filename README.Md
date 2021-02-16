## WikiWatcher

### Describtion about this project 

```sh

Wiki watcher is an python program made to consume wikipedia streaming data and 
generate a report every 1 & 5 minutes 
i.e - 
    1) No.of unique title data changes made to a domain
    2) Accumalte the unique users (excluding bot) and their user text and edit count 
    3) Sort changes made to a domian & user edit count in a descending order 

```

### ToDo List 
    - [ x ] Fetch data from api 
    - [ x ] Filter the data 
    - [ x ] Sort the data dictonary
    - [ x ] Print data capatured for 1 minute
    - [ ] Sliding window to show past 5 minutes collected data

&nbsp;

### Getting started

<br>

Create an empty folder and use the command below:
```sh
git clone https://github.com/NishantGhanate/WikiWatcher.git
```

In Cli : 1. Create virtual enviroment, 2. Activate it,  3. Install requirements 
```sh
 > virtualenv venv

 > venv\Scripts\activate

 > pip install -r requirements.txt

```

### Run the code :
```sh
python WikiWatcher.py
```

&nbsp;

### Whatever you call it Dictionary / Hashmap / Json - Structure
```sh
data = {
    'domain_1' : {
        'users' : {
            'domain_1_user_1' : 1,
            'domain_1_user_2' : 2,
            'domain_1_user_3' : 3
        },
        'page_edit_count': 1
    },
    'domain_2' : {
        'users' : {
            'domain_2_user_1' : 1,
            'domain_2_user_2' : 2,
            'domain_2_user_3' : 3
        },
        'page_edit_count': 2
    }
}

```
where domain_ will be the domain name , 
users will contain user text : user edit count value
and page_edit_count is no. of unique titles updated in that domain

&nbsp;

### Wiki Api guide 
> https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams
&nbsp;


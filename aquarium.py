import websocket
import sqlite3
import json
import pandas as pd
from datetime import datetime
import time
import requests
import os
import sys


def log(message):
    with open('log.txt', 'a') as f:
        f.write('\n')
        f.write(datetime.now().isoformat())
        f.write('    :')
        f.write(message)
    return 

def get_version():
    while True:
        try:
            filename = 'version.txt'
            with open(filename, 'r') as file:
                current_version = float(file.read())
                return current_version
        except:
            pass
    
def version_check():
    global version
    current_version = get_version()
    if current_version > version:
        print('UPDATING NOW')
        log('Update to Version: {}'.format(current_version))
        os.execv(sys.executable, ['python3'] + sys.argv)
    if current_version == version: 
        return 

class Squid():
    message = None
    url = None
    product_id = None
    ws_url = None
    messages = []
    last_message = None
    logfile = 'log.txt'
    
    def __init__(self, product_id, url ='https://api.gdax.com' ,  ws_url = "wss://ws-feed.gdax.com"):
        version_check()
        self.product_id = product_id
        self.url = url
        self.ws_url = ws_url
        self.last_message = datetime.now()
        self.time_difference = pd.to_datetime(self.get('/time').json()['iso']) - datetime.now()
        log('Squid Created')
                
    def get(self, path, **params):
        #Requests passthrough, tries to avoid the too many requests code by adding pauses
        self.last_message
        now = datetime.now()
        if (now - self.last_message).total_seconds() <= 1:
            time.sleep(1)
            
        try:
            done = False
            while not done:
                r = requests.get(self.url + path, params=params, timeout=30, verify = True)
                self.r = r
                if r.status_code == 429:
                    time.sleep(1)
                    pass
                if r.status_code != 429:
                    done = True
                if r.status_code == 500:
                    raise ValueError('Internal Server Error')
                    done = True
                    
        except ConnectionError as e:
            message = datetime.now().isoformat() + ' -----\n'
            message = message + 'CONNECTIONERROR\n'
            message = message + repr(e)
            self.log(message)
            raise
        return r
    
    
    def convert_time(self, time):
        return (time + self.time_difference).isoformat()
    
    
    def on_open(self, ws):
        sub = {'type':'subscribe',
               'channels':['ticker'],
               'product_ids': [self.product_id]
               }
        
        self.ws.send(json.dumps(sub))
        log('Subscription message sent')
        
    def on_close(self, ws):
        log('_____________ CLOSED _____________')
        print('_____________ CLOSED _____________')
        version_check()

    def on_error(self, ws, error):
        print('ERROR')
        log(str(error))
        version_check()

    def log(self, message):
        with open(self.logfile, "a") as f:
            f.write(message)
        
        
    def save_info(self, data):
        done = False
        while not done:
            with sqlite3.connect('fire.db', check_same_thread = False) as con:
                try:
                    data.to_sql(self.product_id, con, if_exists = 'append')
                    con.commit()
                    done = True
                except sqlite3.OperationalError:
                    pass
        version_check() 
        
    def on_message(self, ws, message):
        message = json.loads(message)
         
        if message['type'] == 'ticker':
            self.messages.append(message)
            
        if len(self.messages) > 100:
            data = pd.DataFrame(self.messages)
            data = data[['price','last_size', 'sequence', 'side', 'time', 'trade_id']]
            data = data[~data['side'].isnull()]

            for col in ['price', 'last_size']:
                data[col] = pd.to_numeric(data[col])
            
            for col in ['sequence', 'trade_id']:
                data[col] = data[col].apply(int)
                
            data.set_index('trade_id', inplace = True)
            self.messages = []
            self.save_info(data)
        try:
            print( len(self.messages) ,'---' ,message['side'], '---' , round(float(message['price']), 2), '  ---  ', message['last_size'])
        except Exception as e:
            version_check()
            if message['type'] == 'subscriptions': 
                log('Subscription Confirmed')
                return
            if message['type'] == 'ticker':
                log('First Message Recieved')
                return
            log('unknown message:')
            log(repr(message))
            print(repr(e))
            print(message)
        
    def run_websocket_app(self):
        self.ws = websocket.WebSocketApp(self.ws_url,
                                    on_message = self.on_message,
                                    on_error = self.on_error,
                                    on_close = self.on_close,
                                    on_open= self.on_open)
        
        self.ws.run_forever()


def enforce_data_types(product_id):
    with sqlite3.connect('fire.db', check_same_thread = False) as con:
        try:
            data = pd.read_sql('select * from [{}]'.format(product_id), con, index_col = 'trade_id')
            
            for col in ['price', 'last_size']:
                data[col] = pd.to_numeric(data[col])
        
            data['sequence'] = data['sequence'].apply(int)
            data.index = data.index.map(int)
            
            data.to_sql( product_id, con, if_exists = 'replace')
            con.commit()
            
            log('Data Type Enforced.')
        except Exception as e:
            log('ERROR DURING DATA TYPE ENFORCE')
            log(repr(e))
            log('Waiting For Bugfix Update.')
            while True:
                version_check()
            pass


if __name__ == '__main__':
    version = get_version()
    
    #enforce_data_types('ETH-USD')
    
    while True:
        try:
            version_check()
            squid = Squid('ETH-USD')
            squid.run_websocket_app()
        except Exception as e:
            log('Main Loop Exception' +repr(e))
            pass
    

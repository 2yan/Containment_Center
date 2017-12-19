import websocket
import sqlite3
import json
import pandas as pd
from datetime import datetime
import time
import requests
import os

class Squid():

    message = None
    url = None
    product_id = None
    ws_url = None
    messages = []
    last_message = None
    logfile = 'log.txt'
    
    def __init__(self, product_id, url ='https://api.gdax.com' ,  ws_url = "wss://ws-feed.gdax.com"):
        self.product_id = product_id
        self.url = url
        self.ws_url = ws_url
        self.last_message = datetime.now()
        self.time_difference = pd.to_datetime(self.get('/time').json()['iso']) - datetime.now()
        
                
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
        
    def on_close(self, ws):
        print('_____________ CLOSED _____________')
        
    def on_error(self, ws, error):
        print('ERROR')
        self.log(str(error))
        
    def log(self, message):
        os.makedirs(os.path.dirname(self.logfile), exist_ok=True)
        with open(self.filename, "w") as f:
            f.write(message)
        
        
    def save_info(self, data):
        done = False
        while not done:
            with sqlite3.connect('fire.db', check_same_thread = False) as con:
                try:
                    data.to_sql(self.product_id, con, if_exists = 'replace')
                    con.commit()
                    done = True
                except sqlite3.OperationalError:
                    pass
                
        
    def on_message(self, ws, message):
        message = json.loads(message)
        if message['type'] == 'ticker':
            self.messages.append(message)
            
        if len(self.messages) > 50:
            data = pd.DataFrame(self.messages)
            self.messages = []
            self.save_info(data)
        try:
            print(message['price'])
        except:
            print(message)
        
    def run_websocket_app(self):
        self.ws = websocket.WebSocketApp(self.ws_url,
                                    on_message = self.on_message,
                                    on_error = self.on_error,
                                    on_close = self.on_close,
                                    on_open= self.on_open)
        
        self.ws.run_forever()
        
while True:
    try:
        squid = Squid('ETH-USD')
        squid.run_websocket_app()
    except:
        pass
    
        
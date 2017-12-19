import                            requests
from   datetime import datetime, timedelta
import                                time
import                                  os
import pandas    as                     pd
import numpy     as                     np

class Scythe():
    product_id = None
    url = None
    last_message = None
    r = None
    time_difference = None
    
    
    def __init__(self, product_id, url = 'https://api.gdax.com'):
        self.product_id = product_id
        self.url = url
        self.last_message = datetime.now()
    
        self.time_difference = pd.to_datetime(self.get('/time').json()['iso']) - datetime.now()
        
    
    def convert_time(self, time):
        return (time + self.time_difference).isoformat()
        
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
            
    def log(self, message):
        os.makedirs(os.path.dirname(self.logfile), exist_ok=True)
        with open(self.filename, "w") as f:
            f.write(message)
    
    def get_trades(self, before = None, after = None):
        data = self.get('/products/{}/trades'.format(self.product_id), before = before, after = after).json()
        
        data = pd.DataFrame(data)
        data['time'] = pd.to_datetime(data['time']) - self.time_difference
        
        dtype = {'price': np.float64, 'size':np.float64, 'trade_id':np.int64}
        for col in dtype.keys():
            data[col] = dtype[col](data[col])
            
        data.set_index('trade_id', inplace = True )
        return data
    
    
    def get_trades_until(self, time):
        data = self.get_trades()
        while data['time'].min() >= time:
            oldest = self.r.headers['cb-after']
            trades2 = self.get_trades(after = oldest)
            data = data.append(trades2)
        return data
    
    
    
class Omega():
    
    def add_candle_time(self, trades):
        time_string = '%Y/%m/%d %H:%M'
        trades['candle_time'] = trades['time'].dt.strftime(time_string)
        return trades
    
    def mean(self ,trades):
        trades['dollars'] = trades['price'] * trades['size']
        grouped = trades.groupby('candle_time')
        final = grouped['dollars'].sum()/grouped['size'].sum()
        return final

    def median(self, trades):
        trades['median']
        
scythe = Scythe('ETH-USD')

now = datetime.now()
until = now - timedelta(seconds = 60 * 3)
trades = scythe.get_trades_until(until)
omega = Omega()
trades = omega.add_candle_time(trades)
final = pd.DataFrame(omega.mean(trades), columns = ['mean'])

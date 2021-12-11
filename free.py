import json
import csv
import random
from utils import redis_cache
from timestring import Date
from collections import defaultdict


class TDAnanalyser(object):

    post_days = 10

    def __init__(self, data_path=None, duration=None):
        self._duration = duration
        self._data_path = data_path
        self._history = None
        self._stocks = {}
        if data_path:
            self._history = self.csv2json(data_path)
        self.build_stocks()

    def csv2json(self, csv_path=None):
        return csv.DictReader(open(csv_path, 'r'))
    
    @redis_cache(expire=12*3600, prefix='stocks_info', ignore_first_arg=True)
    def build_stocks(self):
        stocks = defaultdict(lambda: [])
        if self._duration:
            start = Date(self._duration[0])
            end = Date(self._duration[1])
            for row in self._history:
                date = Date(row['date'])
                if start >= date and date <= end:
                    stocks[row['code']].append(row)
        else:
            for row in self._history:
                stocks[row['code']].append(row)
        self._stocks = stocks

    def history(self):
        return self._history

    @property
    def stock_codes(self):
        return list(self._stocks.keys())

    @property
    def stocks(self):
        return self._stocks

    def get_stock(self, code='sz.300180'):
        return self._stocks.get(code, None)

    def seek_td_sequence(self, code='sz.300180', trend='down'):
        td_count = 0
        td_results = []
        day_k = self._stocks.get(code, [])
        for cnt in range(4, len(day_k)):
            if day_k[cnt]['close'] < day_k[cnt-4]['close'] and td_count < 9:
                td_count += 1
            elif td_count == 9:
                try:
                    td_info = {}
                    td_info['td_day'] = day_k[cnt]
                    td_info['post_days'] = day_k[cnt+1:cnt+self.post_days+1]
                    td_info['post_high'] = max([float(day['high'])] for day in td_info['post_days'])[0]
                    td_info['post_low'] = max([float(day['low'])] for day in td_info['post_days'])[0]
                    td_info['is_true'] = True if float(td_info['post_high']) > float(td_info['td_day']['close']) else False
                    td_results.append(td_info)
                    td_count = 0
                except ValueError:
                    pass
                continue
            else:
                td_count = 0
                continue
        return td_results

    def analyse(self, code, stop_loss=0.02, stop_profit=0.04, tx_fee=0.003, tx_amount=100000):
        def gamble(data):
            res = {}
            td_close = float(data['td_day']['close'])
            day1_low = float(data['post_days'][0]['low'])
            cost = random.uniform(day1_low, td_close) # only buy when price under last close
            if data['post_high'] >= cost*(1+stop_profit):
                res['result'] = 'won'
                res['profit'] = tx_amount*(stop_profit - tx_fee)
            elif data['post_low'] <= cost*(1-stop_loss):
                res['result'] = 'lose'
                res['profit'] = tx_amount*(stop_loss - tx_fee)
            else:
                sold =  random.uniform(data['post_low'], data['post_high'])
                res['profit'] = tx_amount*((sold-cost)/cost-tx_fee)
                res['result'] = 'timeout'
            return res

        sequences = self.seek_td_sequence(code)
        ret = {}
        ret['profit'] = 0
        ret['won'] = 0
        ret['lose'] = 0
        ret['timeout'] = 0
        ret['count'] = 0
        for td in sequences:
            result = gamble(td)
            ret['count'] += 1
            ret['profit'] += int(result['profit'])
            if result['result'] == 'won':
                ret['won'] += 1
            elif result['result'] == 'lose':
                ret['lose'] += 1
            elif result['result'] == 'timeout':
                ret['timeout'] += 1
        ret['yield_rate'] = str(int(ret['profit']/tx_amount*100)) + '%'
        ret['won_rate'] = str(int(ret['won']/ret['count']*100)) + '%'
        return ret
    
    def stats(self, quantity=100):
        res = {}
        res['rate_list'] = []
        random_indexes = random.sample(range(0, len(self._stocks.keys())), quantity)
        for idx in random_indexes:
            res['rate_list'].append(self.analyse(list(self._stocks.keys())[idx]))
        res['max'] = max(res['rate_list'])
        res['max'] = min(res['rate_list'])
        res['count'] = len(res['rate_list'])
        return res


if __name__ == '__main__':
    tda = TDAnanalyser('./zz500_day_history.csv', ['1/1/2021', '12/1/2021'])
    for code in  random.sample(range(0, len(tda.stock_codes)), 10):
        print(tda.analyse(tda.stock_codes[code]))


import json
import csv
import random
from profile import A
from timestring import Date
from collections import defaultdict
from rich.json import JSON 
from rich import print_json
from utils import redis_cache


class TDAnanalyser(object):

    def __init__(self, data_path=None, profile=A):
        print('Profile:')
        print_json(data=profile)
        self._data_path = data_path
        self._history = None
        self._stocks = {}
        self.post_days = profile.get('duration', 0)
        self.stop_profit = profile.get('stop_profit', 0)
        self.stop_loss = profile.get('stop_loss', 0)
        self.tx_fee = profile.get('tx_fee', 0)
        self.tx_amount = profile.get('tx_amount', 0)
        self.period = profile.get('history_period', None)
        self.num_stock = profile.get('num_stock', 0)

        self._history = self.csv2json(data_path)
        self._stocks = self.build_stocks()
        self._td_sequences = self.build_stocks(by='date')

    def csv2json(self, csv_path=None):
        return csv.DictReader(open(csv_path, 'r'))
    
    @redis_cache(ignore_first_arg=True, extra_cond=lambda: False, expire=7*24*3600)
    def build_stocks(self, by='code'):
        stocks = defaultdict(lambda: [])
        if self.period:
            start = Date(self.period[0])
            end = Date(self.period[1])
            for row in self._history:
                date = Date(row['date'])
                if date >= start and date <= end:
                    stocks[row[by]].append(row)
        else:
            for row in self._history:
                stocks[row[by]].append(row)
        return stocks
    
    def history(self):
        return self._history

    @property
    def stock_codes(self):
        return list(self._stocks.keys())

    @property
    def stocks(self):
        return self._stocks

    @redis_cache(ignore_first_arg=True, extra_cond=lambda: False, expire=7*24*3600)
    def get_stock(self, code='sz.300180'):
        return self._stocks.get(code, None)

    def seek_td_sequence(self, code='sz.300180', trend='down'):
        td_target = 9 
        td_count = 0
        td_results = []
        day_k = self._stocks.get(code, [])
        for idx in range(4, len(day_k)):
            if day_k[idx]['close'] < day_k[idx-4]['close'] and td_count < td_target:
                td_count += 1
            elif td_count == td_target:
                try:
                    ninth_idx = idx - 1
                    td_info = {}
                    td_info['code'] = code
                    td_info['date'] = day_k[ninth_idx]['date']
                    td_info['td_day'] = day_k[ninth_idx]
                    td_info['post_days'] = day_k[ninth_idx+1:ninth_idx+self.post_days+1]
                    td_info['post_high'] = max([float(day['high'])] for day in td_info['post_days'][1:])[0]
                    td_info['post_low'] = min([float(day['low'])] for day in td_info['post_days'][1:])[0]
                    td_info['next_day_high'] = float(td_info['post_days'][0]['high'])
                    td_info['next_day_low'] = float(td_info['post_days'][0]['low'])
                    td_info['last_day_high'] = float(td_info['post_days'][-1]['high'])
                    td_info['last_day_low'] = float(td_info['post_days'][-1]['low'])
                    td_info['is_true'] = True if float(td_info['post_high']) > float(td_info['td_day']['close']) else False
                    td_results.append(td_info)
                    td_count = 0
                except ValueError:
                    td_count = 0
                    pass
                continue
            else:
                td_count = 0
        return td_results

    def gamble(self, data, stop_loss=0.02, stop_profit=0.04, tx_fee=0.003, tx_amount=100000):
        res = {}
        td_close = float(data['td_day']['close'])
        cost = random.uniform(data['next_day_low'], td_close) # only buy when price under last close
        if data['post_high'] >= cost*(1+self.stop_profit):
            res['result'] = 'won'
            res['profit'] = int(self.tx_amount*(self.stop_profit - self.tx_fee))
        elif data['post_low'] <= cost*(1-self.stop_loss):
            res['result'] = 'lose'
            res['profit'] = -int(self.tx_amount*(self.stop_loss - self.tx_fee))
        else:
            sold = random.uniform(data['last_day_high'], data['last_day_low'])
            res['profit'] = int(self.tx_amount*((sold-cost)/cost-self.tx_fee))
            res['result'] = 'timeout'
        res['code'] = data['code']
        res['date'] = data['date']
        return res

    def analyse_stock(self, sequences=[]):
        if not sequences:
            return {}
        ret = {}
        ret['profit'] = 0
        ret['won'] = 0
        ret['lose'] = 0
        ret['timeout'] = 0
        ret['count'] = 0
        ret['detail'] = {}
        for td in sequences:
            result = self.gamble(td)
            ret['count'] += 1
            ret['profit'] += int(result['profit'])
            if result['result'] == 'won':
                ret['won'] += 1
            elif result['result'] == 'lose':
                ret['lose'] += 1
            elif result['result'] == 'timeout':
                ret['timeout'] += 1
            ret['code'] = td['td_day']['code']
            ret['detail'][result['code']] = result
        ret['won_rate'] = str(int(ret['won']/ret['count']*100)) + '%'
        return ret

    def seek_td_by_date(self, date=None):
        td_list = []
        target_list = []
        for code in self._stocks.keys():
            td_list += self.seek_td_sequence(code)
        for td in td_list:
            if td['td_day']['date'] == date:
                target_list.append(td)
        random.shuffle(target_list)
        return target_list[:self.num_stock if len(target_list) >= 5 else len(target_list)]
        #return target_list

    def stats(self, quantity=100):
        res = {}
        res['rate_list'] = []
        random_indexes = random.sample(range(0, len(self._stocks.keys())), quantity)
        for idx in random_indexes:
            res['rate_list'].append(self.analyse_stock(list(self._stocks.keys())[idx]))
        res['max'] = max(res['rate_list'])
        res['max'] = min(res['rate_list'])
        res['count'] = len(res['rate_list'])
        return res


if __name__ == '__main__':
    tda = TDAnanalyser(data_path='./zz500_day_history.csv', profile=A)
    total = 0
    day = Date('2018-01-03')
    for _ in range(50):
        day = day + '7d'
        date = f"{day.year}-{day.month}-{day.day}"
        print(date)
        res = tda.analyse_stock(tda.seek_td_by_date(date))
        #print_json(data=res)
        #print(f"Profit: {res['profit']} RMB ({res['profit']/1200000*100}%) - {res['code']}: won({res['won']}):lose({res['lose']}):timeout({res['timeout']})")
        total += res.get('profit', 0)
        print(f"{date}: {res.get('profit', 0)}RMB ({total}RMB)")
    """
    for r in range(10):
        res = tda.analyse_stock(tda.seek_td_by_date('2021-05-07'))
        #print_json(data=res)
        print(f"Profit: {res['profit']} RMB ({res['profit']/1200000*100}%) - {res['code']}: won({res['won']}):lose({res['lose']}):timeout({res['timeout']})")
        total += res['profit']
    print(f"Avg. {int(total/10)} RMB")
    days = ['2021-5-17']
    date = Date('2021-5-17')
    temp_val = None
    for i in range(14):
        temp_val = date + '7d'
        print(temp_val)
        days.append(temp_val._original)
    for day in days:
        print(day)
        print_json(data=tda.analyse_stock(tda.seek_td_by_date(day)))
    """

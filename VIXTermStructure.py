# -*- coding: utf-8 -*-
import backtrader as bt
import datetime
import numpy as np
import backtrader as bt
import pandas as pd
import quandl
from tai_pan_converter import file_to_dataframe, tai_pan_dir_to_dataframe_extended
from matplotlib import pyplot as plt
import pyfolio as pf

quandl.ApiConfig.api_key = "jAjUe26LEsdbkUACWYj4"


''' 
PYTHON VERSION 3.7.16

Bachelorarbeit zu der Strategie: 
http://www.diva-portal.org/smash/get/diva2:1447840/FULLTEXT01.pdf
    
Seit 2021 keien VX.1-Daten mehr... RIP Quandl.
    
Performance war historisch sehr gut, aber haben in der letzten Zeit (vor allem seit 
Veröffentlichung der Bachelorarbeit) aber hat sich in letzter Zeit merklich verschlechtert.

Need to consider cheat on open and comissions as well... ~~~
'''

class VIXTermStructure(bt.Strategy):
    params = (
        ('printlog', False),
    )



    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.vix = self.dnames['vix']
        self.vix_front = self.dnames['vix_front']
        self.vixy = self.dnames['VIXY']
        self.svxy = self.dnames['SVXY']
        
        self.n_open_orders = 0
        
    def log(self, txt, dt=None, doprint=False):
        ''' Logging function for this strategy'''
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))
        
    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))       
    
    def notify_order(self, order):
        if order.status in [order.Submitted]:
            self.log(f"(order acc) {'BUY' if order.isbuy() else 'SELL'} for {order.data._name} with size {order.size} and price {order.price}")
            self.n_open_orders += 1

        if order.status in [order.Completed]:
            if order.isbuy():
                self.n_open_orders -= 1
                self.log(f"(executed) BUY for {order.data._name}, "
                         f"Price: {order.executed.price:.2f}, "
                         f"Volume: {order.executed.size:.2f}, "
                         #f"Comm: {order.executed.comm:.2f}"
                         )
            elif order.issell():
                self.n_open_orders -= 1
                self.log(f"(executed) SELL for {order.data._name}, "
                         f"Price: {order.executed.price:.2f}, "
                         f"Volume: {order.executed.size:.2f}, "
                         #f"Comm: {order.executed.comm:.2f}"
                         )
            
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f"Order for {order.data._name} uncompleted because: {order.getstatusname()} \n"
                     f"size: {order.size}, price: {order.price} cant be fullfiled with cash: {self.broker.getcash()}")
            self.n_open_orders -= 1
            
        self.log(f"Cash: {self.broker.getcash()}, Value: {self.broker.getvalue()}")

    def nextstart(self):
        self.old_date_svxy = self.svxy.num2date()
        self.old_date_vixy = self.vixy.num2date()  
        
    def next_open(self):
      
        if self.svxy.num2date() > self.old_date_svxy and self.vixy.num2date() > self.old_date_vixy: 
            self.old_date_svxy = self.svxy.num2date()
            self.old_date_vixy = self.vixy.num2date()
        else:
            return
        
        basis = self.vix_front.open[0]/self.vix.open[0] - 1
        
        if self.n_open_orders == 2: return
        
        if basis > 0:
            
            if self.getposition(data=self.svxy):
                return
            else:
                if self.getposition(data=self.vixy):
                    self.close(data=self.vixy, price=self.vixy.open[0])
                self.buy(data=self.svxy, price=self.svxy.open[0])
                
        elif basis < 0:
            if self.getposition(self.vixy):
                return
            else:
                if self.getposition(self.svxy):
                    self.close(data=self.svxy, price=self.svxy.open[0])
                self.buy(self.vixy, price=self.vixy.open[0])
    
        else:
            return

    def stop(self):
        if self.getposition(self.svxy): self.close(self.svxy)
        if self.getposition(self.vixy): self.close(self.vixy)
        
        
        
class CheatSizer(bt.Sizer):

    def _getsizing(self, comminfo, cash, data, isbuy):
        # Assumes `broker.getvalue()` is based on the last days close, but we want to operate on the open.
        # Thus we need to calculate the money we have at the open...
        assert not self.strategy.getposition(data), self.strategy.getposition(data)
        old_value = self.broker.getvalue()
        commission = self.broker.getcommissioninfo(data).p.commission

        if data == self.strategy.svxy: 
            old_position = self.strategy.getposition(self.strategy.vixy)
            old_data = self.strategy.vixy
        elif data == self.strategy.vixy: 
            old_position = self.strategy.getposition(self.strategy.svxy)
            old_data = self.strategy.svxy
        else:
            pass
        if not old_position:
            return (1 - commission)*old_value  // data.open[0]
        
        self.strategy.log(f"Sizer: old_data.open[0]={old_data.open[0]}, old_data.close[-1]={old_data.close[-1]}")
        new_value = old_value + old_position.size * (old_data.open[0] - old_data.close[-1])
        return (1 - commission)*new_value // data.open[0]
        
    
    
    

if __name__ == '__main__':
    printlog = True

    cerebro = bt.Cerebro(cheat_on_open=True)
    
    # Connecting to Data Feeds
    fromdate = datetime.datetime(year=2011, month=10, day=11)
    todate = datetime.datetime(year=2020, month=3, day=31)
    
    names = ['VIXY', 'SVXY']
    for name in names:
        df = file_to_dataframe(f"{name}.TXT")
        df = df.droplevel('ID').dropna()
        df = df[fromdate:todate]
        df = df.apply(lambda x: x.map(lambda y: y.replace(",", "."))).apply(pd.to_numeric)
        data = bt.feeds.PandasData(dataname=df, name=name, plot=False)
        cerebro.adddata(data, name=name)    
    
    names = ['vix']
    for name in names:
        dataname = f"{name}.csv"
        df = pd.read_csv(dataname)
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.set_index('Date', drop=True)
        df = df.dropna()
        # df['Open'] = df['Open'].fillna(df['Close']) # WARNING!!!
        df = df[fromdate:todate]
        data = bt.feeds.PandasData(dataname=df, name=name, plot=False)
        cerebro.adddata(data, name=name)
        
    name = 'vix_front'
    vix_front = quandl.get("CHRIS/CBOE_VX1", start_date=fromdate, end_date=todate)
    vix_front = vix_front[["Open", "Close"]].dropna()
    # vix_front = vix_front.replace(0, np.nan).dropna() # Data Cleaning
    data = bt.feeds.PandasData(dataname=vix_front, name=name, plot=False)
    cerebro.adddata(data, name=name)
        
    
    # Strategy
    cerebro.addcalendar(bt.PandasMarketCalendar(calendar='NYSE'))
    cerebro.addsizer(CheatSizer)
    cerebro.addstrategy(VIXTermStructure, printlog=printlog)
    
    # Broker Settings
    cerebro.broker.setcash(1_000_000.0)
    cerebro.broker.setcommission(commission=0)
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    #cerebro.addwriter(bt.WriterFile, csv=True)
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')
    strats = cerebro.run()

    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    cerebro.plot()
    
    strat0 = strats[0]
    pyfoliozer = strat0.analyzers.getbyname('pyfolio')
    returns, positions, transactions, gross_lev = pyfoliozer.get_pf_items()
    
    # pf.create_round_trip_tear_sheet(returns, positions=positions, transactions=transactions)
    
    # pf.create_full_tear_sheet(
    #     returns,
    #     positions=positions,
    #     transactions=transactions,
    #     round_trips=True)
    

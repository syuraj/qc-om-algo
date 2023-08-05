from AlgorithmImports import *
from System import *
from QuantConnect import *
from QuantConnect.Algorithm import *
from QuantConnect.Indicators import *
from QuantConnect.Data.Consolidators import *
import decimal as d
# from QuantConnect.Scheduling import *
# import datetime

class MovingAverageCrossAlgorithm(QCAlgorithm):

    def Initialize(self):
        '''Initialise the data and resolution required, as well as the cash and start-end dates for your algorithm. All algorithms must initialized.'''

        self.SetStartDate(2023, 8, 3)
        self.SetEndDate(2023, 8, 4)
        self.SetCash(100000)
        self.symbol = "SPY"
        self.AddEquity(self.symbol, resolution = Resolution.Minute, extendedMarketHours = False)
        # self.Consolidate(self.symbol, datetime.timedelta(minutes=5), self.FiveMinuteBarHandler)

        # Set brokerage to GDAX for cryptos
        # self.SetBrokerageModel(BrokerageName.GDAX, AccountType.Cash)

        # create consolidator for 5 minute
        consFiveMin = TradeBarConsolidator(5)
        consFiveMin.DataConsolidated += self.OnDataConsolidated
        self.SubscriptionManager.AddConsolidator(self.symbol, consFiveMin)

        self.fast = self.EMA(self.symbol, 8);
        self.slow = self.EMA(self.symbol, 21);
        self.slower = self.EMA(self.symbol, 34);

        self.previous = None
        
        self.Schedule.On(self.DateRules.EveryDay(self.symbol), \
                        self.TimeRules.BeforeMarketClose(self.symbol, 20), \
                        Action(self.CloseOpenedPositions))    
    
    def OnDataConsolidated(self, sender, bar):        
        self.fast.Update(bar.EndTime, bar.Close);
        self.slow.Update(bar.EndTime, bar.Close);
        self.slower.Update(bar.EndTime, bar.Close);
        # self.Debug(str(self.Time) + " > New 5 Min Bar!")
        # self.Plot("EMA", self.fast)

    def OnData(self, data):
        '''OnData event is the primary entry point for your algorithm. Each new data point will be pumped in here.'''        

        # wait for our slow ema to fully initialize
        if not self.slow.IsReady:
            return

        current_time = self.Time
        # only run at the close of 5th min candle
        if not current_time.minute % 3 == 0:
            return

        # only trade between 10 am and 3:30 pm est        
        if (current_time.hour < 10) or (current_time.hour == 15 and current_time.minute >= 30) or (current_time.hour > 16):
            return

        holdings = self.Portfolio[self.symbol].Quantity
        current_price = self.Securities[self.symbol].Price

        # we liquidate the Short and go Long
        if holdings <= 0 and current_price > self.fast.Current.Value and self.fast.Current.Value > self.slow.Current.Value and self.slow.Current.Value > self.slower.Current.Value:
            self.Log("BUY  >> {0}".format(self.Securities[self.symbol].Price))
            self.Liquidate(self.symbol)
            self.Buy(self.symbol, 10)

        # we liquidate the Long and go Short
        if holdings > 0 and current_price < self.fast.Current.Value and self.fast.Current.Value < self.slow.Current.Value and self.slow.Current.Value < self.slower.Current.Value:
            self.Log("SELL >> {0}".format(self.Securities[self.symbol].Price))
            self.Liquidate(self.symbol)
            self.Sell(self.symbol, 10)

        self.previous = self.Time

    def CloseOpenedPositions(self):
        holdings = self.Portfolio[self.symbol].Quantity
        if holdings != 0:
            self.Liquidate(self.symbol)
        return

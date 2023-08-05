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

        self.SetStartDate(2019, 1, 1)
        self.SetEndDate(2023, 1, 1)
        self.SetCash(100000)
        self.symbol = "SPY"
        self.AddEquity(self.symbol, Resolution.Minute)        
        # self.Consolidate(self.symbol, datetime.timedelta(minutes=5), self.FiveMinuteBarHandler)

        # Set brokerage to GDAX for cryptos
        # self.SetBrokerageModel(BrokerageName.GDAX, AccountType.Cash)

        # create consolidator for 5 minute
        consFiveMin = TradeBarConsolidator(5)
        consFiveMin.DataConsolidated += self.OnDataConsolidated
        self.SubscriptionManager.AddConsolidator(self.symbol, consFiveMin)

        self.fast = self.EMA(self.symbol, 8);
        self.slow = self.EMA(self.symbol, 21);

        self.previous = None
        
        self.Schedule.On(self.DateRules.EveryDay(self.symbol), \
                        self.TimeRules.BeforeMarketClose(self.symbol, 20), \
                        Action(self.CloseOpenedPositions))    
    
    def OnDataConsolidated(self, sender, bar):
        self.slow.Update(bar.EndTime, bar.Close);
        self.fast.Update(bar.EndTime, bar.Close);
        # self.Debug(str(self.Time) + " > New 5 Min Bar!")
        # self.Plot("EMA", self.fast)

    def OnData(self, data):
        '''OnData event is the primary entry point for your algorithm. Each new data point will be pumped in here.'''        

        # wait for our slow ema to fully initialize
        if not self.slow.IsReady:
            return

        # only once per day
        # if self.previous is not None and self.previous.date() == self.Time.date():
        #     return

        # define a small tolerance on our checks to avoid bouncing
        tolerance = 0.00015;

        holdings = self.Portfolio["SPY"].Quantity
        current_price = self.Securities["SPY"].Price

        # we liquidate the Short and go Long
        if holdings <= 0 and current_price > self.fast.Current.Value and self.fast.Current.Value > self.slow.Current.Value * (1 + tolerance):            
            self.Log("BUY  >> {0}".format(self.Securities["SPY"].Price))
            self.Liquidate("SPY")
            self.Buy("SPY", 10)

        # we liquidate the Long and go Short
        if holdings > 0 and current_price < self.fast.Current.Value and self.fast.Current.Value < self.slow.Current.Value:
            self.Log("SELL >> {0}".format(self.Securities["SPY"].Price))
            self.Liquidate("SPY")
            self.Sell("SPY", 10)        

        self.previous = self.Time

    def CloseOpenedPositions(self):
        holdings = self.Portfolio[self.symbol].Quantity
        if holdings > 0:
            self.Liquidate(self.symbol)
        return

# ============================================================
# TOKEN REGISTRY v1.1
# Official DefineEdge Replica Engine
# Backward Compatible | Thread Safe
# ============================================================
from dhanAPI_helper import DhanApi

import mibian
import os 
import io 
import re
import pdb 
import sys
import time
import pytz
import json
import logging
import requests 
import datetime
import warnings
import threading 
import numpy as np
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP
from typing import Tuple, Dict
import traceback
import urllib.parse

from collections import Counter
from collections import OrderedDict
from pprint import pprint

warnings.filterwarnings("ignore")
# ============================================================
# TokenMap for Global Scope   ________________________________            

TOKEN_BUS = []               

# ============================================================
#   EXCHANGE CLOCK                                                        
# ============================================================

class Exchange_Clock:
    
    def __init__(self, exchange=None):
        self.exch  = str(exchange)
        self._clock = None

        self._exchange_clock()
    
    def _exchange_clock(self):
        if self.exch in ["CDS" , "Cds" , "cds"]:
            self._clock = time(9, 0)
        elif self.exch in ["MCX", "Mcx", "mcx"]:
            self._clock = time(9, 0)
        else:
            self._clock = time(9, 15)

    def clock(self):
        return self._clock

    def is_open(self):
        open = self._clock
        close = None

        if self.exch in ["CDS" , "Cds" , "cds"]:
            close= time(17, 0)  
        elif self.exch in ["MCX", "Mcx", "mcx"]:
            close = time(23, 30)      
        else:
            close = time(15, 30)

        now = datetime.now().time()

        if now < open :
            print (f"Exchange {self.exch} isn't Opened")  
            return False  
        elif now >= open and now < close:
            print (f"Exchange {self.exch} is Open")     ## Running Market  
            return True        
        elif now >= close:
            print (f"Exchange {self.exch} is Closed")
            return False
        else:
            raise Exception ("[Exchange_Clock] Unknown Error !!!")

# ============================================================
#   TOKEN REGISTRY   
# ============================================================

class TokenRegistry:
    clientCode                                      : str
    interval_parameters                             : dict
    instrument_file                                 : pd.core.frame.DataFrame
    step_df                                         : pd.core.frame.DataFrame
    index_step_dict                                 : dict
    index_underlying                                : dict
    call                                            : str
    put                                             : str

    MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

    def __init__(self, api = None , df = None):

        self.Dhan = api
        #  DATA Frame
        self.instrument_df = df
        #_______________________________________________________
        self._lock = threading.Lock()        
        self._loaded = False     
        #_______________________________________________________
        # Logging ______________________________________________
        date_str = str(datetime.datetime.now().today().date())
        if not os.path.exists('Dependencies/log_files'):
            os.makedirs('Dependencies/log_files')
        file = 'Dependencies/log_files/logs' + date_str + '.log'
        logging.basicConfig(filename=file, level=logging.DEBUG,format='%(levelname)s:%(asctime)s:%(threadName)-10s:%(message)s') 
        self.logger = logging.getLogger()
        logging.info('Dhan.py System Started')
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        self.logger.info("System Started")


        try:
            self.status                             = dict()
            self.token_and_exchange                 = dict()

            self.token_and_exchange                 = {}
            self.interval_parameters                = {'minute':  60,'2minute':  120,'3minute':  180,'4minute':  240,'5minute':  300,'day':  86400,'10minute':  600,'15minute':  900,'30minute':  1800,'60minute':  3600,'day':86400}
            self.index_underlying                   = {"NIFTY 50":"NIFTY","NIFTY BANK":"BANKNIFTY","NIFTY FIN SERVICE":"FINNIFTY","NIFTY MID SELECT":"MIDCPNIFTY"}
            self.segment_dict                       = {"NSECM": 1, "NSEFO": 2, "NSECD": 3, "BSECM": 11, "BSEFO": 12, "MCXFO": 51}
            self.index_step_dict                    = {'MIDCPNIFTY':25,'SENSEX':100,'BANKEX':100,'NIFTY': 50, 'NIFTY 50': 50, 'NIFTY BANK': 100, 'BANKNIFTY': 100, 'NIFTY FIN SERVICE': 50, 'FINNIFTY': 50}
            self.token_dict                         = {'NIFTY':{'token':26000,'exchange':'NSECM'},'NIFTY 50':{'token':26000,'exchange':'NSECM'},'BANKNIFTY':{'token':26001,'exchange':'NSECM'},'NIFTY BANK':{'token':26001,'exchange':'NSECM'},'FINNIFTY':{'token':26034,'exchange':'NSECM'},'NIFTY FIN SERVICE':{'token':26034,'exchange':'NSECM'},'MIDCPNIFTY':{'token':26121,'exchange':'NSECM'},'NIFTY MID SELECT':{'token':26121,'exchange':'NSECM'},'SENSEX':{'token':26065,'exchange':'BSECM'},'BANKEX':{'token':26118,'exchange':'BSECM'}}
            self.intervals_dict                     = {'minute': 3, '2minute':4, '3minute': 4, '5minute': 5, '10minute': 10,'15minute': 15, '30minute': 25, '60minute': 40, 'day': 80}
            # self.stock_step_df                        = {'SUNTV': 10, 'LTF': 2, 'VEDL': 10, 'SHRIRAMFIN': 10, 'GODREJPROP': 50, 'BHEL': 5, 'ATUL': 100, 'UNITDSPR': 20, 'SBIN': 10, 'PERSISTENT': 100, 'POWERGRID': 5, 'MARICO': 10, 'MOTHERSON': 2, 'HAVELLS': 20, 'BALKRISIND': 20, 'GRASIM': 20, 'MGL': 20, 'INDUSTOWER': 5, 'NATIONALUM': 5, 'DIVISLAB': 50, 'GNFC': 10, 'DLF': 10, 'AMBUJACEM': 5, 'CHOLAFIN': 20, 'IDFCFIRSTB': 1, 'CHAMBLFERT': 10, 'ABFRL': 5, 'CANFINHOME': 10, 'M&MFIN': 5, 'DABUR': 5, 'HINDCOPPER': 5, 'RAMCOCEM': 10, 'M&M': 50, 'NAVINFLUOR': 50, 'EXIDEIND': 5, 'ICICIGI': 20, 'TATAMOTORS': 10, 'GLENMARK': 20, 'POLYCAB': 100, 'CIPLA': 20, 'IOC': 2, 'INDUSINDBK': 10, 'CROMPTON': 5, 'PIDILITIND': 20, 'PIIND': 50, 'IDEA': 1, 'TATACONSUM': 10, 'METROPOLIS': 20, 'TVSMOTOR': 20, 'DEEPAKNTR': 50, 'RELIANCE': 10, 'CONCOR': 10, 'SUNPHARMA': 20, 'PETRONET': 5, 'ONGC': 2, 'ABBOTINDIA': 250, 'BHARTIARTL': 20, 'BEL': 5, 'BRITANNIA': 50, 'AARTIIND': 5, 'RBLBANK': 2, 'EICHERMOT': 50, 'SRF': 20, 'APOLLOHOSP': 50, 'GMRAIRPORT': 1, 'DRREDDY': 10, 'CANBK': 1, 'BPCL': 5, 'PEL': 20, 'ADANIPORTS': 20, 'TECHM': 20, 'ASIANPAINT': 20, 'ALKEM': 50, 'VOLTAS': 20, 'PNB': 1, 'MCX': 100, 'TATACHEM': 20, 'ZYDUSLIFE': 10, 'LICHSGFIN': 10, 'TATASTEEL': 1, 'BSOFT': 10, 'WIPRO': 2, 'SBICARD': 5, 'JUBLFOOD': 10, 'HAL': 50, 'TORNTPHARM': 50, 'CUMMINSIND': 50, 'COLPAL': 20, 'TCS': 50, 'GAIL': 2, 'IEX': 2, 'TITAN': 50, 'COALINDIA': 5, 'HDFCLIFE': 10, 'PFC': 10, 'CUB': 2, 'SHREECEM': 250, 'KOTAKBANK': 20, 'HEROMOTOCO': 50, 'BERGEPAINT': 5, 'SAIL': 2, 'MANAPPURAM': 2, 'SBILIFE': 20, 'SIEMENS': 100, 'NAUKRI': 100, 'LUPIN': 20, 'GRANULES': 10, 'MPHASIS': 50, 'RECLTD': 10, 'BANDHANBNK': 2, 'INDIAMART': 20, 'ICICIPRULI': 10, 'ULTRACEMCO': 100, 'LTIM': 100, 'DALBHARAT': 20, 'HINDUNILVR': 20, 'INDHOTEL': 10, 'MRF': 500, 'ICICIBANK': 10, 'JSWSTEEL': 10, 'ABCAPITAL': 2, 'BHARATFORG': 20, 'PVRINOX': 20, 'NMDC': 1, 'HDFCAMC': 50, 'LT': 50, 'BAJFINANCE': 200, 'INDIGO': 50, 'OFSS': 250, 'COROMANDEL': 20, 'SYNGENE': 10, 'INFY': 20, 'GODREJCP': 10, 'ABB': 100, 'DIXON': 250, 'UPL': 10, 'MARUTI': 100, 'TATACOMM': 20, 'IRCTC': 10, 'OBEROIRLTY': 20, 'BIOCON': 5, 'GUJGASLTD': 5, 'BAJAJFINSV': 20, 'MFSL': 20, 'HINDALCO': 10, 'HDFCBANK': 20, 'BOSCHLTD': 500, 'AUROPHARMA': 20, 'AXISBANK': 10, 'MUTHOOTFIN': 20, 'JKCEMENT': 50, 'TATAPOWER': 5, 'APOLLOTYRE': 10, 'UBL': 20, 'LALPATHLAB': 50, 'IPCALAB': 20, 'FEDERALBNK': 2, 'LAURUSLABS': 10, 'ADANIENT': 40, 'ACC': 20, 'JINDALSTEL': 20, 'COFORGE': 100, 'ASHOKLEY': 2, 'ASTRAL': 20, 'PAGEIND': 500, 'ESCORTS': 50, 'NESTLEIND': 20, 'BANKBARODA': 2, 'HINDPETRO': 5, 'HCLTECH': 20, 'TRENT': 100, 'BATAINDIA': 10, 'LTTS': 50, 'IGL': 2, 'AUBANK': 5, 'NTPC': 5, 'PAYTM': 20, 'TIINDIA': 50, 'OIL': 10, 'JSL': 10, 'ZOMATO': 5, 'JSWENERGY': 10, 'VBL': 10, 'ADANIENSOL': 20, 'CGPOWER': 10, 'SONACOMS': 10, 'JIOFIN': 5, 'NCC': 5, 'UNIONBANK': 1, 'CYIENT': 20, 'YESBANK': 1, 'LICI': 10, 'HFCL': 2, 'BANKINDIA': 1, 'ADANIGREEN': 20, 'IRB': 1, 'NHPC': 1, 'DELHIVERY': 5, 'PRESTIGE': 50, 'ATGL': 10, 'SJVN': 2, 'CESC': 5, 'MAXHEALTH': 20, 'IRFC': 2, 'APLAPOLLO': 20, 'KPITTECH': 20, 'LODHA': 20, 'DMART': 50, 'INDIANB': 10, 'KALYANKJIL': 20, 'POLICYBZR': 50, 'HUDCO': 5, 'ANGELONE': 200, 'NYKAA': 2, 'KEI': 100, 'SUPREMEIND': 100, 'POONAWALLA': 5, 'TATAELXSI': 100, 'CAMS': 100, 'ITC': 5, 'NBCC':2}
            self.stock_step_df = self.dhan_equity_step_creation()
            self.stock_step_df.update({'BAJAJ-AUTO': 100})
            self.commodity_step_dict                = {'GOLD': 100,'SILVER': 250,'CRUDEOIL': 50,'NATURALGAS': 5,'COPPER': 5,'NICKEL': 10,'ZINC': 2.5,'LEAD': 1, 'ALUMINIUM': 1,    'COTTON': 100,     'MENTHAOIL': 10,   'GOLDM': 50,       'GOLDPETAL': 5,    'GOLDGUINEA': 10,  'SILVERM': 250,     'SILVERMIC': 10,   'BRASS': 5,        'CASTORSEED': 100, 'COTTONSEEDOILCAKE''CARDAMOM': 50,    'RBDPALMOLEIN': 10,'CRUDEPALMOIL': 10,'PEPPER': 100,     'JEERA': 100,      'SOYABEAN': 50,    'SOYAOIL': 10,     'TURMERIC': 100,   'GUARGUM': 100,    'GUARSEED': 100,   'CHANA': 50,       'MUSTARDSEED': 50, 'BARLEY': 50,      'SUGARM': 50,      'WHEAT': 50,       'MAIZE': 50,       'PADDY': 50,       'BAJRA': 50,       'JUTE': 50,        'RUBBER': 100,     'COFFEE': 50,      'COPRA': 50,       'SESAMESEED': 50,  'TEA': 100,        'KAPAS': 100,      'BARLEYFEED': 50,  'RAPESEED': 50,    'LINSEED': 50,     'SUNFLOWER': 50,   'CORIANDER': 50,   'CUMINSEED': 100   }
            self.start_date, self.end_date          = self.get_start_date()
            self.correct_list                       = {'SUNTV': 10, 'LTF': 2, 'VEDL': 10, 'SHRIRAMFIN': 10, 'GODREJPROP': 50, 'BHEL': 5, 'ATUL': 100, 'UNITDSPR': 20, 'SBIN': 10, 'PERSISTENT': 100, 'POWERGRID': 5, 'MARICO': 10, 'MOTHERSON': 2, 'HAVELLS': 20, 'BALKRISIND': 20, 'GRASIM': 20, 'MGL': 20, 'INDUSTOWER': 5, 'NATIONALUM': 5, 'DIVISLAB': 50, 'GNFC': 10, 'DLF': 10, 'AMBUJACEM': 5, 'CHOLAFIN': 20, 'IDFCFIRSTB': 1, 'CHAMBLFERT': 10, 'ABFRL': 5, 'CANFINHOME': 10, 'M&MFIN': 5, 'DABUR': 5, 'HINDCOPPER': 5, 'RAMCOCEM': 10, 'M&M': 50, 'NAVINFLUOR': 50, 'EXIDEIND': 5, 'ICICIGI': 20, 'TATAMOTORS': 10, 'GLENMARK': 20, 'POLYCAB': 100, 'CIPLA': 20, 'IOC': 2, 'INDUSINDBK': 10, 'CROMPTON': 5, 'PIDILITIND': 20, 'PIIND': 50, 'IDEA': 1, 'TATACONSUM': 10, 'METROPOLIS': 20, 'TVSMOTOR': 20, 'DEEPAKNTR': 50, 'RELIANCE': 10, 'CONCOR': 10, 'SUNPHARMA': 20, 'PETRONET': 5, 'ONGC': 2, 'ABBOTINDIA': 250, 'BHARTIARTL': 20, 'BEL': 5, 'BRITANNIA': 50, 'AARTIIND': 5, 'RBLBANK': 2, 'EICHERMOT': 50, 'SRF': 20, 'APOLLOHOSP': 50, 'GMRAIRPORT': 1, 'DRREDDY': 10, 'CANBK': 1, 'BPCL': 5, 'PEL': 20, 'ADANIPORTS': 20, 'TECHM': 20, 'ASIANPAINT': 20, 'ALKEM': 50, 'VOLTAS': 20, 'PNB': 1, 'MCX': 100, 'TATACHEM': 20, 'ZYDUSLIFE': 10, 'LICHSGFIN': 10, 'TATASTEEL': 1, 'BSOFT': 10, 'WIPRO': 2, 'SBICARD': 5, 'JUBLFOOD': 10, 'HAL': 50, 'TORNTPHARM': 50, 'CUMMINSIND': 50, 'COLPAL': 20, 'TCS': 50, 'GAIL': 2, 'IEX': 2, 'TITAN': 50, 'COALINDIA': 5, 'HDFCLIFE': 10, 'PFC': 10, 'CUB': 2, 'SHREECEM': 250, 'KOTAKBANK': 20, 'HEROMOTOCO': 50, 'BERGEPAINT': 5, 'SAIL': 2, 'MANAPPURAM': 2, 'SBILIFE': 20, 'SIEMENS': 100, 'NAUKRI': 100, 'LUPIN': 20, 'GRANULES': 10, 'MPHASIS': 50, 'RECLTD': 10, 'BANDHANBNK': 2, 'INDIAMART': 20, 'ICICIPRULI': 10, 'ULTRACEMCO': 100, 'LTIM': 100, 'DALBHARAT': 20, 'HINDUNILVR': 20, 'INDHOTEL': 10, 'MRF': 500, 'ICICIBANK': 10, 'JSWSTEEL': 10, 'ABCAPITAL': 2, 'BHARATFORG': 20, 'PVRINOX': 20, 'NMDC': 1, 'HDFCAMC': 50, 'LT': 50, 'BAJFINANCE': 200, 'INDIGO': 50, 'OFSS': 250, 'COROMANDEL': 20, 'SYNGENE': 10, 'INFY': 20, 'GODREJCP': 10, 'ABB': 100, 'DIXON': 250, 'UPL': 10, 'MARUTI': 100, 'TATACOMM': 20, 'IRCTC': 10, 'OBEROIRLTY': 20, 'BIOCON': 5, 'GUJGASLTD': 5, 'BAJAJFINSV': 20, 'MFSL': 20, 'HINDALCO': 10, 'HDFCBANK': 20, 'BOSCHLTD': 500, 'AUROPHARMA': 20, 'AXISBANK': 10, 'MUTHOOTFIN': 20, 'JKCEMENT': 50, 'TATAPOWER': 5, 'APOLLOTYRE': 10, 'UBL': 20, 'LALPATHLAB': 50, 'IPCALAB': 20, 'FEDERALBNK': 2, 'LAURUSLABS': 10, 'ADANIENT': 40, 'ACC': 20, 'JINDALSTEL': 20, 'COFORGE': 100, 'ASHOKLEY': 2, 'ASTRAL': 20, 'PAGEIND': 500, 'ESCORTS': 50, 'NESTLEIND': 20, 'BANKBARODA': 2, 'HINDPETRO': 5, 'HCLTECH': 20, 'TRENT': 100, 'BATAINDIA': 10, 'LTTS': 50, 'IGL': 2, 'AUBANK': 5, 'NTPC': 5, 'PAYTM': 20, 'TIINDIA': 50, 'OIL': 10, 'JSL': 10, 'ZOMATO': 5, 'JSWENERGY': 10, 'VBL': 10, 'ADANIENSOL': 20, 'CGPOWER': 10, 'SONACOMS': 10, 'JIOFIN': 5, 'NCC': 5, 'UNIONBANK': 1, 'CYIENT': 20, 'YESBANK': 1, 'LICI': 10, 'HFCL': 2, 'BANKINDIA': 1, 'ADANIGREEN': 20, 'IRB': 1, 'NHPC': 1, 'DELHIVERY': 5, 'PRESTIGE': 50, 'ATGL': 10, 'SJVN': 2, 'CESC': 5, 'MAXHEALTH': 20, 'IRFC': 2, 'APLAPOLLO': 20, 'KPITTECH': 20, 'LODHA': 20, 'DMART': 50, 'INDIANB': 10, 'KALYANKJIL': 20, 'POLICYBZR': 50, 'HUDCO': 5, 'ANGELONE': 200, 'NYKAA': 2, 'KEI': 100, 'SUPREMEIND': 100, 'POONAWALLA': 5, 'TATAELXSI': 100, 'CAMS': 100, 'ITC': 5, 'NBCC':2}
        
        except Exception as e:
            print(e)
            traceback.print_exc()

# =======================================================================================================================
    def correct_step_df_creation(self):
        
        self.correct_list = {} 
        instrument_df = self.instrument_df.copy()
        
        names_list = instrument_df['SEM_CUSTOM_SYMBOL'].str.split(' ').str[0].unique().tolist()
        names_list = [name for name in names_list if isinstance(name, str) and '-' not in name and '%' not in name]

        
        for name in names_list:
            if '-' in name or '%' in name:
                continue
            try:
                filtered_df = instrument_df[
                    (instrument_df['SEM_CUSTOM_SYMBOL'].str.contains(name, na=False)) &
                    (instrument_df['SEM_EXM_EXCH_ID'] == 'NSE') &
                    (instrument_df['SEM_EXCH_INSTRUMENT_TYPE'] == 'OP')
                ]
                if filtered_df.empty:
                    continue
                expiry_dates = filtered_df['SEM_EXPIRY_DATE'].unique()
                if len(expiry_dates) == 0:
                    raise ValueError(f"No expiry date found for {name}")
                
                expiry = expiry_dates[0]
                ce_condition = (
                    (filtered_df['SEM_TRADING_SYMBOL'].str.startswith(name + '-')) &
                    (filtered_df['SEM_CUSTOM_SYMBOL'].str.contains(name)) &
                    (filtered_df['SEM_EXPIRY_DATE'] == expiry) &
                    (filtered_df['SEM_OPTION_TYPE'] == 'CE')
                )
                
                new_df = filtered_df.loc[ce_condition].copy()
                new_df['SEM_STRIKE_PRICE'] = new_df['SEM_STRIKE_PRICE'].astype(int)
                
                sorted_strikes = sorted(new_df['SEM_STRIKE_PRICE'].to_list())
                differences = [sorted_strikes[i + 1] - sorted_strikes[i] for i in range(len(sorted_strikes) - 1)]
                
                difference_counts = Counter(differences)
                step_value, max_frequency = difference_counts.most_common(1)[0]
                
                self.stock_step_df[name] = step_value
                self.correct_list[name] = step_value
                print(f"Correct list for {name} is {self.correct_list}")

            except Exception as e:
                self.logger.exception(f"Error processing {name}: {e}")
# ________________________________________________________________________________________________________________________


    def dhan_equity_step_creation(self) -> dict:
        """
        Build step_size size dictionary for all stock option underlyings (OPTSTK)
        using Dhan instrument file.
        """
        try:
            df = self.instrument_df.copy()

            opt_df = df[(df["SEM_INSTRUMENT_NAME"] == "OPTSTK") & (df["SEM_OPTION_TYPE"] == "CE") & (df["SEM_EXM_EXCH_ID"] == "NSE")].copy()
            opt_df["UNDERLYING"] = opt_df["SEM_TRADING_SYMBOL"].apply(lambda s: s.split("-")[0])
            if opt_df.empty:
                return {}
            opt_df["SEM_STRIKE_PRICE"] = opt_df["SEM_STRIKE_PRICE"].astype(float)
            step_dict = {}
            for symbol, group in opt_df.groupby("UNDERLYING"):
                try:
                    
                    group = group.sort_values("SEM_EXPIRY_DATE")
                    nearest_expiry = group["SEM_EXPIRY_DATE"].iloc[0]
                    g2 = group[group["SEM_EXPIRY_DATE"] == nearest_expiry]
                    if len(g2) < 2:
                        continue
                    strikes = np.sort(g2["SEM_STRIKE_PRICE"].values)
                    diffs = np.diff(strikes)
                    if len(diffs) == 0:
                        continue
                    step_size = Counter(diffs).most_common(1)[0][0]
                    step_size = int(step_size) if float(step_size).is_integer() else step_size
                    step_dict[symbol] = step_size
                except Exception:
                    continue
            return step_dict
        except Exception as e:
            print("Error in dhan_equity_step_creation:", e)
            return {}


# ________________________________________________________________________________________________________________________


    def get_ltp_data(self,names, debug="NO"):
        try:      
            instrument_names = {}
            instrument_df = self.instrument_df.copy()

            instruments = {'NSE_EQ':[],'IDX_I':[],'NSE_FNO':[],'NSE_CURRENCY':[],'BSE_EQ':[],'BSE_FNO':[],'BSE_CURRENCY':[],'MCX_COMM':[]}
            NFO = ["BANKNIFTY","NIFTY","MIDCPNIFTY","FINNIFTY"]
            BFO = ['SENSEX','BANKEX']
            equity = ['CALL','PUT','FUT']       
            exchange_index = {"BANKNIFTY": "NSE_IDX","NIFTY":"NSE_IDX","MIDCPNIFTY":"NSE_IDX", "FINNIFTY":"NSE_IDX","SENSEX":"BSE_IDX","BANKEX":"BSE_IDX", "INDIA VIX":"IDX_I"}
            if not isinstance(names, list):
                names = [names]
            for name in names:
                try:
                    name = name.upper()
                    if name in exchange_index.keys():
                        security_check = instrument_df[((instrument_df['SEM_CUSTOM_SYMBOL']==name)|(instrument_df['SEM_TRADING_SYMBOL']==name))]
                        if security_check.empty:
                            raise Exception("Check the Tradingsymbol")
                        security_id = security_check.iloc[-1]['SEM_SMST_SECURITY_ID']
                        instruments['IDX_I'].append(int(security_id))
                        instrument_names[str(security_id)]=name
                    elif name in self.commodity_step_dict.keys():
                        security_check = instrument_df[(instrument_df['SEM_EXM_EXCH_ID']=='MCX')&(instrument_df['SM_SYMBOL_NAME']==name.upper())&(instrument_df['SEM_INSTRUMENT_NAME']=='FUTCOM')]                      
                        if security_check.empty:
                            raise Exception("Check the Tradingsymbol")
                        security_id = security_check.sort_values(by='SEM_EXPIRY_DATE').iloc[0]['SEM_SMST_SECURITY_ID']
                        instruments['MCX_COMM'].append(int(security_id))
                        instrument_names[str(security_id)]=name
                    else:
                        security_check = instrument_df[((instrument_df['SEM_CUSTOM_SYMBOL']==name)|(instrument_df['SEM_TRADING_SYMBOL']==name))]
                        if security_check.empty:
                            raise Exception("Check the Tradingsymbol")                      
                        security_id = security_check.iloc[-1]['SEM_SMST_SECURITY_ID']
                        nfo_check = ['NSE_FNO' for nfo in NFO if nfo in name]
                        bfo_check = ['BSE_FNO' for bfo in BFO if bfo in name]
                        exchange_nfo ='NSE_FNO' if len(nfo_check)!=0 else False
                        exchange_bfo = 'BSE_FNO' if len(bfo_check)!=0 else False
                        if not exchange_nfo and not exchange_bfo:
                            eq_check =['NSE_FNO' for nfo in equity if nfo in name]
                            exchange_eq ='NSE_FNO' if len(eq_check)!=0 else "NSE_EQ"
                        else:
                            exchange_eq="NSE_EQ"
                        exchange ='NSE_FNO' if exchange_nfo else ('BSE_FNO' if exchange_bfo else exchange_eq)
                        trail_exchange = exchange
                        mcx_check = ['MCX_COMM' for mcx in self.commodity_step_dict.keys() if mcx in name]
                        exchange = "MCX_COMM" if len(mcx_check)!=0 else exchange
                        if exchange == "MCX_COMM": 
                            if instrument_df[((instrument_df['SEM_CUSTOM_SYMBOL']==name)|(instrument_df['SEM_TRADING_SYMBOL']==name))&(instrument_df['SEM_EXM_EXCH_ID']=='MCX')].empty:
                                exchange = trail_exchange
                        if exchange == "MCX_COMM":
                            security_check = instrument_df[((instrument_df['SEM_CUSTOM_SYMBOL']==name)|(instrument_df['SEM_TRADING_SYMBOL']==name))&(instrument_df['SEM_EXM_EXCH_ID']=='MCX')]
                            if security_check.empty:
                                raise Exception("Check the Tradingsymbol")  
                            security_id = security_check.iloc[-1]['SEM_SMST_SECURITY_ID']
                        instruments[exchange].append(int(security_id))
                        instrument_names[str(security_id)]=name
                except Exception as e:
                    print(f"Exception for instrument name {name} as {e}")
                    continue
            time.sleep(0.4)

            # Data Fetching via instrument
            data = self.Dhan.ticker_data(instruments)
            ltp_data=dict()
            
            if debug.upper()=="YES":
                print(data)         
            if data['status']!='failure':
                inner = data["data"]["data"]
                for exchange, sec_dict in inner.items():
                    for sec_id, quotes in sec_dict.items():
                        if sec_id in instrument_names:
                            symbol = instrument_names[sec_id]
                            ltp_data[symbol] = float(quotes["last_price"])
            else:
                raise Exception(data)
            
            return ltp_data
        except Exception as e:
            print(f"Exception at calling ltp as {e}")
            self.logger.exception(f"Exception at calling ltp as {e}")
            return dict()
        
# ________________________________________________________________________________________________________________________


    def order_placement(self,tradingsymbol:str, exchange:str,quantity:int, price:int, trigger_price:int, order_type:str, transaction_type:str, trade_type:str,disclosed_quantity=0,after_market_order=False,validity ='DAY', amo_time='OPEN',bo_profit_value=None, bo_stop_loss_value=None, tag=None, should_slice=False)->str:
        try:
            instrument_df = self.instrument_df.copy()
            tradingsymbol = tradingsymbol.upper()
            exchange = exchange.upper()

            script_exchange = {"NSE":self.Dhan.NSE, "NFO":self.Dhan.NSE_FNO, "BFO":self.Dhan.BSE_FNO, "CUR": self.Dhan.CUR, "BSE":self.Dhan.BSE, "MCX":self.Dhan.MCX}
            self.order_Type = {'LIMIT': self.Dhan.LIMIT, 'MARKET': self.Dhan.MARKET,'STOPLIMIT': self.Dhan.SL, 'STOPMARKET': self.Dhan.SLM}
            product = {'MIS':self.Dhan.INTRA, 'MARGIN':self.Dhan.MARGIN, 'MTF':self.Dhan.MTF, 'CO':self.Dhan.CO,'BO':self.Dhan.BO, 'CNC': self.Dhan.CNC}
            Validity = {'DAY': "DAY", 'IOC': 'IOC'}
            transactiontype = {'BUY': self.Dhan.BUY, 'SELL': self.Dhan.SELL}
            instrument_exchange = {'NSE':"NSE",'BSE':"BSE",'NFO':'NSE','BFO':'BSE','MCX':'MCX','CUR':'NSE'}
            amo_time_check = ['PRE_OPEN', 'OPEN', 'OPEN_30', 'OPEN_60']
            if after_market_order:
                if amo_time.upper() in ['OPEN', 'OPEN_30', 'OPEN_60']:
                    amo_time = amo_time.upper()
                else:
                    raise Exception("amo_time value must be ['PRE_OPEN','OPEN','OPEN_30','OPEN_60']")           
            exchangeSegment = script_exchange[exchange]
            product_Type = product[trade_type.upper()]
            order_type = self.order_Type[order_type.upper()]
            order_side = transactiontype[transaction_type.upper()]
            time_in_force = Validity[validity.upper()]
            security_check = instrument_df[((instrument_df['SEM_TRADING_SYMBOL']==tradingsymbol)|(instrument_df['SEM_CUSTOM_SYMBOL']==tradingsymbol))&(instrument_df['SEM_EXM_EXCH_ID']==instrument_exchange[exchange])]
            if security_check.empty:
                raise Exception("Check the Tradingsymbol")
            security_id = security_check.iloc[-1]['SEM_SMST_SECURITY_ID']
            order = self.Dhan.place_order(security_id=str(security_id), exchange_segment=exchangeSegment,
                                               transaction_type=order_side, quantity=int(quantity),
                                               order_type=order_type, product_type=product_Type, price=float(price),
                                               trigger_price=float(trigger_price),disclosed_quantity=int(disclosed_quantity),
                                                after_market_order=after_market_order, validity=time_in_force, amo_time=amo_time,
                                                bo_profit_value=bo_profit_value, bo_stop_loss_Value=bo_stop_loss_value, tag = tag, should_slice=should_slice)
            
            if order['status']=='failure':
                raise Exception(order)
            if should_slice:
                orderid = [x["orderId"] for x in order["data"]]
                if len(orderid) == 1:
                    print(f"Order is not sliced, so returning the single order ID")
                    orderid = orderid[0]
                    orderid = str(orderid)
            else:
                orderid = str(order["data"]["orderId"])
            return orderid
        except Exception as e:
            print(f"'Got exception in place_order as {e}")
            return None

# ________________________________________________________________________________________________________________________


    def modify_order(self, order_id, order_type, quantity, price=0, trigger_price=0, disclosed_quantity=0, validity='DAY',leg_name = None):
        try:
            self.order_Type = {'LIMIT': self.Dhan.LIMIT, 'MARKET': self.Dhan.MARKET,'STOPLIMIT': self.Dhan.SL, 'STOPMARKET': self.Dhan.SLM}
            Validity = {'DAY': "DAY", 'IOC': 'IOC'}
            order_type = self.order_Type[order_type.upper()]
            time_in_force = Validity[validity.upper()]
            leg_name_check = ['ENTRY_LEG','TARGET_LEG','STOP_LOSS_LEG']
            if leg_name is not None:
                if leg_name.upper() in leg_name_check:
                    leg_name = leg_name.upper()
                else:
                    raise Exception(f'Leg Name value must be "["ENTRY_LEG","TARGET_LEG","STOP_LOSS_LEG"]"')
                
            response = self.Dhan.modify_order(order_id =order_id, order_type=order_type, leg_name=leg_name, quantity=int(quantity), price=float(price), trigger_price=float(trigger_price), disclosed_quantity=int(disclosed_quantity), validity=time_in_force)
            if response['status']=='failure':
                raise Exception(response)
            else:
                orderid = response["data"]["orderId"]
                return str(orderid)
        except Exception as e:
            print(f'Got exception in modify_order as {e}')

# ________________________________________________________________________________________________________________________


    def get_order_detail(self,orderid:str, debug= "NO")->dict:
        try:
            if orderid is None:
                raise Exception('Check the order id, Error as None')
            orderid = str(orderid)
            time.sleep(1)
            response = self.Dhan.get_order_by_id(orderid)
            if debug.upper()=="YES":
                print(response)
            if response['status']=='success':
                return response['data'][0]
            else:
                raise Exception(response)
        except Exception as e:
            print(f"Error at getting order details as {e}")
            return {
                'status':'failure',
                'remarks':str(e),
                'data':response,
            }
    
    def get_order_status(self, orderid:str, debug= "NO")->str:
        try:
            if orderid is None:
                raise Exception('Check the order id, Error as None')            
            orderid = str(orderid)
            time.sleep(1)
            response = self.Dhan.get_order_by_id(orderid)
            if debug.upper()=="YES":
                print(response)         
            if response['status']=='success':
                return response['data'][0]['orderStatus']
            else:
                raise Exception(response)
        except Exception as e:
            print(f"Error at getting order status as {e}")
            return {
                'status':'failure',
                'remarks':str(e),
                'data':response,
            }   

# ________________________________________________________________________________________________________________________


    def convert_to_date_time(self,epoch):
        return self.Dhan.convert_to_date_time(self.Dhan,epoch)
    
    def get_start_date(self):
        try:
            instrument_df = self.instrument_df.copy()

            from_date= datetime.datetime.now()-datetime.timedelta(days=90)
            start_date = (datetime.datetime.now()-datetime.timedelta(days=90)).strftime('%Y-%m-%d')
            from_date = from_date.strftime('%Y-%m-%d')
            to_date = datetime.datetime.now().strftime('%Y-%m-%d')
            instrument_exchange = {'NSE':"NSE",'BSE':"BSE",'NFO':'NSE','BFO':'BSE','MCX':'MCX','CUR':'NSE'}
            tradingsymbol = "NIFTY"
            exchange = "NSE"
            exchange_segment = self.Dhan.INDEX
            security_id     = instrument_df[((instrument_df['SEM_TRADING_SYMBOL']==tradingsymbol)|(instrument_df['SEM_CUSTOM_SYMBOL']==tradingsymbol))&(instrument_df['SEM_EXM_EXCH_ID']==instrument_exchange[exchange])].iloc[-1]['SEM_SMST_SECURITY_ID']
            instrument_type = instrument_df[((instrument_df['SEM_TRADING_SYMBOL']==tradingsymbol)|(instrument_df['SEM_CUSTOM_SYMBOL']==tradingsymbol))&(instrument_df['SEM_EXM_EXCH_ID']==instrument_exchange[exchange])].iloc[-1]['SEM_INSTRUMENT_NAME']
            expiry_code     = instrument_df[((instrument_df['SEM_TRADING_SYMBOL']==tradingsymbol)|(instrument_df['SEM_CUSTOM_SYMBOL']==tradingsymbol))&(instrument_df['SEM_EXM_EXCH_ID']==instrument_exchange[exchange])].iloc[-1]['SEM_EXPIRY_CODE']
            time.sleep(0.5)

            # Data Fetching
            ohlc = self.Dhan.historical_daily_data(int(security_id),exchange_segment,instrument_type,from_date,to_date,int(expiry_code), oi = True)
            if ohlc['status']!='failure':
                df = pd.DataFrame(ohlc['data'])
                if not df.empty:
                    df['timestamp'] = df['timestamp'].apply(lambda x: self.convert_to_date_time(x))
                    start_date = df.iloc[0]['timestamp']
                    start_date = start_date.strftime('%Y-%m-%d')
                    return start_date, to_date
                else:
                    return start_date, to_date
            else:
                return start_date, to_date          
        except Exception as e:
            self.logger.exception(f"Error at getting start date as {e}")
            return start_date, to_date


# ________________________________________________________________________________________________________________________


    def get_historical_data(self,tradingsymbol,exchange,timeframe, sector = "NO",debug="NO"):           
        try:
            # tradingsymbol = tradingsymbol.upper()
            instrument_df = self.instrument_df.copy()
            exchange = exchange.upper()

            from_date= datetime.datetime.now()-datetime.timedelta(days=365)
            from_date = from_date.strftime('%Y-%m-%d')
            to_date = datetime.datetime.now().strftime('%Y-%m-%d') 
            script_exchange = {"NSE":self.Dhan.NSE, "NFO":self.Dhan.NSE_FNO, "BFO":self.Dhan.BSE_FNO, "CUR": self.Dhan.CUR, "BSE":self.Dhan.BSE, "MCX":self.Dhan.MCX, "INDEX":self.Dhan.INDEX}
            instrument_exchange = {'NSE':"NSE",'BSE':"BSE",'NFO':'NSE','BFO':'BSE','MCX':'MCX','CUR':'NSE'}
            exchange_segment = script_exchange[exchange]
            index_exchange = {"NIFTY":'NSE',"BANKNIFTY":"NSE","FINNIFTY":"NSE","MIDCPNIFTY":"NSE","BANKEX":"BSE","SENSEX":"BSE"}
            
            if tradingsymbol in index_exchange:
                exchange =index_exchange[tradingsymbol]
            if sector.upper()=="YES":
                security_check = instrument_df[((instrument_df['SEM_TRADING_SYMBOL']==tradingsymbol)|(instrument_df['SEM_CUSTOM_SYMBOL']==tradingsymbol))&(instrument_df['SEM_EXM_EXCH_ID']==exchange)]
                if security_check.empty:
                    raise Exception("Check the Tradingsymbol or Exchange")
                security_id = security_check.iloc[-1]['SEM_SMST_SECURITY_ID']
                exchange_segment = "IDX_I"
            if tradingsymbol in self.commodity_step_dict.keys():
                security_check = instrument_df[(instrument_df['SEM_EXM_EXCH_ID']=='MCX')&(instrument_df['SM_SYMBOL_NAME']==tradingsymbol.upper())&(instrument_df['SEM_INSTRUMENT_NAME']=='FUTCOM')]                     
                if security_check.empty:
                    raise Exception("Check the Tradingsymbol or Exchange")
                security_id = security_check.sort_values(by='SEM_EXPIRY_DATE').iloc[0]['SEM_SMST_SECURITY_ID']
                tradingsymbol = security_check.sort_values(by='SEM_EXPIRY_DATE').iloc[0]['SEM_CUSTOM_SYMBOL']
            else:                       
                security_check = instrument_df[((instrument_df['SEM_TRADING_SYMBOL']==tradingsymbol)|(instrument_df['SEM_CUSTOM_SYMBOL']==tradingsymbol))&(instrument_df['SEM_EXM_EXCH_ID']==instrument_exchange[exchange])]
                if security_check.empty:
                    raise Exception("Check the Tradingsymbol or Exchange")
                security_id = security_check.iloc[-1]['SEM_SMST_SECURITY_ID']                       
            Symbol          = instrument_df[((instrument_df['SEM_TRADING_SYMBOL']==tradingsymbol)|(instrument_df['SEM_CUSTOM_SYMBOL']==tradingsymbol))&(instrument_df['SEM_EXM_EXCH_ID']==instrument_exchange[exchange])].iloc[-1]['SEM_TRADING_SYMBOL']
            instrument_type = instrument_df[((instrument_df['SEM_TRADING_SYMBOL']==tradingsymbol)|(instrument_df['SEM_CUSTOM_SYMBOL']==tradingsymbol))&(instrument_df['SEM_EXM_EXCH_ID']==instrument_exchange[exchange])].iloc[-1]['SEM_INSTRUMENT_NAME']
            if 'FUT' in instrument_type and timeframe.upper()=="DAY":
                raise Exception('For Future or Commodity, DAY - Timeframe not supported by API, SO choose another timeframe')           
            expiry_code     = instrument_df[((instrument_df['SEM_TRADING_SYMBOL']==tradingsymbol)|(instrument_df['SEM_CUSTOM_SYMBOL']==tradingsymbol))&(instrument_df['SEM_EXM_EXCH_ID']==instrument_exchange[exchange])].iloc[-1]['SEM_EXPIRY_CODE']
            if timeframe in ['1', '5', '15', '25', '60']:
                interval = int(timeframe)
            elif timeframe.upper()=="DAY":
                pass
            else:
                raise Exception("interval value must be ['1','5','15','25','60','DAY']")
            
            if timeframe.upper() == "DAY":
                ohlc = self.Dhan.historical_daily_data(int(security_id),exchange_segment,instrument_type,from_date,to_date,int(expiry_code), oi = True)
            else:
                ohlc = self.Dhan.intraday_minute_data(str(security_id),exchange_segment,instrument_type,self.start_date,self.end_date,int(interval), oi = True)
            
            if debug.upper()=="YES":
                print(ohlc)
            
            if ohlc['status']!='failure':
                df = pd.DataFrame(ohlc['data'])
                if not df.empty:
                    df['timestamp'] = df['timestamp'].apply(lambda x: self.convert_to_date_time(x))
                    
                    return df
                else:
                    return df
            else:
                raise Exception(ohlc) 
        except Exception as e:
            print(f"Exception in Getting OHLC data as {e}")
            self.logger.exception(f"Exception in Getting OHLC data as {e}")
            # traceback.print_exc()


# ________________________________________________________________________________________________________________________


    def resample_timeframe(self, df, timeframe='5T'):
        try:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            
            market_start = pd.to_datetime("09:15:00").time()
            market_end = pd.to_datetime("15:30:00").time()
            timezone = pytz.timezone('Asia/Kolkata')
                        
            resampled_data = []
            for date, group in df.groupby(df.index.date):
                origin_time = timezone.localize(pd.Timestamp(f"{date} 09:15:00"))
                daily_data = group.between_time(market_start, market_end)
                if not daily_data.empty:
                    resampled = daily_data.resample(timeframe, origin=origin_time).agg({
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last',
                        'volume': 'sum'
                    }).dropna(how='all') 
                    resampled_data.append(resampled)
            if resampled_data:
                resampled_df = pd.concat(resampled_data)
            else:
                resampled_df = pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            resampled_df.reset_index(inplace=True)
            return resampled_df
        except Exception as e:
            self.logger.exception(f"Error in resampling timeframe: {e}")
            return pd.DataFrame()

# ________________________________________________________________________________________________________________________


    def get_lot_size(self,tradingsymbol: str):

        instrument_df = self.instrument_df.copy()

        data = instrument_df[((instrument_df['SEM_TRADING_SYMBOL']==tradingsymbol)|(instrument_df['SEM_CUSTOM_SYMBOL']==tradingsymbol))]
        if len(data) == 0:
            self.logger.exception("Enter valid Script Name")
            print("Enter valid Script Name")
            return 0
        else:
            return int(data.iloc[0]['SEM_LOT_UNITS'])


# ________________________________________________________________________________________________________________________


    def ATM_Strike_Selection(self, Underlying, Expiry):
        try:
            instrument_df = self.instrument_df.copy()
            Underlying = Underlying.upper()
            strike = 0
            
            exchange_index = {"BANKNIFTY": "NSE","NIFTY":"NSE","MIDCPNIFTY":"NSE", "FINNIFTY":"NSE","SENSEX":"BSE","BANKEX":"BSE"}
            instrument_df['SEM_EXPIRY_DATE'] = pd.to_datetime(instrument_df['SEM_EXPIRY_DATE'], errors='coerce')
            instrument_df['ContractExpiration'] = instrument_df['SEM_EXPIRY_DATE'].dt.date
            instrument_df['ContractExpiration'] = instrument_df['ContractExpiration'].astype(str)
            if Underlying in exchange_index:
                exchange = exchange_index[Underlying]
                expiry_exchange = 'INDEX'
            elif Underlying in self.commodity_step_dict.keys():
                exchange = "MCX"
                expiry_exchange = exchange
            else:
                # exchange = instrument_df[((instrument_df['SEM_TRADING_SYMBOL']==Underlying)|(instrument_df['SEM_CUSTOM_SYMBOL']==Underlying))].iloc[0]['SEM_EXM_EXCH_ID']
                exchange = "NSE"
                expiry_exchange = exchange
            expiry_list = self.get_expiry_list(Underlying=Underlying, exchange = expiry_exchange)
            if len(expiry_list)==0:
                print(f"Unable to find the correct Expiry for {Underlying}")
                return None
            if len(expiry_list)<Expiry:
                Expiry_date = expiry_list[-1]
            else:
                Expiry_date = expiry_list[Expiry]
            
            # Data Fetching    
            ltp_data = self.get_ltp_data(Underlying)
            ltp = ltp_data[Underlying]
            
            if Underlying in self.index_step_dict:
                step_size = self.index_step_dict[Underlying]
            elif Underlying in self.stock_step_df:
                step_size = self.stock_step_df[Underlying]
            elif Underlying in self.commodity_step_dict:
                step_size = self.commodity_step_dict[Underlying]
            else:
                data = f'{Underlying} Not in the step_size list'
                raise Exception(data)
            strike = round(ltp/step_size) * step_size
            
            if Underlying in self.index_step_dict:
                ce_condition = (instrument_df['SEM_EXM_EXCH_ID'] == exchange) & ((instrument_df['SEM_TRADING_SYMBOL'].str.startswith(Underlying))|(instrument_df['SEM_CUSTOM_SYMBOL'].str.startswith(Underlying))) & (instrument_df['ContractExpiration'] == Expiry_date) & (instrument_df['SEM_OPTION_TYPE']=='CE') 
                pe_condition = (instrument_df['SEM_EXM_EXCH_ID'] == exchange) & ((instrument_df['SEM_TRADING_SYMBOL'].str.startswith(Underlying))|(instrument_df['SEM_CUSTOM_SYMBOL'].str.startswith(Underlying))) & (instrument_df['ContractExpiration'] == Expiry_date) & (instrument_df['SEM_OPTION_TYPE']=='PE')
            elif exchange =="MCX":      
                ce_condition = (instrument_df['SEM_EXM_EXCH_ID'] == exchange) & ((instrument_df['SEM_TRADING_SYMBOL'].str.startswith(Underlying))|(instrument_df['SEM_CUSTOM_SYMBOL'].str.startswith(Underlying))) & (instrument_df['ContractExpiration'] == Expiry_date) & (instrument_df['SEM_OPTION_TYPE']=='CE') & (instrument_df['SM_SYMBOL_NAME']==Underlying) 
                pe_condition = (instrument_df['SEM_EXM_EXCH_ID'] == exchange) & ((instrument_df['SEM_TRADING_SYMBOL'].str.startswith(Underlying))|(instrument_df['SEM_CUSTOM_SYMBOL'].str.startswith(Underlying))) & (instrument_df['ContractExpiration'] == Expiry_date) & (instrument_df['SEM_OPTION_TYPE']=='PE')    & (instrument_df['SM_SYMBOL_NAME']==Underlying)
            elif Underlying in self.stock_step_df:
                ce_condition = (instrument_df['SEM_EXM_EXCH_ID'] == exchange) & ((instrument_df['SEM_TRADING_SYMBOL'].str.startswith(Underlying + '-'))&(instrument_df['SEM_CUSTOM_SYMBOL'].str.startswith(Underlying))) & (instrument_df['ContractExpiration'] == Expiry_date) & (instrument_df['SEM_OPTION_TYPE']=='CE')
                pe_condition = (instrument_df['SEM_EXM_EXCH_ID'] == exchange) & ((instrument_df['SEM_TRADING_SYMBOL'].str.startswith(Underlying + '-'))&(instrument_df['SEM_CUSTOM_SYMBOL'].str.startswith(Underlying))) & (instrument_df['ContractExpiration'] == Expiry_date) & (instrument_df['SEM_OPTION_TYPE']=='PE')
            else:
                data = f'{Underlying} Not in the step_size list'
                raise Exception(data)
            ce_df = instrument_df[ce_condition].copy()
            pe_df = instrument_df[pe_condition].copy()
            if ce_df.empty or pe_df.empty:
                raise Exception(f"Unable to find the ATM strike for the {Underlying}")
            ce_df['SEM_STRIKE_PRICE'] = ce_df['SEM_STRIKE_PRICE'].astype("float")
            pe_df['SEM_STRIKE_PRICE'] = pe_df['SEM_STRIKE_PRICE'].astype("float")
            ce_df =ce_df[ce_df['SEM_STRIKE_PRICE']==float(strike)]
            pe_df =pe_df[pe_df['SEM_STRIKE_PRICE']==float(strike)]

            if ce_df.empty or pe_df.empty:
                raise Exception(f"Unable to find the ATM strike for the {Underlying}")          
            if ce_df.empty or len(ce_df)==0:
                ce_df['diff'] = abs(ce_df['SEM_STRIKE_PRICE'] - strike)
                closest_index = ce_df['diff'].idxmin()
                strike = ce_df.loc[closest_index, 'SEM_STRIKE_PRICE']
                ce_df =ce_df[ce_df['SEM_STRIKE_PRICE']==strike]
            
            ce_df = ce_df.iloc[-1]  
            if pe_df.empty or len(pe_df)==0:
                pe_df['diff'] = abs(pe_df['SEM_STRIKE_PRICE'] - strike)
                closest_index = pe_df['diff'].idxmin()
                strike = pe_df.loc[closest_index, 'SEM_STRIKE_PRICE']
                pe_df =pe_df[pe_df['SEM_STRIKE_PRICE']==strike]
            
            pe_df = pe_df.iloc[-1]          
            ce_strike = ce_df['SEM_CUSTOM_SYMBOL']
            pe_strike = pe_df['SEM_CUSTOM_SYMBOL']
            if ce_strike== None:
                self.logger.info("No Scripts to Select from ce_spot_difference for ")
                print("No Scripts to Select from ce_spot_difference for ")
                return
            if pe_strike == None:
                self.logger.info("No Scripts to Select from pe_spot_difference for ")
                print("No Scripts to Select from pe_spot_difference for ")
                return
            
            return ce_strike, pe_strike, strike
        except Exception as e:
            print('exception got in ce_pe_option_df',e)
            return None, None, strike


# ________________________________________________________________________________________________________________________


    def OTM_Strike_Selection(self, Underlying, Expiry,OTM_count=1):
        try:
            instrument_df = self.instrument_df.copy()
            Underlying = Underlying.upper()

            # Expiry = pd.to_datetime(Expiry, format='%d-%m-%Y').strftime('%Y-%m-%d')
            exchange_index = {"BANKNIFTY": "NSE","NIFTY":"NSE","MIDCPNIFTY":"NSE", "FINNIFTY":"NSE","SENSEX":"BSE","BANKEX":"BSE"}

            instrument_df['SEM_EXPIRY_DATE'] = pd.to_datetime(instrument_df['SEM_EXPIRY_DATE'], errors='coerce')
            instrument_df['ContractExpiration'] = instrument_df['SEM_EXPIRY_DATE'].dt.date
            instrument_df['ContractExpiration'] = instrument_df['ContractExpiration'].astype(str)
            if Underlying in exchange_index:
                exchange = exchange_index[Underlying]
                expiry_exchange = 'INDEX'
            elif Underlying in self.commodity_step_dict.keys():
                exchange = "MCX"
                expiry_exchange = exchange
            else:
                # exchange = instrument_df[((instrument_df['SEM_TRADING_SYMBOL']==Underlying)|(instrument_df['SEM_CUSTOM_SYMBOL']==Underlying))].iloc[0]['SEM_EXM_EXCH_ID']
                exchange = "NSE"
                expiry_exchange = exchange
            expiry_list = self.get_expiry_list(Underlying=Underlying, exchange = expiry_exchange)
            if len(expiry_list)==0:
                print(f"Unable to find the correct Expiry for {Underlying}")
                return None
            if len(expiry_list)<Expiry:
                Expiry_date = expiry_list[-1]
            else:
                Expiry_date = expiry_list[Expiry]           
    
            ltp_data = self.get_ltp_data(Underlying)
            ltp = ltp_data[Underlying]
            
            if Underlying in self.index_step_dict:
                step_size = self.index_step_dict[Underlying]
            elif Underlying in self.stock_step_df:
                step_size = self.stock_step_df[Underlying]
            elif Underlying in self.commodity_step_dict:
                step_size = self.commodity_step_dict[Underlying]
            else:
                data = f'{Underlying} Not in the step_size list'
                raise Exception(data)
            strike = round(ltp/step_size) * step_size
            if OTM_count<1:
                return "INVALID OTM DISTANCE"
            step_size = float(OTM_count*step_size)
            ce_OTM_price = strike+step_size
            pe_OTM_price = strike-step_size
            if Underlying in self.index_step_dict:
                ce_condition = (instrument_df['SEM_EXM_EXCH_ID'] == exchange) & ((instrument_df['SEM_TRADING_SYMBOL'].str.startswith(Underlying))|(instrument_df['SEM_CUSTOM_SYMBOL'].str.startswith(Underlying))) & (instrument_df['ContractExpiration'] == Expiry_date) & (instrument_df['SEM_OPTION_TYPE']=='CE') 
                pe_condition = (instrument_df['SEM_EXM_EXCH_ID'] == exchange) & ((instrument_df['SEM_TRADING_SYMBOL'].str.startswith(Underlying))|(instrument_df['SEM_CUSTOM_SYMBOL'].str.startswith(Underlying))) & (instrument_df['ContractExpiration'] == Expiry_date) & (instrument_df['SEM_OPTION_TYPE']=='PE')
            elif exchange =="MCX":      
                ce_condition = (instrument_df['SEM_EXM_EXCH_ID'] == exchange) & ((instrument_df['SEM_TRADING_SYMBOL'].str.startswith(Underlying))|(instrument_df['SEM_CUSTOM_SYMBOL'].str.startswith(Underlying))) & (instrument_df['ContractExpiration'] == Expiry_date) & (instrument_df['SEM_OPTION_TYPE']=='CE') & (instrument_df['SM_SYMBOL_NAME']==Underlying) 
                pe_condition = (instrument_df['SEM_EXM_EXCH_ID'] == exchange) & ((instrument_df['SEM_TRADING_SYMBOL'].str.startswith(Underlying))|(instrument_df['SEM_CUSTOM_SYMBOL'].str.startswith(Underlying))) & (instrument_df['ContractExpiration'] == Expiry_date) & (instrument_df['SEM_OPTION_TYPE']=='PE') & (instrument_df['SM_SYMBOL_NAME']==Underlying)
            elif Underlying in self.stock_step_df:
                ce_condition = (instrument_df['SEM_EXM_EXCH_ID'] == exchange) & ((instrument_df['SEM_TRADING_SYMBOL'].str.startswith(Underlying + '-'))&(instrument_df['SEM_CUSTOM_SYMBOL'].str.startswith(Underlying))) & (instrument_df['ContractExpiration'] == Expiry_date) & (instrument_df['SEM_OPTION_TYPE']=='CE')
                pe_condition = (instrument_df['SEM_EXM_EXCH_ID'] == exchange) & ((instrument_df['SEM_TRADING_SYMBOL'].str.startswith(Underlying + '-'))&(instrument_df['SEM_CUSTOM_SYMBOL'].str.startswith(Underlying))) & (instrument_df['ContractExpiration'] == Expiry_date) & (instrument_df['SEM_OPTION_TYPE']=='PE')      
            else:
                data = f'{Underlying} Not in the step_size list'
                raise Exception(data)                           
            
            ce_df = instrument_df[ce_condition].copy()
            pe_df = instrument_df[pe_condition].copy()
            if ce_df.empty or pe_df.empty:
                raise Exception(f"Unable to find the OTM strike for the {Underlying}")          
            ce_df['SEM_STRIKE_PRICE'] = ce_df['SEM_STRIKE_PRICE'].astype("float")
            pe_df['SEM_STRIKE_PRICE'] = pe_df['SEM_STRIKE_PRICE'].astype("float")
            ce_df =ce_df[ce_df['SEM_STRIKE_PRICE']==float(ce_OTM_price)]
            pe_df =pe_df[pe_df['SEM_STRIKE_PRICE']==float(pe_OTM_price)]
            if ce_df.empty or pe_df.empty:
                raise Exception(f"Unable to find the OTM strike for the {Underlying}")          
            if ce_df.empty or len(ce_df)==0:
                ce_df['diff'] = abs(ce_df['SEM_STRIKE_PRICE'] - ce_OTM_price)
                closest_index = ce_df['diff'].idxmin()
                ce_OTM_price = ce_df.loc[closest_index, 'SEM_STRIKE_PRICE']
                ce_df =ce_df[ce_df['SEM_STRIKE_PRICE']==ce_OTM_price]
            
            ce_df = ce_df.iloc[-1]  
            if pe_df.empty or len(pe_df)==0:
                pe_df['diff'] = abs(pe_df['SEM_STRIKE_PRICE'] - pe_OTM_price)
                closest_index = pe_df['diff'].idxmin()
                pe_OTM_price = pe_df.loc[closest_index, 'SEM_STRIKE_PRICE']
                pe_df =pe_df[pe_df['SEM_STRIKE_PRICE']==pe_OTM_price]
            
            pe_df = pe_df.iloc[-1]          
            ce_strike = ce_df['SEM_CUSTOM_SYMBOL']
            pe_strike = pe_df['SEM_CUSTOM_SYMBOL']
            if ce_strike== None:
                self.logger.info("No Scripts to Select from ce_spot_difference for ")
                print("No Scripts to Select from ce_spot_difference for ")
                return
            if pe_strike == None:
                self.logger.info("No Scripts to Select from pe_spot_difference for ")
                print("No Scripts to Select from pe_spot_difference for ")
                return
            
            return ce_strike, pe_strike, ce_OTM_price, pe_OTM_price
        except Exception as e:
            print(f"Getting Error at OTM strike Selection as {e}")
            return None,None,0,0


# ________________________________________________________________________________________________________________________


    def ITM_Strike_Selection(self, Underlying, Expiry, ITM_count=1):
        try:
            instrument_df = self.instrument_df.copy()            
            Underlying = Underlying.upper()

            # Expiry = pd.to_datetime(Expiry, format='%d-%m-%Y').strftime('%Y-%m-%d')
            exchange_index = {"BANKNIFTY": "NSE","NIFTY":"NSE","MIDCPNIFTY":"NSE", "FINNIFTY":"NSE","SENSEX":"BSE","BANKEX":"BSE"}

            instrument_df['SEM_EXPIRY_DATE'] = pd.to_datetime(instrument_df['SEM_EXPIRY_DATE'], errors='coerce')
            instrument_df['ContractExpiration'] = instrument_df['SEM_EXPIRY_DATE'].dt.date
            instrument_df['ContractExpiration'] = instrument_df['ContractExpiration'].astype(str)
            if Underlying in exchange_index:
                exchange = exchange_index[Underlying]
                expiry_exchange = 'INDEX'
            elif Underlying in self.commodity_step_dict.keys():
                exchange = "MCX"
                expiry_exchange = exchange
            else:
                # exchange = instrument_df[((instrument_df['SEM_TRADING_SYMBOL']==Underlying)|(instrument_df['SEM_CUSTOM_SYMBOL']==Underlying))].iloc[0]['SEM_EXM_EXCH_ID']
                exchange = "NSE"
                expiry_exchange = exchange
            expiry_list = self.get_expiry_list(Underlying=Underlying, exchange = expiry_exchange)
            if len(expiry_list)==0:
                print(f"Unable to find the correct Expiry for {Underlying}")
                return None
            if len(expiry_list)<Expiry:
                Expiry_date = expiry_list[-1]
            else:
                Expiry_date = expiry_list[Expiry]           
    
            ltp_data = self.get_ltp_data(Underlying)
            ltp = ltp_data[Underlying]
            
            if Underlying in self.index_step_dict:
                step_size = self.index_step_dict[Underlying]
            elif Underlying in self.stock_step_df:
                step_size = self.stock_step_df[Underlying]
            elif Underlying in self.commodity_step_dict:
                step_size = self.commodity_step_dict[Underlying]
            else:
                data = f'{Underlying} Not in the step_size list'
                raise Exception(data)
            strike = round(ltp/step_size) * step_size
            if ITM_count<1:
                return "INVALID ITM DISTANCE"
            
            step_size = float(ITM_count*step_size)
            ce_ITM_price = strike-step_size
            pe_ITM_price = strike+step_size
            if Underlying in self.index_step_dict:
                ce_condition = (instrument_df['SEM_EXM_EXCH_ID'] == exchange) & ((instrument_df['SEM_TRADING_SYMBOL'].str.startswith(Underlying))|(instrument_df['SEM_CUSTOM_SYMBOL'].str.startswith(Underlying))) & (instrument_df['ContractExpiration'] == Expiry_date) & (instrument_df['SEM_OPTION_TYPE']=='CE') 
                pe_condition = (instrument_df['SEM_EXM_EXCH_ID'] == exchange) & ((instrument_df['SEM_TRADING_SYMBOL'].str.startswith(Underlying))|(instrument_df['SEM_CUSTOM_SYMBOL'].str.startswith(Underlying))) & (instrument_df['ContractExpiration'] == Expiry_date) & (instrument_df['SEM_OPTION_TYPE']=='PE')
            elif exchange =="MCX":      
                ce_condition = (instrument_df['SEM_EXM_EXCH_ID'] == exchange) & ((instrument_df['SEM_TRADING_SYMBOL'].str.startswith(Underlying))|(instrument_df['SEM_CUSTOM_SYMBOL'].str.startswith(Underlying))) & (instrument_df['ContractExpiration'] == Expiry_date) & (instrument_df['SEM_OPTION_TYPE']=='CE') & (instrument_df['SM_SYMBOL_NAME']==Underlying) 
                pe_condition = (instrument_df['SEM_EXM_EXCH_ID'] == exchange) & ((instrument_df['SEM_TRADING_SYMBOL'].str.startswith(Underlying))|(instrument_df['SEM_CUSTOM_SYMBOL'].str.startswith(Underlying))) & (instrument_df['ContractExpiration'] == Expiry_date) & (instrument_df['SEM_OPTION_TYPE']=='PE') & (instrument_df['SM_SYMBOL_NAME']==Underlying)
            elif Underlying in self.stock_step_df:
                ce_condition = (instrument_df['SEM_EXM_EXCH_ID'] == exchange) & ((instrument_df['SEM_TRADING_SYMBOL'].str.startswith(Underlying + '-'))&(instrument_df['SEM_CUSTOM_SYMBOL'].str.startswith(Underlying))) & (instrument_df['ContractExpiration'] == Expiry_date) & (instrument_df['SEM_OPTION_TYPE']=='CE')
                pe_condition = (instrument_df['SEM_EXM_EXCH_ID'] == exchange) & ((instrument_df['SEM_TRADING_SYMBOL'].str.startswith(Underlying + '-'))&(instrument_df['SEM_CUSTOM_SYMBOL'].str.startswith(Underlying))) & (instrument_df['ContractExpiration'] == Expiry_date) & (instrument_df['SEM_OPTION_TYPE']=='PE')
            else:
                data = f'{Underlying} Not in the step_size list'
                raise Exception(data)           
                        
            ce_df = instrument_df[ce_condition].copy()
            pe_df = instrument_df[pe_condition].copy()
            if ce_df.empty or pe_df.empty:
                raise Exception(f"Unable to find the ITM strike for the {Underlying}")          
            ce_df['SEM_STRIKE_PRICE'] = ce_df['SEM_STRIKE_PRICE'].astype("float")
            pe_df['SEM_STRIKE_PRICE'] = pe_df['SEM_STRIKE_PRICE'].astype("float")
            ce_df =ce_df[ce_df['SEM_STRIKE_PRICE']==float(ce_ITM_price)]
            pe_df =pe_df[pe_df['SEM_STRIKE_PRICE']==float(pe_ITM_price)]
            if ce_df.empty or pe_df.empty:
                raise Exception(f"Unable to find the ITM strike for the {Underlying}")
            if ce_df.empty or len(ce_df)==0:
                ce_df['diff'] = abs(ce_df['SEM_STRIKE_PRICE'] - ce_ITM_price)
                closest_index = ce_df['diff'].idxmin()
                ce_ITM_price = ce_df.loc[closest_index, 'SEM_STRIKE_PRICE']
                ce_df =ce_df[ce_df['SEM_STRIKE_PRICE']==ce_ITM_price]
            
            ce_df = ce_df.iloc[-1]  
            if pe_df.empty or len(pe_df)==0:
                pe_df['diff'] = abs(pe_df['SEM_STRIKE_PRICE'] - pe_ITM_price)
                closest_index = pe_df['diff'].idxmin()
                pe_ITM_price = pe_df.loc[closest_index, 'SEM_STRIKE_PRICE']
                pe_df =pe_df[pe_df['SEM_STRIKE_PRICE']==pe_ITM_price]
            
            pe_df = pe_df.iloc[-1]          
            ce_strike = ce_df['SEM_CUSTOM_SYMBOL']
            pe_strike = pe_df['SEM_CUSTOM_SYMBOL']
            if ce_strike== None:
                self.logger.info("No Scripts to Select from ce_spot_difference for ")
                print("No Scripts to Select from ce_spot_difference for ")
                return
            if pe_strike == None:
                self.logger.info("No Scripts to Select from pe_spot_difference for ")
                print("No Scripts to Select from pe_spot_difference for ")
                return
            
            return ce_strike, pe_strike, ce_ITM_price, pe_ITM_price
        except Exception as e:
            print(f"Getting Error at OTM strike Selection as {e}")
            return None,None,0,0

# ________________________________________________________________________________________________________________________


    def get_expiry_list(self, Underlying, exchange):
        try:
            instrument_df = self.instrument_df.copy()
            Underlying = Underlying.upper()
            exchange = exchange.upper()
            script_exchange = {"NSE":self.Dhan.NSE, "NFO":self.Dhan.FNO, "BFO":"BSE_FNO", "CUR": self.Dhan.CUR, "BSE":self.Dhan.BSE, "MCX":self.Dhan.MCX, "INDEX":self.Dhan.INDEX}
            instrument_exchange = {'NSE':"NSE",'BSE':"BSE",'NFO':'NSE','BFO':'BSE','MCX':'MCX','CUR':'NSE'}
            exchange_segment = script_exchange[exchange]
            index_exchange = {"NIFTY":'NSE',"BANKNIFTY":"NSE","FINNIFTY":"NSE","MIDCPNIFTY":"NSE","BANKEX":"BSE","SENSEX":"BSE"}
            if Underlying in index_exchange:
                exchange =index_exchange[Underlying]
            if Underlying in self.commodity_step_dict.keys():
                security_check = instrument_df[(instrument_df['SEM_EXM_EXCH_ID']=='MCX')&(instrument_df['SM_SYMBOL_NAME']==Underlying.upper())&(instrument_df['SEM_INSTRUMENT_NAME']=='FUTCOM')]                        
                if security_check.empty:
                    raise Exception("Check the Tradingsymbol")
                security_id = security_check.sort_values(by='SEM_EXPIRY_DATE').iloc[0]['SEM_SMST_SECURITY_ID']
            else:                       
                security_check = instrument_df[((instrument_df['SEM_TRADING_SYMBOL']==Underlying)|(instrument_df['SEM_CUSTOM_SYMBOL']==Underlying))&(instrument_df['SEM_EXM_EXCH_ID']==instrument_exchange[exchange])]
                if security_check.empty:
                    raise Exception("Check the Tradingsymbol")
                security_id = security_check.iloc[-1]['SEM_SMST_SECURITY_ID']
            response = self.Dhan.expiry_list(under_security_id =int(security_id), under_exchange_segment = exchange_segment)
            if response['status']=='success':
                return response['data']['data']
            else:
                raise Exception(response)
        except Exception as e:
            print(f"Exception at getting Expiry list as {e}")
            return list()


# ________________________________________________________________________________________________________________________

        
    def get_option_greek(self, strike: int, expiry: int, asset: str, interest_rate: float, flag: str, scrip_type: str):
        try:
            asset = asset.upper()
            # expiry = pd.to_datetime(expiry_date, format='%d-%m-%Y').strftime('%Y-%m-%d')
            exchange_index = {"BANKNIFTY": "NSE", "NIFTY": "NSE", "MIDCPNIFTY": "NSE", "FINNIFTY": "NSE", "SENSEX": "BSE", "BANKEX": "BSE"}
            asset_dict = {'NIFTY BANK': "BANKNIFTY", "NIFTY 50": "NIFTY", 'NIFTY FIN SERVICE': 'FINNIFTY', 'NIFTY MID SELECT': 'MIDCPNIFTY', "SENSEX": "SENSEX", "BANKEX": "BANKEX"}
            if asset in asset_dict:
                inst_asset = asset_dict[asset]
            elif asset in asset_dict.values():
                inst_asset = asset
            else:
                inst_asset = asset
            if inst_asset in exchange_index:
                exchange = exchange_index[inst_asset]
                expiry_exchange = 'INDEX'
            elif inst_asset in self.commodity_step_dict.keys():
                exchange = "MCX"
                expiry_exchange = exchange
            else:
                # exchange = instrument_df[((instrument_df['SEM_TRADING_SYMBOL']==Underlying)|(instrument_df['SEM_CUSTOM_SYMBOL']==Underlying))].iloc[0]['SEM_EXM_EXCH_ID']
                exchange = "NSE"
                expiry_exchange = exchange
            expiry_list = self.get_expiry_list(Underlying=inst_asset, exchange = expiry_exchange)
            if len(expiry_list)==0:
                print(f"Unable to find the correct Expiry for {inst_asset}")
                return None
            if len(expiry_list)<expiry:
                expiry_date = expiry_list[-1]
            else:
                expiry_date = expiry_list[expiry]
                
            # exchange = exchange_index[inst_asset]
            instrument_df = self.instrument_df.copy()

            instrument_df['SEM_EXPIRY_DATE'] = pd.to_datetime(instrument_df['SEM_EXPIRY_DATE'], errors='coerce')
            instrument_df['ContractExpiration'] = instrument_df['SEM_EXPIRY_DATE'].dt.date.astype(str)
            # check_ecpiry = datetime.datetime.strptime(expiry_date, '%d-%m-%Y')

            data = instrument_df[
                # (instrument_df['SEM_EXM_EXCH_ID'] == exchange) &
                ((instrument_df['SEM_TRADING_SYMBOL'].str.contains(inst_asset)) | 
                 (instrument_df['SEM_CUSTOM_SYMBOL'].str.contains(inst_asset))) &
                (instrument_df['ContractExpiration'] == expiry_date) &
                (instrument_df['SEM_STRIKE_PRICE'] == strike) &
                (instrument_df['SEM_OPTION_TYPE']==scrip_type)
            ]
            if data.empty:
                self.logger.error('No data found for the specified parameters.')
                raise Exception('No data found for the specified parameters.')
            script_list = data['SEM_CUSTOM_SYMBOL'].tolist()
            script = script_list[0]
            days_to_expiry = (datetime.datetime.strptime(expiry_date, "%Y-%m-%d").date() - datetime.datetime.now().date()).days
            if days_to_expiry <= 0:
                days_to_expiry = 1
            ltp_data = self.get_ltp_data([asset,script])
            asset_price = ltp_data[asset]
            ltp = ltp_data[script]
            # asset_price = self.get_ltp(asset)
            # ltp = self.get_ltp(script)
            if scrip_type == 'CE':
                civ = mibian.BS([asset_price, strike, interest_rate, days_to_expiry], callPrice= ltp)
                cval = mibian.BS([asset_price, strike, interest_rate, days_to_expiry], volatility = civ.impliedVolatility ,callPrice= ltp)
                if flag == "price":
                    return cval.callPrice
                if flag == "delta":
                    return cval.callDelta
                if flag == "delta2":
                    return cval.callDelta2
                if flag == "theta":
                    return cval.callTheta
                if flag == "rho":
                    return cval.callRho
                if flag == "vega":
                    return cval.vega
                if flag == "gamma":
                    return cval.gamma
                if flag == "all_val":
                    return {'callPrice' : cval.callPrice, 'callDelta' : cval.callDelta, 'callDelta2' : cval.callDelta2, 'callTheta' : cval.callTheta, 'callRho' : cval.callRho, 'vega' : cval.vega, 'gamma' : cval.gamma}
            if scrip_type == "PE":
                piv = mibian.BS([asset_price, strike, interest_rate, days_to_expiry], putPrice= ltp)
                pval = mibian.BS([asset_price, strike, interest_rate, days_to_expiry], volatility = piv.impliedVolatility ,putPrice= ltp)
                if flag == "price":
                    return pval.putPrice
                if flag == "delta":
                    return pval.putDelta
                if flag == "delta2":
                    return pval.putDelta2
                if flag == "theta":
                    return pval.putTheta
                if flag == "rho":
                    return pval.putRho
                if flag == "vega":
                    return pval.vega
                if flag == "gamma":
                    return pval.gamma
                if flag == "all_val":
                    return {'callPrice' : pval.putPrice, 'callDelta' : pval.putDelta, 'callDelta2' : pval.putDelta2, 'callTheta' : pval.putTheta, 'callRho' : pval.putRho, 'vega' : pval.vega, 'gamma' : pval.gamma}
        except Exception as e:
            print(f"Exception in get_option_greek: {e}")
            return None


# ________________________________________________________________________________________________________________________


    def format_option_chain(self,data):

        try:
            option_chain_rows = []
            for strike, details in data["oc"].items():
                ce = details.get("ce", {})
                pe = details.get("pe", {})
                ce_greeks = ce.get("greeks", {})
                pe_greeks = pe.get("greeks", {})
                
                option_chain_rows.append({
                    # Calls (CE) data
                    "CE OI": ce.get("oi", None),
                    "CE Chg in OI": ce.get("oi", 0) - ce.get("previous_oi", 0),
                    "CE Volume": ce.get("volume", None),
                    "CE IV": ce.get("implied_volatility", None),
                    "CE LTP": ce.get("last_price", None),
                    "CE Bid Qty": ce.get("top_bid_quantity", None),
                    "CE Bid": ce.get("top_bid_price", None),
                    "CE Ask": ce.get("top_ask_price", None),
                    "CE Ask Qty": ce.get("top_ask_quantity", None),
                    "CE Delta": ce_greeks.get("delta", None),
                    "CE Theta": ce_greeks.get("theta", None),
                    "CE Gamma": ce_greeks.get("gamma", None),
                    "CE Vega": ce_greeks.get("vega", None),
                    # Strike Price
                    "Strike Price": strike,
                    # Puts (PE) data
                    "PE Bid Qty": pe.get("top_bid_quantity", None),
                    "PE Bid": pe.get("top_bid_price", None),
                    "PE Ask": pe.get("top_ask_price", None),
                    "PE Ask Qty": pe.get("top_ask_quantity", None),
                    "PE LTP": pe.get("last_price", None),
                    "PE IV": pe.get("implied_volatility", None),
                    "PE Volume": pe.get("volume", None),
                    "PE Chg in OI": pe.get("oi", 0) - pe.get("previous_oi", 0),
                    "PE OI": pe.get("oi", None),
                    "PE Delta": pe_greeks.get("delta", None),
                    "PE Theta": pe_greeks.get("theta", None),
                    "PE Gamma": pe_greeks.get("gamma", None),
                    "PE Vega": pe_greeks.get("vega", None),
                })
            
            df = pd.DataFrame(option_chain_rows)
            
            columns = list(df.columns)
            strike_index = columns.index("Strike Price")
            new_order = columns[:strike_index] + columns[strike_index + 1:]
            middle_index = len(new_order) // 2
            new_order = new_order[:middle_index] + ["Strike Price"] + new_order[middle_index:]
            df = df[new_order]
            
            return df
        except Exception as e:
            print(f"Unable to form the Option chain as {e}")
            return data


# ________________________________________________________________________________________________________________________


        
    def get_option_chain(self, Underlying, exchange, expiry,num_strikes = 10):
        
        instrument_df = self.instrument_df.copy()

        try:
            Underlying = Underlying.upper()
            exchange = exchange.upper()
            script_exchange = {"NSE":self.Dhan.NSE, "NFO":self.Dhan.FNO, "BFO":"BSE_FNO", "CUR": self.Dhan.CUR, "BSE":self.Dhan.BSE, "MCX":self.Dhan.MCX, "INDEX":self.Dhan.INDEX}
            instrument_exchange = {'NSE':"NSE",'BSE':"BSE",'NFO':'NSE','BFO':'BSE','MCX':'MCX','CUR':'NSE'}
            exchange_segment = script_exchange[exchange]
            index_exchange = {"NIFTY":'NSE',"BANKNIFTY":"NSE","FINNIFTY":"NSE","MIDCPNIFTY":"NSE","BANKEX":"BSE","SENSEX":"BSE"}
            
            if Underlying in index_exchange:
                exchange =index_exchange[Underlying]
            if Underlying in self.commodity_step_dict.keys():
                security_check = instrument_df[(instrument_df['SEM_EXM_EXCH_ID']=='MCX')&(instrument_df['SM_SYMBOL_NAME']==Underlying.upper())&(instrument_df['SEM_INSTRUMENT_NAME']=='FUTCOM')]                        
                if security_check.empty:
                    raise Exception("Check the Tradingsymbol")
                security_id = security_check.sort_values(by='SEM_EXPIRY_DATE').iloc[0]['SEM_SMST_SECURITY_ID']
            else:                       
                security_check = instrument_df[((instrument_df['SEM_TRADING_SYMBOL']==Underlying)|(instrument_df['SEM_CUSTOM_SYMBOL']==Underlying))&(instrument_df['SEM_EXM_EXCH_ID']==instrument_exchange[exchange])]
                if security_check.empty:
                    raise Exception("Check the Tradingsymbol")
                security_id = security_check.iloc[-1]['SEM_SMST_SECURITY_ID']
            if Underlying in index_exchange:
                expiry_exchange = 'INDEX'
            elif Underlying in self.commodity_step_dict.keys():
                exchange = "MCX"
                expiry_exchange = exchange
            else:
                # exchange = instrument_df[((instrument_df['SEM_TRADING_SYMBOL']==Underlying)|(instrument_df['SEM_CUSTOM_SYMBOL']==Underlying))].iloc[0]['SEM_EXM_EXCH_ID']
                exchange = "NSE"
                expiry_exchange = exchange
            expiry_list = self.get_expiry_list(Underlying=Underlying, exchange = expiry_exchange)
            if len(expiry_list)==0:
                print(f"Unable to find the correct Expiry for {Underlying}")
                return None
            if len(expiry_list)<expiry:
                Expiry_date = expiry_list[-1]
            else:
                Expiry_date = expiry_list[expiry]                       
            # time.sleep(3)
            response = self.Dhan.option_chain(under_security_id =int(security_id), under_exchange_segment = exchange_segment, expiry = Expiry_date)
            if response['status']=='success':
                oc = response['data']['data']
                oc_df = self.format_option_chain(oc)
                atm_price = self.get_ltp_data(Underlying)
                oc_df['Strike Price'] = pd.to_numeric(oc_df['Strike Price'], errors='coerce')
                if Underlying in self.index_step_dict:
                    strike_step = self.index_step_dict[Underlying]
                elif Underlying in self.stock_step_df:
                    strike_step = self.stock_step_df[Underlying]
                elif Underlying in self.commodity_step_dict:
                    strike_step = self.commodity_step_dict[Underlying]
                else:
                    raise Exception(f"No option chain data available for the {Underlying}")
                atm_strike = round(atm_price[Underlying]/strike_step) * strike_step
                df = oc_df[(oc_df['Strike Price'] >= atm_strike - num_strikes * strike_step) & (oc_df['Strike Price'] <= atm_strike + num_strikes * strike_step)].sort_values(by='Strike Price').reset_index(drop=True)
                return atm_strike, df
            else:
                raise Exception(response)           
        except Exception as e:
            print(f"Getting Error at Option Chain as {e}")


# ________________________________________________________________________________________________________________________


    def get_quote_data(self,names, debug="NO"):
        try:
            instrument_df = self.instrument_df.copy()
            instrument_names = {}

            instruments = {'NSE_EQ':[],'IDX_I':[],'NSE_FNO':[],'NSE_CURRENCY':[],'BSE_EQ':[],'BSE_FNO':[],'BSE_CURRENCY':[],'MCX_COMM':[]}
            NFO = ["BANKNIFTY","NIFTY","MIDCPNIFTY","FINNIFTY"]
            BFO = ['SENSEX','BANKEX']
            equity = ['CALL','PUT','FUT']           
            exchange_index = {"BANKNIFTY": "NSE_IDX","NIFTY":"NSE_IDX","MIDCPNIFTY":"NSE_IDX", "FINNIFTY":"NSE_IDX","SENSEX":"BSE_IDX","BANKEX":"BSE_IDX", "INDIA VIX":"IDX_I"}
            if not isinstance(names, list):
                names = [names]
            for name in names:
                try:
                    name = name.upper()
                    if name in exchange_index.keys():
                        security_check = instrument_df[((instrument_df['SEM_CUSTOM_SYMBOL']==name)|(instrument_df['SEM_TRADING_SYMBOL']==name))]
                        if security_check.empty:
                            raise Exception("Check the Tradingsymbol")
                        security_id = security_check.iloc[-1]['SEM_SMST_SECURITY_ID']
                        instruments['IDX_I'].append(int(security_id))
                        instrument_names[str(security_id)]=name
                    elif name in self.commodity_step_dict.keys():
                        security_check = instrument_df[(instrument_df['SEM_EXM_EXCH_ID']=='MCX')&(instrument_df['SM_SYMBOL_NAME']==name.upper())&(instrument_df['SEM_INSTRUMENT_NAME']=='FUTCOM')]                      
                        if security_check.empty:
                            raise Exception("Check the Tradingsymbol")
                        security_id = security_check.sort_values(by='SEM_EXPIRY_DATE').iloc[0]['SEM_SMST_SECURITY_ID']
                        instruments['MCX_COMM'].append(int(security_id))
                        instrument_names[str(security_id)]=name
                    else:
                        security_check = instrument_df[((instrument_df['SEM_CUSTOM_SYMBOL']==name)|(instrument_df['SEM_TRADING_SYMBOL']==name))]
                        if security_check.empty:
                            raise Exception("Check the Tradingsymbol")                      
                        security_id = security_check.iloc[-1]['SEM_SMST_SECURITY_ID']
                        nfo_check = ['NSE_FNO' for nfo in NFO if nfo in name]
                        bfo_check = ['BSE_FNO' for bfo in BFO if bfo in name]
                        exchange_nfo ='NSE_FNO' if len(nfo_check)!=0 else False
                        exchange_bfo = 'BSE_FNO' if len(bfo_check)!=0 else False
                        if not exchange_nfo and not exchange_bfo:
                            eq_check =['NSE_FNO' for nfo in equity if nfo in name]
                            exchange_eq ='NSE_FNO' if len(eq_check)!=0 else "NSE_EQ"
                        else:
                            exchange_eq="NSE_EQ"
                        exchange ='NSE_FNO' if exchange_nfo else ('BSE_FNO' if exchange_bfo else exchange_eq)
                        trail_exchange = exchange
                        mcx_check = ['MCX_COMM' for mcx in self.commodity_step_dict.keys() if mcx in name]
                        exchange = "MCX_COMM" if len(mcx_check)!=0 else exchange
                        if exchange == "MCX_COMM": 
                            if instrument_df[((instrument_df['SEM_CUSTOM_SYMBOL']==name)|(instrument_df['SEM_TRADING_SYMBOL']==name))&(instrument_df['SEM_EXM_EXCH_ID']=='MCX')].empty:
                                exchange = trail_exchange
                        if exchange == "MCX_COMM":
                            security_check = instrument_df[((instrument_df['SEM_CUSTOM_SYMBOL']==name)|(instrument_df['SEM_TRADING_SYMBOL']==name))&(instrument_df['SEM_EXM_EXCH_ID']=='MCX')]
                            if security_check.empty:
                                raise Exception("Check the Tradingsymbol")  
                            security_id = security_check.iloc[-1]['SEM_SMST_SECURITY_ID']
                        instruments[exchange].append(int(security_id))
                        instrument_names[str(security_id)]=name
                except Exception as e:
                    print(f"Exception for instrument name {name} as {e}")
                    continue
            time.sleep(1)

            # Data Fetching
            data = self.Dhan.quote_data(instruments)
                        
            ltp_data=dict()
            
            if debug.upper()=="YES":
                print(data)         
            if data['status']!='failure':
                all_values = data['data']['data']
                for exchange in data['data']['data']:
                    for key, values in all_values[exchange].items():
                        symbol = instrument_names[key]
                        ltp_data[symbol] = values
            else:
                raise Exception(data)
            
            return ltp_data
        except Exception as e:
            print(f"Exception at calling Quote as {e}")
            self.logger.exception(f"Exception at calling Quote as {e}")
            return dict()


# ________________________________________________________________________________________________________________________


    def get_ohlc_data(self,names, debug="NO"):
        try:
            instrument_df = self.instrument_df.copy()
            instrument_names = {}

            instruments = {'NSE_EQ':[],'IDX_I':[],'NSE_FNO':[],'NSE_CURRENCY':[],'BSE_EQ':[],'BSE_FNO':[],'BSE_CURRENCY':[],'MCX_COMM':[]}
            NFO = ["BANKNIFTY","NIFTY","MIDCPNIFTY","FINNIFTY"]
            BFO = ['SENSEX','BANKEX']
            equity = ['CALL','PUT','FUT']           
            exchange_index = {"BANKNIFTY": "NSE_IDX","NIFTY":"NSE_IDX","MIDCPNIFTY":"NSE_IDX", "FINNIFTY":"NSE_IDX","SENSEX":"BSE_IDX","BANKEX":"BSE_IDX", "INDIA VIX":"IDX_I"}
            if not isinstance(names, list):
                names = [names]
            for name in names:
                try:
                    name = name.upper()
                    if name in exchange_index.keys():
                        security_check = instrument_df[((instrument_df['SEM_CUSTOM_SYMBOL']==name)|(instrument_df['SEM_TRADING_SYMBOL']==name))]
                        if security_check.empty:
                            raise Exception("Check the Tradingsymbol")
                        security_id = security_check.iloc[-1]['SEM_SMST_SECURITY_ID']
                        instruments['IDX_I'].append(int(security_id))
                        instrument_names[str(security_id)]=name
                    elif name in self.commodity_step_dict.keys():
                        security_check = instrument_df[(instrument_df['SEM_EXM_EXCH_ID']=='MCX')&(instrument_df['SM_SYMBOL_NAME']==name.upper())&(instrument_df['SEM_INSTRUMENT_NAME']=='FUTCOM')]                      
                        if security_check.empty:
                            raise Exception("Check the Tradingsymbol")
                        security_id = security_check.sort_values(by='SEM_EXPIRY_DATE').iloc[0]['SEM_SMST_SECURITY_ID']
                        instruments['MCX_COMM'].append(int(security_id))
                        instrument_names[str(security_id)]=name
                    else:
                        security_check = instrument_df[((instrument_df['SEM_CUSTOM_SYMBOL']==name)|(instrument_df['SEM_TRADING_SYMBOL']==name))]
                        if security_check.empty:
                            raise Exception("Check the Tradingsymbol")                      
                        security_id = security_check.iloc[-1]['SEM_SMST_SECURITY_ID']
                        nfo_check = ['NSE_FNO' for nfo in NFO if nfo in name]
                        bfo_check = ['BSE_FNO' for bfo in BFO if bfo in name]
                        exchange_nfo ='NSE_FNO' if len(nfo_check)!=0 else False
                        exchange_bfo = 'BSE_FNO' if len(bfo_check)!=0 else False
                        if not exchange_nfo and not exchange_bfo:
                            eq_check =['NSE_FNO' for nfo in equity if nfo in name]
                            exchange_eq ='NSE_FNO' if len(eq_check)!=0 else "NSE_EQ"
                        else:
                            exchange_eq="NSE_EQ"
                        exchange ='NSE_FNO' if exchange_nfo else ('BSE_FNO' if exchange_bfo else exchange_eq)
                        trail_exchange = exchange
                        mcx_check = ['MCX_COMM' for mcx in self.commodity_step_dict.keys() if mcx in name]
                        exchange = "MCX_COMM" if len(mcx_check)!=0 else exchange
                        if exchange == "MCX_COMM": 
                            if instrument_df[((instrument_df['SEM_CUSTOM_SYMBOL']==name)|(instrument_df['SEM_TRADING_SYMBOL']==name))&(instrument_df['SEM_EXM_EXCH_ID']=='MCX')].empty:
                                exchange = trail_exchange
                        if exchange == "MCX_COMM":
                            security_check = instrument_df[((instrument_df['SEM_CUSTOM_SYMBOL']==name)|(instrument_df['SEM_TRADING_SYMBOL']==name))&(instrument_df['SEM_EXM_EXCH_ID']=='MCX')]
                            if security_check.empty:
                                raise Exception("Check the Tradingsymbol")  
                            security_id = security_check.iloc[-1]['SEM_SMST_SECURITY_ID']
                        instruments[exchange].append(int(security_id))
                        instrument_names[str(security_id)]=name
                except Exception as e:
                    print(f"Exception for instrument name {name} as {e}")
                    continue
            time.sleep(1)

            # Data Fetching
            data = self.Dhan.ohlc_data(instruments)
                        
            ltp_data=dict()
            
            if debug.upper()=="YES":
                print(data)         
            if data['status']!='failure':
                all_values = data['data']['data']
                for exchange in data['data']['data']:
                    for key, values in all_values[exchange].items():
                        symbol = instrument_names[key]
                        ltp_data[symbol] = values
            else:
                raise Exception(data)
            
            return ltp_data
        except Exception as e:
            print(f"Exception at calling OHLC as {e}")
            self.logger.exception(f"Exception at calling OHLC as {e}")
            return dict()

# ________________________________________________________________________________________________________________________

    # Long Term Historical Data
    def get_long_term_historical_data(self, tradingsymbol, exchange, timeframe, from_date, to_date, sector = "NO", debug="NO"):
        """
        Fetch historical data from Dhan between custom date range (supports DAY and intraday timeframes).
        
        Parameters:
            tradingsymbol (str): Trading symbol (e.g., "RELIANCE")
            exchange (str): Exchange name (e.g., "NSE")
            timeframe (str): One of ['1', '5', '15', '25', '60', 'DAY']
            from_date (str or datetime.date): Start date (e.g., '2022-01-01')
            to_date (str or datetime.date): End date (e.g., '2024-12-31')
            debug (str): If "YES", prints chunk info
            
        Returns:
            pd.DataFrame: Combined OHLCV data
        """
        try:
            instrument_df = self.instrument_df.copy()
            # tradingsymbol = tradingsymbol.upper()
            exchange = exchange.upper()

            if isinstance(from_date, str):
                from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d").date()
            if isinstance(to_date, str):
                to_date = datetime.datetime.strptime(to_date, "%Y-%m-%d").date()
            max_start_date = datetime.datetime.now().date() - datetime.timedelta(days=5 * 365)
            if from_date < max_start_date:
                print(f"Warning: from_date{from_date} exceeds Dhan's 5-year limit. Resetting to {max_start_date}")
                from_date = max_start_date
            if from_date > to_date:
                raise ValueError("from_date must be earlier than to_date")
            script_exchange = {
                "NSE": self.Dhan.NSE,
                "NFO": self.Dhan.NSE_FNO,
                "BFO": self.Dhan.BSE_FNO,
                "CUR": self.Dhan.CUR,
                "BSE": self.Dhan.BSE,
                "MCX": self.Dhan.MCX,
                "INDEX": self.Dhan.INDEX
            }
            instrument_exchange = {'NSE': "NSE", 'BSE': "BSE", 'NFO': 'NSE', 'BFO': 'BSE', 'MCX': 'MCX', 'CUR': 'NSE'}
            index_exchange = {"NIFTY": 'NSE', "BANKNIFTY": "NSE", "FINNIFTY": "NSE", "MIDCPNIFTY": "NSE", "BANKEX": "BSE", "SENSEX": "BSE", "INDIA VIX": "NSE"}
            exchange_segment = script_exchange[exchange]
            if tradingsymbol in index_exchange:
                exchange = index_exchange[tradingsymbol]
            if sector.upper()=="YES":
                security_check = instrument_df[((instrument_df['SEM_TRADING_SYMBOL']==tradingsymbol)|(instrument_df['SEM_CUSTOM_SYMBOL']==tradingsymbol))&(instrument_df['SEM_EXM_EXCH_ID']==exchange)]
                if security_check.empty:
                    raise Exception("Check the Tradingsymbol or Exchange")
                security_id = security_check.iloc[-1]['SEM_SMST_SECURITY_ID']
                exchange_segment = "IDX_I"
            if tradingsymbol in self.commodity_step_dict.keys():
                security_check = instrument_df[
                    (instrument_df['SEM_EXM_EXCH_ID'] == 'MCX') &
                    (instrument_df['SM_SYMBOL_NAME'] == tradingsymbol.upper()) &
                    (instrument_df['SEM_INSTRUMENT_NAME'] == 'FUTCOM')
                ]
                if security_check.empty:
                    raise Exception("Invalid symbol or exchange for commodity")
                security_id = security_check.sort_values(by='SEM_EXPIRY_DATE').iloc[0]['SEM_SMST_SECURITY_ID']
                tradingsymbol = security_check.sort_values(by='SEM_EXPIRY_DATE').iloc[0]['SEM_CUSTOM_SYMBOL']
            else:
                security_check = instrument_df[
                    ((instrument_df['SEM_TRADING_SYMBOL'] == tradingsymbol) |
                    (instrument_df['SEM_CUSTOM_SYMBOL'] == tradingsymbol)) &
                    (instrument_df['SEM_EXM_EXCH_ID'] == instrument_exchange[exchange])
                ]
                if security_check.empty:
                    raise Exception("Invalid symbol or exchange")
                security_id = security_check.iloc[-1]['SEM_SMST_SECURITY_ID']
            instrument_type = security_check.iloc[-1]['SEM_INSTRUMENT_NAME']
            expiry_code = security_check.iloc[-1]['SEM_EXPIRY_CODE']
            sample_from = max_start_date
            sample_to = datetime.datetime.now().date()
            # Fetch 90-day daily data to infer listing/start date
            day_data = self.Dhan.historical_daily_data(int(security_id), exchange_segment, instrument_type,sample_from.strftime('%Y-%m-%d'), sample_to.strftime('%Y-%m-%d'), int(expiry_code), oi = True)
            if day_data['status'] != 'failure':
                df_day = pd.DataFrame(day_data['data'])
                if not df_day.empty:
                    df_day['timestamp'] = df_day['timestamp'].apply(lambda x: self.convert_to_date_time(x))
                    earliest_day_date = df_day['timestamp'].min()
                    if from_date < earliest_day_date:
                        print(f"Adjusting from_date from {from_date} to {earliest_day_date} based on available data")
                        from_date = earliest_day_date
                else:
                    raise Exception("No DAY data found in test range.")
            else:
                raise Exception(f"Failed to retrieve DAY timeframe data: {day_data}")
            if timeframe in ['1', '5', '15', '25', '60']:
                interval = int(timeframe)
                is_intraday = True
            elif timeframe.upper() == "DAY":
                is_intraday = False
            else:
                raise Exception("interval must be one of ['1','5','15','25','60','DAY']")
            step_days = 90
            all_data = []
            current_from = from_date
            print(f"Fetching data for {tradingsymbol} from {from_date} to {to_date}")
            while current_from <= to_date:
                current_to = min(current_from + datetime.timedelta(days=step_days - 1), to_date)
                from_str = current_from.strftime('%Y-%m-%d')
                to_str = current_to.strftime('%Y-%m-%d')
                print(f"Fetching data for {tradingsymbol} from {from_str} to {to_str}")
                time.sleep(0.4)
                if is_intraday:
                    response = self.Dhan.intraday_minute_data(
                        str(security_id), exchange_segment, instrument_type,
                        from_str, to_str, interval, oi = True)
                else:
                    response = self.Dhan.historical_daily_data(
                        int(security_id), exchange_segment, instrument_type,
                        from_str, to_str, int(expiry_code), oi = True
                    )
                if response['status'] != 'failure':
                    df = pd.DataFrame(response['data'])
                    if not df.empty:
                        df['timestamp'] = df['timestamp'].apply(lambda x: self.convert_to_date_time(x))
                        all_data.append(df)
                        if debug.upper() == "YES":
                            print(f"{tradingsymbol} [{from_str} to {to_str}] {len(df)} rows")
                else:
                    print(f"Failed: {from_str} to {to_str}: {response}")
                current_from = current_to + datetime.timedelta(days=1)
            return pd.concat(all_data).reset_index(drop=True) if all_data else pd.DataFrame()
        except Exception as e:
            print(f"Exception: {e}")
            self.logger.exception(f"Custom range fetch failed: {e}")
            return pd.DataFrame()

# ________________________________________________________________________________________________________________________


    def convert_to_df(self, response):
        try:
            data = response.get("data", {}).get("data", {})
            ce = data.get("ce")
            pe = data.get("pe")
            def _df(x):
                df = pd.DataFrame({
                    "timestamp": x.get("timestamp", []),
                    "open": x.get("open", []),
                    "high": x.get("high", []),
                    "low": x.get("low", []),
                    "close": x.get("close", []),
                    "volume": x.get("volume", []),
                    "iv": x.get("iv", []),
                    "oi": x.get("oi", []),
                    "spot": x.get("spot", []),
                    "strike": x.get("strike", [])
                })
                df["datetime"] = df["timestamp"].apply(lambda t: datetime.datetime.utcfromtimestamp(t) + datetime.timedelta(hours=5, minutes=30))
                return df[["datetime","open","high","low","close","volume","iv","oi","spot","strike"]]
            if ce and not pe:
                return _df(ce)
            if pe and not ce:
                return _df(pe)
            if ce and pe:
                return {"CE": _df(ce), "PE": _df(pe)}
            return pd.DataFrame()
        except:
            return pd.DataFrame()

# ________________________________________________________________________________________________________________________


    def _resolve_security_id(self, tradingsymbol: str, exchange: str) -> str:

        instrument_df = self.instrument_df.copy()

        tradingsymbol = tradingsymbol.upper().strip()
        exchange = exchange.upper().strip()
        instrument_exchange = {
            "NSE": "NSE", "BSE": "BSE",
            "NFO": "NSE", "BFO": "BSE",
            "MCX": "MCX", "CUR": "NSE"
        }
        if exchange not in instrument_exchange:
            raise Exception(f"Invalid exchange: {exchange}")
        sec = instrument_df[
            ((instrument_df["SEM_TRADING_SYMBOL"] == tradingsymbol) |
            (instrument_df["SEM_CUSTOM_SYMBOL"] == tradingsymbol)) &
            (instrument_df["SEM_EXM_EXCH_ID"] == instrument_exchange[exchange])
        ]
        if sec.empty:
            raise Exception(f"Tradingsymbol not found: {tradingsymbol} ({exchange})")
        return str(sec.iloc[-1]["SEM_SMST_SECURITY_ID"])


# ________________________________________________________________________________________________________________________

    def _map_conditional_exchange_segment(self, exchange: str) -> str:
        """
        Conditional Trigger API supports only Equities and Indices.
        Dhan expects exchangeSegment: NSE_EQ / BSE_EQ / IDX_I
        """
        ex = exchange.upper().strip()
        if ex == "NSE":
            return "NSE_EQ"
        if ex == "BSE":
            return "BSE_EQ"
        if ex in ("IDX", "IDX_I"):
            return "IDX_I"
        raise Exception("Conditional trigger supports only NSE/BSE/IDX (Equities/Indices).")


    def _map_exchange_segment(self, exchange: str):
        exchange = exchange.upper().strip()
        script_exchange = {
            "NSE": self.Dhan.NSE,
            "NFO": self.Dhan.NSE_FNO,
            "BSE": self.Dhan.BSE,
            "BFO": self.Dhan.BSE_FNO,
            "MCX": self.Dhan.MCX,
            "CUR": self.Dhan.CUR
        }
        if exchange not in script_exchange:
            raise Exception(f"Invalid exchange: {exchange}")
        return script_exchange[exchange]

# ________________________________________________________________________________________________________

        # token for instrument

		# security_id = self._resolve_security_id(tradingsymbol, exchange)
		
        # exchange_segment = self._map_conditional_exchange_segment(exchange)


#_
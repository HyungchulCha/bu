from _c import *
from _u import *
from dateutil.relativedelta import *
import os
import copy
import time
import datetime
import threading
import pyupbit


class BotUpbit():


    def __init__(self):

        self.is_aws = True
        self.access_key = UB_ACCESS_KEY_AWS if self.is_aws else UB_ACCESS_KEY_NAJU
        self.secret_key = UB_SECRET_KEY_AWS if self.is_aws else UB_SECRET_KEY_NAJU
        self.ubt = pyupbit.Upbit(self.access_key, self.secret_key)
        
        self.q_l = []
        self.b_l = []
        self.r_l = []
        self.o_l = {}

        self.time_order = None
        self.time_rebalance = None

        self.bool_start = False
        self.bool_balance = False
        self.bool_order = False
        
        self.prc_ttl = 0
        self.prc_lmt = 0
        self.prc_buy = 0

        self.const_up = 500000000
        self.const_dn = 5500

    
    def init_per_day(self):

        if self.bool_balance == False:

            tn = datetime.datetime.now()
            tn_0 = tn.replace(hour=0, minute=0, second=0)
            tn_d = int(((tn - tn_0).seconds) % 300)
            print(f'{tn_d} Second')

            if tn_d <= 150:
                time.sleep(300 - tn_d - 150)
            else:
                time.sleep(300 - tn_d + 150)

            self.bool_balance = True

        print('##############################')

        self.ubt = pyupbit.Upbit(self.access_key, self.secret_key)

        self.q_l = pyupbit.get_tickers("KRW")
        prc_ttl, prc_lmt, _, bal_lst  = self.get_balance_info(self.q_l)
        self.b_l = list(set(self.q_l + bal_lst))
        self.r_l = list(set(bal_lst).difference(self.q_l))
        self.prc_ttl = prc_ttl if prc_ttl < self.const_up else self.const_up
        self.prc_ttl = 5000000
        self.prc_lmt = prc_lmt if prc_ttl < self.const_up else prc_lmt - (prc_ttl - self.const_up)
        prc_buy = self.prc_ttl / (len(self.q_l) * 3)
        self.prc_buy = prc_buy if prc_buy > self.const_dn else self.const_dn

        if os.path.isfile(FILE_URL_TIKR_3M):
            self.o_l = load_file(FILE_URL_TIKR_3M)
        else:
            self.o_l = {}
            save_file(FILE_URL_TIKR_3M, self.o_l)

        for tk in self.b_l:
            if not (tk in self.o_l):
                self.get_tiker_data_init(tk)

        if self.prc_lmt < self.prc_buy:
            line_message('BotUpbit Insufficient Balance !!!')

        int_prc_ttl = int(self.prc_ttl)
        int_prc_lmt = int(self.prc_lmt)
        len_bal_lst = len(self.b_l)

        line_message(f'BotUpbit \nTotal Price : {int_prc_ttl:,} KRW \nLimit Price : {int_prc_lmt:,} KRW \nSymbol List : {len_bal_lst}')

        __tn = datetime.datetime.now()
        __tn_min = __tn.minute % 5
        __tn_sec = __tn.second

        self.time_rebalance = threading.Timer(300 - (60 * __tn_min) - __tn_sec + 150, self.init_per_day)
        self.time_rebalance.start()


    def stock_order(self):

        if self.bool_order == False:

            tn = datetime.datetime.now()
            tn_0 = tn.replace(hour=0, minute=0, second=0)
            tn_d = int(((tn - tn_0).seconds) % 300)
            time.sleep(300 - tn_d)
            self.bool_order = True

        _tn = datetime.datetime.now()

        # self.get_remain_cancel(self.b_l)

        _, _, bal_lst, _ = self.get_balance_info(self.q_l)
        sel_lst = []

        for symbol in self.b_l:

            df = self.strategy_rsi(self.gen_ubt_df(symbol, 'minute5', 170))
            is_df = not (df is None)

            if is_df:
                
                df_h = df.tail(2).head(1)
                close = df_h['close'].iloc[-1]
                rsi = df_h['rsi'].iloc[-1]
                rsi_prev = df_h['rsi_prev'].iloc[-1]
                volume_osc = df_h['volume_osc'].iloc[-1]

                cur_prc = float(close)
            
                is_symbol_bal = symbol in bal_lst
                is_psb_sel = (is_symbol_bal and (cur_prc * bal_lst[symbol]['b'] > self.const_dn))
                ol_bool_buy = copy.deepcopy(self.o_l[symbol]['bool_buy'])
                is_nothing = ol_bool_buy and ((not is_symbol_bal) or (is_symbol_bal and (cur_prc * bal_lst[symbol]['b'] < self.const_dn)))

                if is_nothing:
                    self.get_tiker_data_init(symbol)

                if is_psb_sel and ol_bool_buy:

                    bl_balance = copy.deepcopy(bal_lst[symbol]['b'])
                    ol_buy_price = float(copy.deepcopy(self.o_l[symbol]['buy_price']))
                    ol_quantity_ratio = copy.deepcopy(self.o_l[symbol]['quantity_ratio'])
                    ol_bool_sell_1n = copy.deepcopy(self.o_l[symbol]['bool_sell_1n'])
                    ol_bool_sell_2p = copy.deepcopy(self.o_l[symbol]['bool_sell_2p'])
                    ol_70_position = copy.deepcopy(self.o_l[symbol]['70_position'])
                    sell_qty = bl_balance * (1 / ol_quantity_ratio)
                    is_psb_sel_div = (cur_prc * sell_qty) > self.const_dn

                    if (not is_psb_sel_div) and ol_quantity_ratio > 1:
                        sell_qty = bl_balance * (1 / ol_quantity_ratio - 1)

                    if rsi <= 50 and ol_bool_sell_1n:
                        self.ubt.sell_market_order(symbol, bl_balance)
                        self.get_tiker_data_init(symbol)

                        _ror = get_ror(ol_buy_price, cur_prc)
                        print(f'Sell - Symbol: {symbol}, Profit: {round(_ror, 4)}')
                        sel_lst.append({'c': '[S] ' + symbol, 'r': round(_ror, 4)})

                    elif (cur_prc / ol_buy_price) >= 1.02 and (not ol_bool_sell_2p):
                        self.ubt.sell_market_order(symbol, sell_qty)
                        self.o_l[symbol]['quantity_ratio'] = ol_quantity_ratio - 1
                        self.o_l[symbol]['bool_sell_1n'] = True
                        self.o_l[symbol]['bool_sell_2p'] = True

                        if (not is_psb_sel_div) and ol_quantity_ratio > 1:
                            self.o_l[symbol]['quantity_ratio'] = ol_quantity_ratio - 2

                        if self.o_l[symbol]['quantity_ratio'] == 0:
                            self.get_tiker_data_init(symbol)

                        _ror = get_ror(ol_buy_price, cur_prc)
                        print(f'Sell - Symbol: {symbol}, Profit: {round(_ror, 4)}')
                        sel_lst.append({'c': '[S] ' + symbol, 'r': round(_ror, 4)})

                    elif rsi >= 70 and ((ol_70_position == '70_down') or (ol_70_position == '70_up' and (rsi_prev < rsi))):
                        self.ubt.sell_market_order(symbol, sell_qty)
                        self.o_l[symbol]['quantity_ratio'] = ol_quantity_ratio - 1
                        self.o_l[symbol]['bool_sell_1n'] = True

                        if (not is_psb_sel_div) and ol_quantity_ratio > 1:
                            self.o_l[symbol]['quantity_ratio'] = ol_quantity_ratio - 2

                        if self.o_l[symbol]['quantity_ratio'] == 0:
                            self.get_tiker_data_init(symbol)

                        _ror = get_ror(ol_buy_price, cur_prc)
                        print(f'Sell - Symbol: {symbol}, Profit: {round(_ror, 4)}')
                        sel_lst.append({'c': '[S] ' + symbol, 'r': round(_ror, 4)})
                

                if (rsi <= 30) and (rsi_prev > rsi) and (volume_osc > 0):

                    is_psb_ord = self.prc_lmt > self.prc_buy
                    is_remain_symbol = symbol in self.r_l
                    buy_qty = float(self.prc_buy / cur_prc)

                    if is_psb_ord and (not is_remain_symbol):

                        self.ubt.buy_market_order(symbol, self.prc_buy)
                        ol_bool_buy = copy.deepcopy(self.o_l[symbol]['bool_buy'])
                        ol_quantity_ratio = copy.deepcopy(self.o_l[symbol]['quantity_ratio'])

                        if ol_bool_buy:
                            self.o_l[symbol]['buy_price'] = cur_prc
                            self.o_l[symbol]['quantity_ratio'] = ol_quantity_ratio + 1
                        else:
                            self.o_l[symbol] = {
                                'bool_buy': True,
                                'buy_price': cur_prc,
                                'quantity_ratio': 2,
                                'bool_sell_1n': False,
                                'bool_sell_2p': False,
                                '70_position': ''
                            }

                        print(f'Buy - Symbol: {symbol}, Balance: {buy_qty}')
                        sel_lst.append({'c': '[B] ' + symbol, 'r': (buy_qty)})
                        

                self.o_l[symbol]['70_position'] = '70_down' if rsi < 70 else '70_up'


        save_file(FILE_URL_TIKR_3M, self.o_l)
        print(self.o_l)

        sel_txt = ''
        for sl in sel_lst:
            sel_txt = sel_txt + '\n' + str(sl['c']) + ' : ' + str(sl['r'])

        __tn = datetime.datetime.now()
        __tn_min = __tn.minute % 5
        __tn_sec = __tn.second

        self.time_backtest = threading.Timer(300 - (60 * __tn_min) - __tn_sec, self.stock_order)
        self.time_backtest.start()

        int_prc_ttl = int(self.prc_ttl)
        str_start = _tn.strftime('%Y/%m/%d %H:%M:%S')
        str_end = __tn.strftime('%Y/%m/%d %H:%M:%S')

        line_message(f'BotUpbit \nStart : {str_start}, \nEnd : {str_end}, \nTotal Price : {int_prc_ttl:,} KRW {sel_txt}')
    

    # Tiker Data Init
    def get_tiker_data_init(self, tk):
        self.o_l[tk] = {
            'bool_buy': False,
            'buy_price': 0,
            'quantity_ratio': 0,
            'bool_sell_1n': False,
            'bool_sell_2p': False,
            '70_position': ''
        }
    
    
    # Strategy RSI
    def strategy_rsi(self, df):
        if not (df is None):
            df['rsi'] = indicator_rsi(df['close'], 14)
            df['rsi_prev'] = df['rsi'].shift()
            df['volume_osc'] = indicator_volume_oscillator(df['volume'], 5, 10)
            return df
    

    # Generate DataFrame
    def gen_ubt_df(self, tk, tf, lm):
        ohlcv = pyupbit.get_ohlcv(ticker=tk, interval=tf, count=lm)
        if not (ohlcv is None) and len(ohlcv) >= lm:
            return ohlcv
        

    # Balance Code List    
    def get_balance_info(self, tks):
        bal_cur = pyupbit.get_current_price(tks)
        bal_lst = self.ubt.get_balances()
        bal_krw = 0
        prc = 0
        obj = {}
        lst = []
        if len(bal_lst) > 0:
            for bl in bal_lst:
                avgp = float(bl['avg_buy_price'])
                blnc = float(bl['balance'])
                tikr = bl['unit_currency'] + '-' + bl['currency']
                if tikr != 'KRW-KRW':
                    obj[tikr] = {
                        'a': avgp,
                        'b': blnc
                    }
                    if tikr in bal_cur:
                        prc = prc + (bal_cur[tikr] * blnc)
                    lst.append(tikr)
                else:
                    prc = prc + blnc
                    bal_krw = blnc

        return prc, bal_krw, obj, lst
    
        
    # Not Signed Cancel Order
    def get_remain_cancel(self, l):
        for _l in l:
            rmn_lst = self.ubt.get_order(_l)
            if len(rmn_lst) > 0:
                for rmn in rmn_lst:
                    self.ubt.cancel_order(rmn['uuid'])

    
    # All Sell
    def all_sell_order(self):
        _, _, bal_lst, _  = self.get_balance_info(self.q_l)
        for bl in bal_lst:
            resp = self.ubt.sell_market_order(bl, bal_lst[bl]['b'])
            print(resp)
            time.sleep(0.25)


if __name__ == '__main__':

    bu = BotUpbit()
    # bu.init_per_day()
    # bu.stock_order()
    # bu.all_sell_order()

    while True:

        try:

            tn = datetime.datetime.now()
            tn_start = tn.replace(hour=0, minute=0, second=0)

            if tn >= tn_start and bu.bool_start == False:
                bu.init_per_day()
                bu.stock_order()
                bu.bool_start = True

        except Exception as e:

            line_message(f"BotUpbit Error : {e}")
            break
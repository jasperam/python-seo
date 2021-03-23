import numpy as np
import pandas as pd

from jt.invest.constants import YESTODAY, att_db, nav_db, qi_db, trade_db, calendar
from ps.utils import attach_security_type
from ps.data_loader import get_daily_quote

MULTIPLIER_DICT = {
    'IC': 200,
    'IF': 300,
    'IH': 300,
    'CU': 100000
}

EXCLUDED_SECURITY_LIST = ['204001', '204001.SZ']

def get_multiplier():
    def _inner(x):
        return MULTIPLIER_DICT[x[0:2]]
    return _inner

def insert_market_data(df, prices):
    df = df.merge(prices[['symbol','close','pre_close','change_price','change_rate','trade_status','multiplier']], on='symbol', how='left')    
    return df


def get_pos(sdt, edt):
    sql = f'''
        SELECT "date",symbol,volume 
        FROM "position" 
        where account_id like '105_'
        and volume!=0
        and "date" between '{sdt}' and '{edt}'
        order by "date", symbol
    '''
    ret = qi_db.read(sql)
    if not ret.empty:
        ret = ret.loc[~ret.symbol.isin(EXCLUDED_SECURITY_LIST), :].reset_index(drop=True)
        ret = attach_security_type(ret)
        ret.rename(columns={'security_type':'type'}, inplace=True)
    return ret


def get_trade(sdt, edt):
    sql = f'''
        SELECT Trade_dt as 'date', WindCode as 'symbol', (1.5-Side)*2*qty as 'volume', 
        price, case when type = 'S' then qty*price*0.0012 else Commission end as 'fee'
        FROM [dbo].[JasperTradeDetail] 
        where trade_dt between '{sdt}' and '{edt}'
        and account='105'
    '''
    ret = trade_db.read(sql)
    if not ret.empty:
        ret = ret.loc[~ret.symbol.isin(EXCLUDED_SECURITY_LIST), :].reset_index(drop=True)
        ret = attach_security_type(ret)
        ret.rename(columns={'security_type':'type'}, inplace=True)
    return(ret)


def get_dz_pos():
    sql = f'''
        SELECT sub_date,record_date,list_date,symbol,round(real_used_capital/price,0) as "volume", 'STOCK' as "type", price
        FROM "seo_security_detail"
        where product_id='105'
    '''
    ret = att_db.read(sql)
    return ret


def get_amount(sdt, edt):
    sql = f'''
        select trade_dt as 'date', totalasset
        from [dbo].[JasperAccountDetail]
        where trade_dt between '{sdt}' and '{edt}'
        and account = '105'
    '''
    ret = nav_db.read(sql)
    return ret


def cal_pnl_aft_level(sdt, edt):
    pos = get_pos(sdt, edt)
    trade = get_trade(sdt, edt)
    dz_pos = get_dz_pos()
    dt_lst = calendar.get_trading_calendar(sdt, edt)
    stat = pd.DataFrame()
    for dt in dt_lst:
        print(dt)
        tmp_stat = pd.DataFrame({
            'date': [dt],
            'pos_pnl': [0],
            'trade_pnl': [0],
            'pnl': [0]})

        y_dt = calendar.get_trading_date(dt, -1)        
        tpos = pos.loc[pos.date == y_dt, ['symbol', 'volume', 'type']]
        ttrade = trade.loc[trade.date == dt, ['symbol', 'volume', 'price', 'type', 'fee']]

        if tpos.empty and ttrade.empty:
            continue
       
        stock_prices = get_daily_quote(dt, 'stock', encoding_='gbk')      
        hkstock_prices = get_daily_quote(dt, 'hkstock', encoding_='gbk') 
        forex_prices = get_daily_quote(dt, 'forex')  
        hkstock_prices['forex'] = forex_prices.loc[forex_prices['symbol'].str.contains('HKDCNY'), 'close'].to_numpy()[0]
        future_prices = get_daily_quote(dt, 'future')
        cta_prices = get_daily_quote(dt, 'cta')

        future_prices['multiplier'] = future_prices['symbol'].apply(get_multiplier())

        for _type in ['STOCK', 'FUTURE', 'CTA', 'HK']:
            _pos = tpos[tpos.type==_type]
            _trade = ttrade[ttrade.type==_type]

            if _pos.empty and _trade.empty:
                continue

            if _type == 'STOCK':
                if not _pos.empty:
                    _pos = _pos.merge(stock_prices[['symbol', 'change_price']], on='symbol', how='left')
                    _pos['pos_pnl'] = _pos['change_price']*_pos['volume'] 
                if not _trade.empty:
                    _trade = _trade.merge(stock_prices[['symbol', 'close']], on='symbol', how='left')
                    _trade['trade_pnl'] = (_trade['close']-_trade['price'])*_trade['volume'] - _trade['fee']
            elif _type == 'HK':
                if not _pos.empty:
                    _pos = _pos.merge(hkstock_prices[['symbol', 'change_price', 'forex']], on='symbol', how='left')
                    _pos['pos_pnl'] = _pos['change_price']*_pos['volume']*_pos['forex']
                if not _trade.empty:
                    _trade = _trade.merge(hkstock_prices[['symbol', 'close', 'forex']], on='symbol', how='left')
                    _trade['trade_pnl'] = ((_trade['close']-_trade['price'])*_trade['volume'] - _trade['fee'])*_trade['forex']
            elif _type == 'FUTURE':
                if not _pos.empty:
                    _pos = _pos.merge(future_prices[['symbol','settle','pre_settle','multiplier']], on='symbol', how='left')      
                    _pos['pos_pnl'] = (_pos['settle']-_pos['pre_settle'])*_pos['multiplier']*_pos['volume'] 
                if not _trade.empty:
                    _trade = _trade.merge(future_prices[['symbol','settle','multiplier']], on='symbol', how='left') 
                    _trade['trade_pnl'] = (_trade['settle']-_trade['price'])*_trade['multiplier']*_trade['volume'] - _trade['fee']
            elif _type == 'CTA':
                if not _pos.empty:
                    _pos = _pos.merge(cta_prices[['symbol','change_price','multiplier']], on='symbol', how='left')      
                    _pos['pos_pnl'] = _pos['change_price']*_pos['multiplier']*_pos['volume'] 
                if not _trade.empty:
                    _trade = _trade.merge(cta_prices[['symbol','settle','multiplier']], on='symbol', how='left') 
                    _trade['trade_pnl'] = (_trade['settle']-_trade['price'])*_trade['multiplier']*_trade['volume'] - _trade['fee']
            else:
                print(f'undefined type {_type}')

            if not _pos.empty:               
                pos_pnl = _pos['pos_pnl'].sum()
            else:
                pos_pnl = 0

            if not _trade.empty:                
                trade_pnl = _trade['trade_pnl'].sum()
            else:
                trade_pnl = 0

            tmp_stat['pos_pnl'] = pos_pnl + tmp_stat['pos_pnl']
            tmp_stat['trade_pnl'] = trade_pnl + tmp_stat['trade_pnl']
            # tmp_stat['pnl'] = pos_pnl + trade_pnl + tmp_stat['pnl']

        def cal_dz_valuation(dt, flag):
            def _inner(x):
                close_price = x.pre_close if flag == -1 else x.close            
                if x.price < close_price:
                    diff = calendar.date_diff(x.record_date, dt)
                    ret = x.price + (close_price - x.price) * diff / 120
                else:
                    ret = close_price
                return ret
            return _inner

        # 计算定增估值
        n_dt = calendar.get_trading_date(dt, 1)
        tdz_pos = dz_pos.loc[(dz_pos.record_date<=y_dt) & (dz_pos.list_date>n_dt), ['symbol', 'volume', 'type', 'record_date', 'list_date', 'price']]
        if not tdz_pos.empty:
            tdz_pos = tdz_pos.merge(stock_prices[['symbol', 'close', 'pre_close']], on='symbol', how='left')
            tdz_pos['est_close'] = tdz_pos.apply(cal_dz_valuation(dt, 0), axis=1)
            tdz_pos['est_y_close'] = tdz_pos.apply(cal_dz_valuation(y_dt, -1), axis=1)
            tdz_pos['pos_pnl'] = (tdz_pos['est_close'] - tdz_pos['est_y_close']) * tdz_pos['volume']
            tmp_stat['tdz_pos_pnl'] = tdz_pos['pos_pnl'].sum()
        else:
            tmp_stat['tdz_pos_pnl'] = 0

        tmp_stat['pnl'] = tmp_stat['pos_pnl'] + tmp_stat['trade_pnl'] + tmp_stat['tdz_pos_pnl']

        stat = stat.append(tmp_stat)
    
    acc_info = get_amount(sdt, edt)
    acc_info['date'] = acc_info['date'].astype(str)
    stat = stat.merge(acc_info, on='date', how='left')
    return stat

if __name__ == '__main__':
    sdt = '20210129'
    edt = YESTODAY
    stat = cal_pnl_aft_level(sdt, edt)

    # stat = pd.read_excel(r'e:\temp\x.xlsx')
    # acc_info = get_amount(sdt, edt)
    # acc_info['date'] = acc_info['date'].astype(str)
    # stat['date'] = stat['date'].astype(str)
    # stat = stat.merge(acc_info, on='date', how='left')
    stat.to_excel(r'e:\temp\x.xlsx', index=False)
    
    pass

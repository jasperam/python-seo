#
# research.py
# @author Neo Lin
# @description 
# @created 2021-01-19T10:37:01.129Z+08:00
# @last-modified 2021-03-22T08:58:07.503Z+08:00
#
import os
import numpy as np
import pandas as pd
import statsmodels.api as sm

from jt.invest.constants import w_db, calendar
from jt.invest.statistics import merge_A_and_B_with_lag
from jt.invest.utils import get_nlag_report_period, sm_model2dataframe


def get_fund_hold_ratio():
    """
    get fund hold ratio
    """
    def _inner(_df):   
        sql = f'''
            SELECT sum(F_PRT_STKQUANTITY)/10000 as 'fund_hold_qty'
            FROM [dbo].[CHINAMUTUALFUNDSTOCKPORTFOLIO] a, 
            (select S_INFO_WINDCODE, max(ANN_DATE) as max_date 
                from [dbo].[CHINAMUTUALFUNDSTOCKPORTFOLIO]
            where ANN_DATE <= '{_df.date}'
            group by S_INFO_WINDCODE) b
            where a.ANN_DATE = b.max_date
            and a.CRNCY_CODE = 'CNY'
            and a.S_INFO_WINDCODE = b.S_INFO_WINDCODE
            and a.S_INFO_STOCKWINDCODE = '{_df.symbol}'
            GROUP BY S_INFO_STOCKWINDCODE
        '''
        tpr = w_db.read(sql)
        ret = 0 if tpr.empty else tpr.fund_hold_qty[0]       
        return ret
    return _inner


def get_CAGR_net_profit_deducted():
    """
    get 3 years net profit CAGR
    """
    def _inner(_df):
        sql_template = """
            select sum(ROUND(S_QFA_DEDUCTEDPROFIT/100000000,2)) as 'q_deducted_profit_sum'
            from ASHAREFINANCIALINDICATOR 
            where REPORT_PERIOD between '{0}' and '{1}'
            and S_INFO_WINDCODE = '{2}'
        """    
        y_rp = get_nlag_report_period(_df.date)
        tpr = w_db.read(sql_template.format(y_rp, y_rp, _df.symbol))
        if tpr.empty:
            y_rp = get_nlag_report_period(_df.date, -1)
            
        y_4_rp = get_nlag_report_period(y_rp, -3)
        tpr = w_db.read(sql_template.format(y_4_rp, y_rp, _df.symbol))

        y1_rp = get_nlag_report_period(y_rp, -12)            
        y1_4_rp = get_nlag_report_period(y_4_rp, -12)
        tpr1 = w_db.read(sql_template.format(y1_4_rp, y1_rp, _df.symbol))
        
        if (tpr1.q_deducted_profit_sum[0] is None) or (tpr.q_deducted_profit_sum[0] is None):
            ret = np.nan
        else:
            if tpr.q_deducted_profit_sum[0] < 0:
                ret = 1 - round(tpr.q_deducted_profit_sum[0]/tpr1.q_deducted_profit_sum[0], 3) ** (1/3)
            else:
                ret = round(tpr.q_deducted_profit_sum[0]/tpr1.q_deducted_profit_sum[0], 3) ** (1/3)-1
        return ret
    return _inner


def get_CAGR_net_profit():
    """
    get 3 years net profit CAGR
    """
    def _inner(_df):
        sql_template = """
            select sum(ROUND(NET_PROFIT_EXCL_MIN_INT_INC/10000,2)) as 'profit_sum'
            from ASHAREINCOME 
            where REPORT_PERIOD between '{0}' and '{1}'
            and S_INFO_WINDCODE = '{2}'
            and STATEMENT_TYPE = '408002000'
        """    
        y_rp = get_nlag_report_period(_df.date)
        tpr = w_db.read(sql_template.format(y_rp, y_rp, _df.symbol))
        if tpr.empty:
            y_rp = get_nlag_report_period(_df.date, -1)
            
        y_4_rp = get_nlag_report_period(y_rp, -3)
        tpr = w_db.read(sql_template.format(y_4_rp, y_rp, _df.symbol))

        y1_rp = get_nlag_report_period(y_rp, -12)            
        y1_4_rp = get_nlag_report_period(y_4_rp, -12)
        tpr1 = w_db.read(sql_template.format(y1_4_rp, y1_rp, _df.symbol))

        if tpr.profit_sum[0] < 0:
            ret = 1 - round(tpr.profit_sum[0]/tpr1.profit_sum[0], 3) ** (1/3)
        else:
            ret = round(tpr.profit_sum[0]/tpr1.profit_sum[0], 3) ** (1/3)-1
        return ret
    return _inner


def cal_hold_index_alpha(px, index_px, bm='h00905.CSI'):
    def _inner(_df):
        sdt = _df.date
        edt = calendar.get_trading_date(_df.list_date, -1)
        s_pct = px.loc[(px.date==edt) & (px.symbol==_df.symbol), 'adjclose'].to_numpy()[0] / \
            px.loc[(px.date==sdt) & (px.symbol==_df.symbol), 'adjclose'].to_numpy()[0] - 1
        i_pct = index_px.loc[(index_px.date==edt) & (index_px.symbol==bm), 'close'].to_numpy()[0] / \
            index_px.loc[(index_px.date==sdt) & (index_px.symbol==bm), 'close'].to_numpy()[0] - 1
        return s_pct - i_pct
    return _inner


def cal_neighbor_alpha(sdt, edt, symbol, nalpha):  
    n_a = nalpha.loc[(nalpha.date>=sdt) & (nalpha.date<=edt) & (nalpha.symbol==symbol), ['date','neighbor_alpha']].sort_values(by='date').copy()
    n_a['neighbor_alpha'] = n_a['neighbor_alpha'] + 1
    return n_a['neighbor_alpha'].prod()-1


def cal_hold_neighbor_alpha(nalpha):
    def _inner(_df):
        sdt = _df.date
        edt = calendar.get_trading_date(_df.list_date, -1)
        return cal_neighbor_alpha(sdt, edt, _df.symbol, nalpha)
    return _inner


def cal_list_neighbor_alpha(nalpha):
    def _inner(_df):
        sdt = _df.list_date
        edt = calendar.get_trading_date(_df.list_date, 10)
        return cal_neighbor_alpha(sdt, edt, _df.symbol, nalpha)
    return _inner
    

def get_list_records(sdt, edt, px, index_px, nalpha):    

    sdt = '20200214' if sdt is None else sdt
    edt = YESTODAY if edt is None else edt
    
    sql = f"""
        SELECT S_INFO_WINDCODE as 'symbol', S_FELLOW_DATE as 'date',
            S_FELLOW_OTCDATE as 'otc_date', S_FELLOW_INSTLISTDATE as 'list_date',
            S_FELLOW_PRICE as 'price', S_FELLOW_DISCNTRATIO as 'discount', S_FELLOW_COLLECTION/10000 as 'amount',
            S_FELLOW_AMOUNT as 'volume'
        FROM [dbo].[ASHARESEO]
        where S_FELLOW_INSTLISTDATE between '{sdt}' and '{edt}'
        and S_FELLOW_DATE > '{sdt}'
        and PRICINGMODE=275001000
        and S_FELLOW_ISSUETYPE='439006000' 
        and IS_NO_PUBLIC=1
    """
    _p = w_db.read(sql)
    _p = merge_A_and_B_with_lag(_p, px[['date','symbol','total_shares','close']], n=-1)   
    _p['issue_ratio'] = _p['volume'] / _p['total_shares']
    _p['discount'] = 1 - _p['price']/_p['close'] 
    
    # * ROE
    dt_lst = calendar.get_trading_calendar(sdt, edt)
    roe_df = pd.DataFrame()
    for _dt in dt_lst:
        _file = os.path.join(r'E:\project\research_material\fundamental\roe_3y', f'{_dt}.csv')
        _tdf = pd.read_csv(_file)
        _tdf['date'] = _dt
        roe_df = roe_df.append(_tdf)    
    roe_df.rename(columns={'S_INFO_WINDCODE':'symbol'}, inplace=True)
    _p = merge_A_and_B_with_lag(_p, roe_df, n=-1)
   
    # * Debt Asset ratio
    dt_lst = calendar.get_trading_calendar(sdt, edt)
    d2r_df = pd.DataFrame()
    for _dt in dt_lst:
        _file = os.path.join(r'E:\project\research_material\fundamental\debt2asset', f'{_dt}.csv')
        _tdf = pd.read_csv(_file)
        _tdf['date'] = _dt
        d2r_df = d2r_df.append(_tdf)    
    d2r_df.rename(columns={'S_INFO_WINDCODE':'symbol'}, inplace=True)
    _p = merge_A_and_B_with_lag(_p, d2r_df, n=-1)

    # * fund hold ratio
    _p['fund_hold_qty'] = _p.apply(get_fund_hold_ratio(), axis=1)
    _p['fund_hold_ratio'] = _p['fund_hold_qty'] / _p['total_shares']

    # * 3 year CARG(净利润复合年增长率)
    _p['net_profit_deducted_carg'] = _p.apply(get_CAGR_net_profit_deducted(), axis=1)
    _p['net_profit_carg'] = _p.apply(get_CAGR_net_profit(), axis=1)

    # * bm_alpha & neighbor alpha
    _p['hold_alpha'] = _p.apply(cal_hold_index_alpha(px, index_px), axis=1)
    _p['hold_nalpha'] = _p.apply(cal_hold_neighbor_alpha(nalpha), axis=1)
    _p['list_nalpha'] = _p.apply(cal_list_neighbor_alpha(nalpha), axis=1)

    # * Transaction multiple 
    _p = merge_A_and_B_with_lag(_p, px[['date','symbol','med60amount']], n=-1)
    _p['liq_multi'] = _p['amount'] / (_p['med60amount'] / 10)

    return _p


def stat_ols(facotr_lst, y_lst, p):
    # for _y in y_lst:
    #     for _factor in facotr_lst:
    #         df = p.copy()
    #         df.dropna(subset=[_y, _factor], inplace=True)
            
    #         x = df[_factor]
    #         y = df[_y]
    #         result = sm.OLS(y, x).fit()

    #         _df = pd.DataFrame({f'{_y}':sm_model2dataframe(result)})
    #         print(_df)

    R = pd.DataFrame()
    for _y in y_lst:
        df = p.copy()
        df.dropna(subset=[_y, *facotr_lst], inplace=True)
        
        x = df[facotr_lst]
        y = df[_y]
        result = sm.OLS(y, x).fit()

        _df = pd.DataFrame({f'{_y}':sm_model2dataframe(result)})
        print(_df)
        R = pd.concat([R, _df], axis=1)
    R.to_excel(rf'E:\project\research_material\seo\list_summary\list_ols_{sdt}_{edt}.xlsx')
    print(R)


def stat_group(sdt, edt, factor_lst, p):

    for _factor in factor_lst:
        df = p.copy()    
        df.dropna(subset=[_factor], inplace=True)
        # 分组
        df['rank'] = np.ceil(df[_factor].rank(pct=True)*5)
        stats = df.groupby('rank').apply(lambda x: 
            pd.DataFrame(data=[[
                x.hold_alpha.count(),
                x[_factor].min(),                    
                x[_factor].max(),                    
                x.hold_alpha.mean(),
                x.hold_alpha.median(),
                x.hold_nalpha.mean(),
                x.hold_nalpha.median(),
                x.list_nalpha.mean(),
                x.list_nalpha.median()
            ]],
            columns = [
                'nstk',
                'min',
                'max',
                'hold_alpha_mean',
                'hold_alpha_median',
                'hold_nalpha_mean',
                'hold_nalpha_median',
                'list_nalpha_mean',
                'list_nalpha_median'
            ]))

        stats.to_excel(rf'E:\project\research_material\seo\list_summary\list_group_{_factor}_{sdt}_{edt}.xlsx', index=False)


def alpha_backtest(sdt, edt, px, index_px, nalpha):
    _p = get_list_records(sdt, edt, px, index_px, nalpha)
    _p.to_excel(rf'E:\project\research_material\seo\list_summary\list_summary_{sdt}_{edt}.xlsx')
    # _p = pd.read_excel(rf'E:\project\research_material\seo\list_summary\list_summary_{sdt}_{edt}.xlsx')
    _p.replace([np.inf, -np.inf], np.nan, inplace=True)

    factor_lst = ['discount', 'issue_ratio', 'ROE_DEDUCTED_AVG_3Y', 'DEBTTOASSET', 'fund_hold_ratio', 
        'net_profit_deducted_carg', 'liq_multi']
    
    y_lst = ['hold_alpha', 'hold_nalpha', 'list_nalpha']
    
    # stat_ols(factor_lst, y_lst, _p)
    stat_group(sdt, edt, factor_lst, _p)
    pass

if __name__ == '__main__':
    from jt.invest.constants import px_path, index_px_path, neighbor_alpha_gby_size_path, YESTODAY
    px = pd.read_pickle(px_path)
    index_px = pd.read_pickle(index_px_path)
    nalpha = pd.read_pickle(neighbor_alpha_gby_size_path)
    sdt = '20200214'
    edt = '20210312'
    # _p = get_list_records(sdt, edt, px, index_px, nalpha)
    # _p.to_excel(rf'E:\project\research_material\seo\list_summary_{sdt}_{edt}.xlsx', index=False)
    alpha_backtest(sdt, edt, px, index_px, nalpha)
    
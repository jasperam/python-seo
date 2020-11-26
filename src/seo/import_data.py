#
# import_data.py
# @author Neo Lin
# @description import data of seo from server
# @created 2020-04-06T16:13:38.976Z+08:00
# @last-modified 2020-11-26T09:46:21.337Z+08:00
#
import os
import re
import numpy as np
import pandas as pd

from jt.utils.db import PgSQLLoader

ATTDB = PgSQLLoader('attribution')
SERVER_ROOT = r'\\192.168.1.75\定增2.0'
DATE_STR_PATTERN = r'\d{4}(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])'

def import_seo_share_info(date_ = None):
    """
    import ding sheng's purchase infomation
    """
    _file_path = os.path.join(SERVER_ROOT, '11 运营管理', '大岩定晟认购申赎情况.xlsx')
    df = pd.read_excel(_file_path)
    if not date_ is None:
        assert re.match(DATE_STR_PATTERN, date_), 'date_ should be like "yyyymmdd"!' 
        df = df.loc[df['purchase_date']>=int(date_), :]
    if not df.empty:
        ATTDB.upsert('public.seo_purchase_detail', df, keys_=['purchase_date','name','shares','product_id'])


def import_in_project(date_ = None):
    """
    import projects that is participated
    """
    col_dict = {
        '序号': 'id',
        '代码': 'symbol',
        '简称': 'name',
        '类型': 's_type',
        '申万一级行业': 'industry_sw',
        '报价日': 'sub_date',
        '股权登记日': 'record_date',
        '解禁日': 'list_date',
        '数量': 'quantity',
        '报价前一日收盘价': 'pre_price',
        '成交价': 'price',
        '成交折扣': 'discount_rate',
        '成本': 'cost',
        '产品编号': 'product_id',
        '产品名称': 'product_name',
        '转股价': 'transfer_price',
        '状态': 's_status',
        '修改日期': 'update_date',
        '备注': 'remarks',
        '是否期权': 'is_option'
    }
    _file_path = os.path.join(SERVER_ROOT, '11 运营管理', '参与项目汇总.xlsx')
    df = pd.read_excel(_file_path).fillna(0)
    df = df.loc[:, col_dict.keys()]
    df.rename(columns=col_dict, inplace=True)
    if not date_ is None:
        assert re.match(DATE_STR_PATTERN, date_), 'date_ should be like "yyyymmdd"!' 
        df = df.loc[df['sub_date']>=int(date_), :]
    if not df.empty:
        df['record_date'] = df['record_date'].apply(lambda x: str(int(x)) if not x is None else x)
        df['list_date'] = df['list_date'].apply(lambda x: str(int(x)) if not x is None else x)
        ATTDB.upsert('public.seo_security_detail', df, keys_=['id'])


def import_project_sub_info(date_ = None):
    col_dict = {       
        '代码': 'symbol',
        '名称': 'name',      
        '预计报价日期': 'estimated_sub_date',
        '最低一份金额(亿元)': 'min_purchase_amount',        
    }
    _file_path = os.path.join(SERVER_ROOT, '定增项目追踪.xlsx')
    df = pd.read_excel(_file_path, sheet_name='定增筛选')
    df.rename(columns=col_dict, inplace=True)
    df = df.loc[not (pd.to_numeric(df['estimated_sub_date'], errors='coerce').isna()),['symbol','name','estimated_sub_date','min_purchase_amount']]
    if not date_ is None:
        assert re.match(DATE_STR_PATTERN, date_), 'date_ should be like "yyyymmdd"!' 
        df = df.loc[df['estimated_sub_date']>=int(date_), :]
    df['min_purchase_amount'] = df['min_purchase_amount'] *10000
    df['estimated_margin'] = df['min_purchase_amount'] * 0.15
    df['estimated_sub_date'] = df['estimated_sub_date'].astype(int).astype(str)
    if not df.empty:
        ATTDB.upsert('public.seo_estimated_sub_info', df, keys_=['symbol','estimated_sub_date'])
    pass

if __name__ == "__main__":
    # import_seo_share_info()
    import_in_project()
    # import_project_sub_info()
    pass

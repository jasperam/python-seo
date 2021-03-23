import os

from jt.invest.constants import w_db, futils
from jt.utils.misc.log import Logger

from seo.constants import root

log = Logger()

def check_project(start_dt='20200401'):
    # 检查已完成项目是否已经归档
    all_project = futils.get_current_folder_dirs(os.path.join(root, '00 项目资料'))

    sql = f"""
        select b.S_INFO_NAME+'_'+left(a.S_INFO_WINDCODE,6) as symbol
        from ASHARESEO a, ASHAREDESCRIPTION b
        where a.S_FELLOW_PROGRESS='3'
        and a.S_FELLOW_DATE>='{start_dt}'
        and a.IS_NO_PUBLIC=1 
        and a.PRICINGMODE='275001000'
        and a.S_FELLOW_ISSUETYPE = '439006000'
        and a.S_INFO_WINDCODE = b.S_INFO_WINDCODE
        order by a.S_FELLOW_DATE desc;
    """
    df = w_db.read(sql)

    for _p in all_project:        
        if _p in df.symbol.values:
            f = os.path.join(root, '00 项目资料')
            t = os.path.join(root, '00 项目资料', '00 归档')
            log.info(f'Archive dir {_p}')
            futils.move_dir(_p, f, t, copy_=False, replace_=True)
    
    all_material = futils.get_current_folder_files(os.path.join(root, '01 路演材料'))

    for _m in all_material:
        for _n in df.symbol.values:
            if _n[:-7] in _m:
                f = os.path.join(root, '01 路演材料')
                t = os.path.join(root, '01 路演材料', '已实施项目')
                log.info(f'Archive mateiral {_m}')
                futils.move_file(_m, f, t, copy_=False)

    # 检查未跟踪项目
    sql = f"""
        select b.S_INFO_NAME+'_'+left(a.S_INFO_WINDCODE,6) as symbol, LEADUNDERWRITER as writer
        from ASHARESEO a, ASHAREDESCRIPTION b
        where a.S_FELLOW_PROGRESS='5'
        and a.S_FELLOW_APPROVEDDATE>='{start_dt}'
        and a.IS_NO_PUBLIC=1 
        and a.PRICINGMODE='275001000'
        and a.S_FELLOW_ISSUETYPE = '439006000'
        and a.S_INFO_WINDCODE = b.S_INFO_WINDCODE
        order by a.S_FELLOW_APPROVEDDATE desc;
    """

    df = w_db.read(sql)
    all_project = futils.get_current_folder_dirs(os.path.join(root, '00 项目资料'))

    p_name, p_writer = list(), list()
    for i in df.index:
        if not df.loc[i, 'symbol'] in all_project:
            p_name.append(df.loc[i, 'symbol'][:-7])
            p_writer.append(df.loc[i, 'writer'])
            log.info(f"Not tracked project {df.loc[i, 'symbol']}, main writer {df.loc[i, 'writer']}")
    log.info(p_name)
    log.info(p_writer)
    return p_name, p_writer

 
if __name__ == "__main__":
   check_project('20210223')

    

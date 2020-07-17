#
# report.py
# @author Neo Lin
# @description SEO risk report
# @created 2020-05-26T14:19:21.122Z+08:00
# @last-modified 2020-07-16T17:52:03.485Z+08:00
#
import os
import docx

from datetime import datetime
from jt.invest.constants import w_db
from jt.utils.fs.utils import Utils as futils
from jt.utils.misc.log import Logger

logger = Logger(__name__)

def make_intent_letter(broker, project):
    _month = str(datetime.now().month)
    _day = str(datetime.now().day) 
    root_path = r'\\192.168.1.75\定增2.0'  

    sql = f"""
        select S_INFO_CODE as code, s_info_name as name
        from [dbo].[ASHAREDESCRIPTION] where S_INFO_NAME in ('{"','".join(project)}')
    """
    ret = w_db.read(sql)

    for _pro in project:
        _doc = docx.Document(os.path.join(root_path, u'30 合作券商/认购意向函.docx'))
        for _p in _doc.paragraphs:
            if ('【】' in _p.text) or ('broker' in _p.text) or (u'x月y日' in _p.text):
                for _r in _p.runs:
                    if ('【】' in _r.text):
                        _r.text = _r.text.replace('【】', _pro)
                    if ('broker' in _r.text):
                        _r.text = _r.text.replace('broker', broker)
                    if ('x' in _r.text):
                        _r.text = _r.text.replace(u'x', _month)
                    if ('y' in _r.text):
                        _r.text = _r.text.replace(u'y', _day)
        
        _symbol = ret.loc[ret['name']==_pro, 'code'].to_numpy()[0]
        _dir = os.path.join(root_path, f'00 项目资料\\{_pro}_{_symbol}')
        if not futils.is_dir(_dir):
            futils.make_dir(_dir)

        file_path = os.path.join(_dir, f'{_pro}认购意向函.docx')
        _doc.save(file_path)
        logger.info(f'make intent file: {file_path}')

if __name__ == "__main__":    
    project = [u'新疆天业']
    broker = u'申万宏源证券承销保荐有限责任公司'
    make_intent_letter(broker, project)




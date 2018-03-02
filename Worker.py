#!-*- coding: utf-8 -*-
import os
import sys
import json
import logging
import urlparse
import datetime

reload(sys)
sys.setdefaultencoding('utf-8')
from handler import ckanHandler
from handler import psqlHandler
from handler import apiHandler
from lib import CodeBase

class Worker:
    def __init__(self, *args, **kwargs):
        self.opt = kwargs

        self.psql_post_handler = psqlHandler.psqlHandler(distinction='postgres',**kwargs)
        self.psql_odp_handler = psqlHandler.psqlHandler(distinction='odp',**kwargs)

    def run(self):
        logging.info("Process Start : %s"%(self.__class__.__name__))
        for product_id, client_id, count in self.psql_post_handler._execute(self.opt['sql']['get_count_sql'][0]) :
            update_count_sql = self.opt['sql']['update_count_sql'][0].replace("${api_id}","%s").replace("${client_id}", "%s").replace(" ${count}", "%s")

            self.psql_odp_handler._execute2(update_count_sql, *[count, product_id, client_id])

        logging.info("Process End : %s"%(self.__class__.__name__))

def main():
    module = os.path.basename(sys.argv[0])
    conf_file = ""
    if len(sys.argv) < 2:
        if os.path.exists("api_used_counter.conf"):
            conf_file = "api_used_counter.conf"
        else:
            sys.stderr.write('Usage  : %s config  \n' % (module))
            sys.stderr.write('Example: %s ~/SODAS/conf/%s.conf\n' % (module, os.path.splitext(module)[0]))
            sys.stderr.flush()
            sys.exit()
    else:
        conf_file = sys.argv[1]

    delimiter = ','
    if not os.path.exists(conf_file):
        sys.stderr.write("Not Found Config File.\n")
        sys.stderr.flush()
        sys.exit()

    obj = CodeBase.CodeBase(conf_file, delimiter)
    common_dict = obj.get_conf('COMMON', upper=False)
    path_dict = obj.get_conf('PATH', upper=False)
    sql_dict = obj.get_conf('SQL', upper=False, used_split=False)
    # logging.info(path_dict)
    obj.creat_path(path_dict)
    if common_dict.has_key("log_mode"):
        obj.log_init(path_dict['log_path'][0], mode=common_dict["log_mode"][0])
    else:
        obj.log_init(path_dict['log_path'][0])

    obj.view_conf(common_dict)
    obj.view_conf(path_dict)
    obj.view_conf(sql_dict)
    opt = dict()
    opt['path'] = path_dict.copy()
    opt['sql'] = sql_dict.copy()
    conf_path = path_dict["config_data_path"][0]

    for info_dict in obj.open_json(conf_path):
        opt.update(info_dict)

    try :
        info_dict = obj.open_excel(conf_path, common_dict['key_column'])
        opt.update(info_dict)
    except : pass
    # logging.info(opt)
    proc = Worker(ckanHandler.ckanHandler, **opt)
    proc.run()


if __name__ == '__main__':

    try:
        main()
    except Exception, e:
        logging.exception(e.message)

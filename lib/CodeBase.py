#!/usr/bin/env python
#-*-coding:utf-8-*-
import os
import sys
import json
import glob
import logging
import ConfigParser
from  logging import config


class Singleton(object):
    __instance = None
    @classmethod
    def __getInstance(cls):
        return cls.__instance

    @classmethod
    def instance(cls, *args, **kargs):
        cls.__instance = cls(*args, **kargs)
        cls.instance = cls.__getInstance
        return cls.__instance

class CodeBase():
    def __init__(self, conf_file, delimiter):

        self.cfg = ConfigParser.ConfigParser()
        self.cfg.read(conf_file)
        self.delimiter = delimiter

    def get_conf(self, section, donsplit_option_list = [] , not_split_patt = [], upper = True, used_split = True):

        section_dict = dict()
        options = self.cfg.options(section)

        for option in options:
            if option.isdigit():
                option = int(option)
            else:
                if upper:
                    option = option.upper()
            section_dict[option] = self.cfg.get(section, str(option))
            not_split = False
            for patt in not_split_patt:
                if patt in option:
                    not_split = True
                    break



            if used_split and (self.delimiter in section_dict[option] and not option in donsplit_option_list and not not_split):

                section_dict[option] = section_dict[option].split(self.delimiter)
                for i in range(len(section_dict[option])):
                    if section_dict[option][i].isdigit():
                        section_dict[option][i] = int(section_dict[option][i])
                    else:
                        section_dict[option][i] = section_dict[option][i].strip()
            else:
                if section_dict[option].isdigit():
                    section_dict[option] = [int(section_dict[option])]
                elif section_dict[option].strip().upper() == 'FALSE':
                    section_dict[option] = [False]
                elif section_dict[option].strip().upper() == 'TRUE':
                    section_dict[option] = [True]
                else:
                    section_dict[option] = [section_dict[option].strip()]
        return section_dict

    def get_all_conf( self, donsplit_option_list = [], not_split_patt = [], exclude_sections = [] ):
        conf_dict = dict()
        for section in self.cfg.sections():
            if section in exclude_sections: continue
            conf_dict[section] = self.get_conf(section, donsplit_option_list, not_split_patt)
        return conf_dict

    def creat_path(self, path_dict):
        for key in path_dict:
            for path in path_dict[key]:
                if os.path.exists(path): continue
                else: os.makedirs(path)

    def view_conf(self, dict_obj):
        for key in dict_obj:
            try:
                logging.info(str(key) + ": " + str(dict_obj[key]) +' ' + str(type(dict_obj[key])))
            except Exception, e:
                print str(key) + ": " + str(dict_obj[key]) +' ' + str(type(dict_obj[key]))

    def log_init(self, conf_path, mode="DEBUG", key=None):

        file_name = os.path.basename(sys.argv[0])
        try :  file_name = os.path.splitext(file_name)[0] + '.' + key + '.log'
        except: file_name = os.path.splitext(file_name)[0] + '.log'
        log_name = os.path.join(conf_path, file_name)
        LOGGING_CONF = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters':{
                'fmt_console':{
                    'format':'%(asctime)s, %(lineno)s, %(levelname)s [%(name)s] %(message)s'
                },
                'fmt_rolling':{
                    'format':'[%(asctime)s] %(process)d, "%(filename)s", %(lineno)s, %(funcName)s: %(message)s',
                    'datefmt': '%Y/%m/%d %H:%M:%S'
                }
            },
            'handlers':{
                'console':{
                    'level': 'DEBUG',
                    'class': 'logging.StreamHandler',
                    'formatter': 'fmt_console',
                    'stream': 'ext://sys.stdout',
                },
                'rolling': {
                    'level': 'DEBUG',
                    'class': 'logging.handlers.RotatingFileHandler',
                    'formatter': 'fmt_rolling',
                    'filename': log_name,
                    'mode': 'a',
                    'maxBytes': 1024*1024*10,
                    'backupCount': 10
                }
            },
            'loggers':{
                '': {
                    'level': mode,
                    'handlers': ['console', 'rolling']
                },
            }
        }
        config.dictConfig(LOGGING_CONF)
    # def log_init(self, conf_path, key=None):
    #
    #     file_name = os.path.basename(sys.argv[0])
    #
    #     try :
    #
    #         file_name = os.path.splitext(file_name)[0] + '.' + key + '.log'
    #
    #     except:
    #
    #         file_name = os.path.splitext(file_name)[0] + '.log'
    #
    #     log_file = os.path.join(conf_path, file_name)
    #     Log.Init(Log.CRotatingLog(log_file, 1000000, 10))
    def open_json(self,conf_path):
        try:
            if os.path.exists(conf_path):
                conf_base_path = conf_path
            else:
                conf_base_path = 'info'
            json_files = glob.glob(os.path.join(conf_base_path,"*.json"))

            for filename in json_files:
                with open(filename) as fr:
                    logging.info("get %s"%filename)
                    yield json.loads(fr.read())
        except Exception as e :
            logging.error("fail get file content [ %s ]"%fr.name)
            raise Exception(e.message)

    def open_excel(self, conf_path, key_columes):
        import pandas
        from collections import defaultdict
        if os.path.exists(conf_path):
            conf_base_path = conf_path
        else:
            conf_base_path = 'info'
        excel_files = glob.glob(os.path.join(conf_base_path,"*.xlsx"))
        result_dict = dict()
        for filename in excel_files:
            if '~' in filename : continue
            logging.info("get %s"%filename)
            info = pandas.ExcelFile(filename)
            for sheet_name in info.sheet_names :
                fr = pandas.read_excel(filename, sheet_name=sheet_name)
                standard_dict = defaultdict(dict)
                key_dict_list = list()
                for key in fr:
                    key_dict = defaultdict(dict)
                    for line in str(fr.get(key)).split('\n'):
                        tmp = line.split(' ', 1)
                        #48                   전국 음식점 정보 조회
                        if key in key_columes:
                            standard_dict[key][tmp[0]] = tmp[-1].strip()
                        else :
                            key_dict[key][tmp[0]] = tmp[-1].strip()
                            key_dict_list.append(key_dict)
                for key in standard_dict :
                    if not result_dict.has_key(key):
                        result_dict[key]= dict()
                    for no in standard_dict[key] :
                        for key_dict in key_dict_list:
                            for key2 in key_dict:
                                if not result_dict[key].has_key(standard_dict[key][no]):
                                    result_dict[key][standard_dict[key][no]] = dict()
                                if not result_dict[key][standard_dict[key][no]].has_key(key2):
                                    result_dict[key][standard_dict[key][no]][key2] = dict()
                                result_dict[key][standard_dict[key][no]][key2] = key_dict[key2][no]
        return result_dict
    def __del__(self): pass

def main():
    #test code

    conf_file = '/home/leo/user/jun/conf/MigMQLoader.conf'
    delimiter = ','
    if os.path.exists(conf_file):
        proc = CodeBase(conf_file, delimiter)
        sec_dict = proc.get_conf('GENERAL')
        path_dict =  proc.get_conf('PATH')
    else:
        print "Not Found Config File.\n"
        sys.exit()

    for key in sec_dict:
        print key, ': ', sec_dict[key]
        print type(sec_dict[key][0])


    print "_"*50
    opt = proc.get_all_conf()
    for key in opt:
        print key, ': ', opt[key]
        print type(opt[key][0])



    print sec_dict
    print path_dict

if __name__ == '__main__':
    main()
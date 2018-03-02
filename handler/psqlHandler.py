import os
import logging
import psycopg2
from lib import CodeBase

class psqlHandler(CodeBase.Singleton):
    def __init__(self, be_conn = True, distinction = 'postgresql',  **kwargs):
        self.opt = kwargs
        self.conn = None
        self.curs = None
        self.distinction = distinction
        if be_conn : self.connect()

    def connect(self):
        try:
            args = ''
            for key in self.opt["database_info"][self.distinction] :
                args += "%s='%s' "%(key,self.opt["database_info"][self.distinction][key])

            logging.debug("args : " + args)
            self.conn = psycopg2.connect(args)
            self.conn.set_client_encoding('utf-8')
            self.curs = self.conn.cursor()

        except Exception as e:
            self.conn = None
            logging.error(e.message)
            logging.exception(e.message)

    def _execute(self, query, *args):
        try :

            logging.debug("Qeury : %s [ %s ] ", query, args)
            self.curs.execute(query, tuple(args))
            result = self.curs.fetchall()
            return result
        except Exception as e :
            logging.exception(e.message)
            return []

    def _execute2(self,query, *args):
        try :

            logging.debug("Qeury : %s  [ %s ]", query, args)
            self.curs.execute(query, tuple(args))
            self.conn.commit()
            return True
        except Exception as e :
            logging.exception(e.message)
            self.conn.rollback()
            return False



    def get_tables(self):
        query = "SELECT * FROM pg_tables where tableowner = '%s' and schemaname = 'portal';"%(self.opt["database_info"]["user"])
        return self._execute(query)

    def set_current_sequence(self,sequence, cnt):
        query = "select setval('portal.%s',%s);"%(sequence, cnt)
        return self._execute(query)

    def get_sequences(self, schema, table):

        query = "SELECT sequence_name, start_value FROM information_schema.sequences where sequence_schema = '%s' and sequence_name like '%s_%%_seq%%';"%(schema, table)
        return self._execute(query)
    def get_count(self, schema, table):
        # query = "SELECT count(*) FROM %s.%s ;"%(schema, table)
        query = "SELECT max(cast(id as int)) FROM %s.%s ;"%(schema, table)
        return self._execute(query)

    def get_org_code(self,code):
        query = "select name, title from public.group where title like '%%%s%%' and name like 'org%%'"%code
        return self._execute(query)[0]

    def get_category_code(self,code):
        query = "select name, title from public.group where title like '%%%s%%' and name like 'mn%%'"%code
        return self._execute(query)[0]

    def insert(self, table_name, **kwargs):
        keys = kwargs.keys()
        query = 'INSERT INTO %s ('%table_name
        query += ", ".join(keys) + ') '
        query +="VALUES ( "

        for key in keys :
            value = kwargs[key]

            if  isinstance(value, bool) :
                query += str(value) + ','

            else :

                value = "'"+ value + "'"

                if isinstance(value, unicode) : value = value.encode('utf-8')
                query += value
                query += ','
        query = query[:-1] + ')'
        return self._execute2(query)

    def select(self, table_name, **kwargs):
        keys = kwargs.keys()
        query = "select * from %s " %(table_name)
        query += "where "

        for key in keys :
            value = kwargs[key]
            if  isinstance(value, bool) :
                tmp = key + ' = ' + str(value)
                query += tmp + ' AND '
            else :
                tmp = key + " = '" + value + "'"
                query += tmp + ' AND '
        query = query[:-4]

        return self._execute(query)

    def delete(self, table_name, **kwargs):
        keys = kwargs.keys()
        query = "delete from %s " %(table_name)
        query += "where "

        for key in keys :
            value = kwargs[key]
            if  isinstance(value, bool) :
                tmp = key + ' = ' + str(value)
                query += tmp + ' AND '
            else :
                tmp = key + " = '" + value + "'"
                query += tmp
                query += ' AND '
        query = query[:-4]
        return self._execute2(query)

    def close(self):
        try :
            self.curs.close()
        except : pass
        try :
            self.conn.close()
        except : pass

    def __del__(self):
        self.close()
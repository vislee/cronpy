#!/usr/bin/env python
#-*-coding: utf-8 -*-
#
#
#

import sys
import urllib
import urllib2
import optparse
import ConfigParser
import logging
import MySQLdb
from threading import Timer
from threading import Thread
import pdb


rule_status_original   = 0
rule_status_have_set   = 1
rule_status_have_del   = 2
rule_status_have_skip  = 3

class TimerTask:
    def __init__(self, times=1, flag=False):
        self._timer= None
        self._tm = None
        self._fn = None
        self._lst = None
        self.limit = times
        self.flag = flag
        self.ts = 0

    def _do_func(self, *lst):
        if self._fn:
            self._fn(lst)
            self._do_start()

    def _do_start(self):
        self._timer = Timer(self._tm, self._do_func, self._lst)
        if self.flag or self.ts < self.limit:
            self._timer.start()
            self.ts += 1

    def start(self, tm, fn, args):
        self._tm = tm
        self._fn = fn
        if args is not None:
            self._lst = args
        self._do_start()

    def stop(self):
        try:
            self._timer.cancel()
        except Exception, e:
            raise TimerTaskException(e)



class MysqlException(Exception):
    pass

class MySQL:
    '''mysql 连接驱动类
          使用：paraMydb = MySQL(MysqlHost, MysqlUser, MysqlPwd, autoComm=True)
                paraMydb.selectDb(MysqlDb)
                n = paraMydb.execute(confSql)
                infoList = paraMydb.fetchAll()
                for infoDict in infoList:
                    print infoDict
    '''
    def __init__(self, host, user, password, port=3306, charset="utf8", autoComm = False):
        self.host=host
        self.port=port
        self.user=user
        self.password=password
        self.charset=charset
        self.autoComm = autoComm
        self._connect()

    def _connect(self):
        try:
            self.conn=MySQLdb.connect(host=self.host,port=self.port,user=self.user,passwd=self.password)
            self.conn.autocommit(self.autoComm)
            self.conn.set_character_set(self.charset)
            self.cur=self.conn.cursor()
        except MySQLdb.Error as e:
            raise MysqlException, "Mysql Error %d: %s" % (e.args[0], e.args[1])
            return 0

    def __del__(self):
        self.close()

    def ping(self):
        try:
            self.conn.ping()
            return 1
        except:
            try:
                self._connect()
                return 1
            except:
                return 0


    def selectDb(self,db):
        try:
            self.conn.select_db(db)
        except MySQLdb.Error as e:
            raise MysqlException, "Mysql Error %d: %s" % (e.args[0], e.args[1])


    def execute(self, sql, parameters=None):
        try:
            n=self.cur.execute(sql, parameters)
            return n
        except MySQLdb.Error as e:
            raise MysqlException, "Mysql Error:%s\nSQL:%s" %(e,sql)


    def fetchRow(self):
        result = self.cur.fetchone()
        return result


    def fetchAll(self):
        result=self.cur.fetchall()
        desc =self.cur.description
        dtlist = []
        for inv in result:
            _dt = {}
            for i in range(0,len(inv)):
                _dt[desc[i][0]] = str(inv[i])
                # _d[desc[i][0]] = str(inv[i]).decode('utf-8')
            dtlist.append(_dt)
        return dtlist


    def rowcount(self):
        return self.cur.rowcount


    def commit(self):
        self.conn.commit()


    def rollback(self):
        self.conn.rollback()


    def close(self):
        self.cur.close()
        self.conn.close()



class Log(object):
    """
    lg = Log(logFile='', fmt='', level='')
    loger = lg.getLogger()
    loger.info('test')
    """
    def __init__(self, logFile, fmt, level=logging.INFO):
        self.logFile = logFile
        self.fmt = fmt
        self.level = level
        self.logger = None
        self.hdlr = None

    def __del__(self):
        if self.logger is not None and self.hdlr is not None:
            self.logger.removeHandler(self.hdlr)
            self.hdlr.close()


    def getLogger(self):
        if self.logger:
            return self.logger
        self.logger = logging.getLogger()
        self.hdlr = logging.FileHandler(self.logFile)
        formatter = logging.Formatter(self.fmt)
        self.hdlr.setFormatter(formatter)
        self.logger.addHandler(self.hdlr)
        self.logger.setLevel(self.level)
        return self.logger



def test(log, msdb, urls):
    log.debug("test ... ")

    sql = "SELECT id, rule FROM test_tt WHERE status = %d AND status_time < now() - expire limit 0, 500 " %(rule_status_have_set)
    id_list = []
    if msdb.execute(sql) > 0:
        args = {}
        rules = msdb.fetchAll()

        not_expire = get_not_expire_rules(msdb)
        for r in rules:
            id = r.get('id')
            ip = r.get('rule')
            id_list.append(str(id))

            if ip in not_expire:
                log.info("test: ip:%s equte new rules id %d" %(str(ip), id(not_expire.get(ip).get('id', 0))))
                continue

            args['ip'] = ip

            for k, v in urls.iteritems():
                sc, sm = myCurl(log, v, '/del' , args)
                if int(sc) != 200 and 'ok' != str(sm):
                    log.error("ngproxy rule del: curl code: %s error:%s ngproxy:%s id:%s" %(str(sc), sm, k, str(id)))

            # id_list.append(str(id))

    if len(id_list) > 0:
        n = set_rule_status(log, msdb, rule_status_have_del, id_list)

        if len(id_list) != n:
            log.warn("ngproxy rule del: update %d <> del %d" %(n, len(id_list)))

    return


def cycle(args):
    if len(args) != 3:
        print "args error"
        sys.exit(1)

    cf = args[0]
    mdb = args[1]
    urls = args[2]

    if not isinstance(cf, ConfigParser.RawConfigParser):
        print "args[0] type error, not ConfigParser.RawConfigParser"
        sys.exit(1)

    if not isinstance(mdb, MySQL):
        print "args[1] type error, not Mysql"
        sys.exit(1)

    log_file = cf.get('log', 'file')
    log_level = cf.get('log', 'level')

    if "INFO" == log_level:
        log_level = logging.INFO
    elif "ERROR" == log_level:
        log_level = logging.ERROR
    elif "WARING" == log_level:
        log_level = logging.WARN
    else:
        log_level = logging.DEBUG

    log_fmt = r"%(asctime)s %(levelname)s %(message)s"

    log = Log(log_file, log_fmt, log_level)
    logger = log.getLogger()

    logger.debug("cycle ... ")

    mdb.ping()
    test(logger, mdb, urls)

    logger.debug("cycle done")



USAGE = ''' %s [OPTIONS] -- deal the table test_tt.
''' % (sys.argv[0])

parser = optparse.OptionParser(usage=USAGE, version='ngproxy_rule')
def usage():
    parser.add_option('-c', '--config', action='store', dest='conf',
                      help=r'the config file')

def main():
    usage()
    options, _ = parser.parse_args()
    conf = options.conf
    if conf is None or len(conf) == 0:
        print USAGE
        print "-c:Please enter the correct conf file\n"
        sys.exit(1)


    cf = ConfigParser.RawConfigParser()
    cf.read(conf)

    mysql_host = cf.get('mysql', 'host')
    mysql_port = cf.get('mysql', 'port')
    mysql_user = cf.get('mysql', 'user')
    mysql_pass = cf.get('mysql', 'pass')
    mysql_db   = cf.get('mysql', 'db')

    ngproxy_url = dict(cf.items('ngproxy'))

    cycle_sleep = 3
    if cf.has_option('cycle', 'sleep'):
        cycle_sleep = cf.get('cycle', 'sleep')



    mydb = MySQL(mysql_host, mysql_user, mysql_pass, int(mysql_port), autoComm=True)
    mydb.selectDb(mysql_db)

    args_list = [cf, mydb, ngproxy_url]

    tt = TimerTask(1, True)
    tt.start(int(cycle_sleep), cycle, args_list)

    # cycle(args_list)

if __name__ == '__main__':
    main()


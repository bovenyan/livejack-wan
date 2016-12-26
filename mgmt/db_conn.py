#!/usr/bin/env
import MySQLdb
import ConfigParser
import time

time_f = '%Y-%m-%d %H:%M:%S'

class db_api(object):
    def __init__(self, conf_path):
        """
        parse configuration
        """
        config = ConfigParser.ConfigParser()
        config.read(conf_path)
        self.hostip = config.get('dbconfig', 'hostip')
        self.user = config.get('dbconfig', 'dbuser')
        self.passwd = config.get('dbconfig', 'passwd')
        self.dbname = config.get('dbconfig', 'dbname')
        self.port = config.get('dbconfig', 'port')
        self.charset = config.get('dbconfig', 'charset')
        self.lastUID = 0

    def conn(self):
        """
        connecting to the database
        """
        if self.conn != None:
            return self.conn
        try:
            conn = MySQLdb.connect(host=self.hostip, user=self.user,
                                   passwd=self.passwd, db=self.dbname,
                                   port=int(self.port), charset=self.charset)
            conn.ping(True)
            self.conn = conn
            return conn
        except MySQLdb.Error, e:
            error_msg = 'Error {}: {}'.format(e.args[0], e.args[1])
            print error_msg

    def close(self):
        self.conn().close()

    def add_session(self, role, ip, port, channel):
        try:
            conn = self.conn()
            cur = conn.cursor()
            cur.execute("INSERT INTO session(uid, role, ip, port, channel, stop_time, status, dep_session)"
                        "VALUES ({}, {}, {}, {}, {}, {}, {}, {})"
                        .format(self.lastUID, role, ip, port, channel, time.time(), 1, 1 if role == 1 else 0))
            if role == 2:
                cur.execute("UPDATE session SET dep_session = dep_session + 1 WHERE channel = {} AND role = 1".format(channel))
            conn.commit()
            self.lastUID += 1
            return self.lastUID - 1
        except Exception, e:
            print str(e)
            return False

    def expire_session(self):
        try:
            conn = self.conn()
            cur = conn.cursor()
            currentTime = time.time()
            cur.execute("UPDATE session SET status = 0 WHERE stop_time < {}".format(currentTime))
            conn.commit()
            cur.execute("SELECT uid FROM session WHERE status = 0")
            expiredUIDs = []
            row = cur.fetchone()
            while row != None:
                expiredUIDs.append(row[0])
            cur.execute("DELETE FROM session WHERE status = 0")
            conn.commit()
            return expiredUIDs
        except Exception, e:
            print str(e)
            return False

    def stop_streamer_session(self, uid):
        try:
            conn = self.conn()
            cur = conn.cursor()
            cur.execute("SELECT channel FROM session WHERE uid = {}".format(uid))
            row = cur.fetchone()
            if row is not None:
                channel = row[0]
            else:
                return False
            cur.execute("DELETE FROM session WHERE uid = {}".format(uid))
            cur.execute("SELECT uid FROM session WHERE channel = {} AND uid != {}".format(channel, uid))
            removedUIDs = []
            row = cur.fetchone()
            while row != None:
                removedUIDs.append(row[0])
            cur.execute("DELETE FROM session WHERE channel = {}".format(channel))
            conn.commit()
            return removedUIDs
        except Exception, e:
            print str(e)
            return False

    def insert_dns(self, request_url, remote_ip, response_ip):
        try:
            conn = self.conn()
            cur = conn.cursor()
            cur.execute("INSERT INTO dns(request_url, remote_ip, response_ip) "
                        "VALUES ({}, {}, {})"
                        "ON DUPLICATE KEY UPDATE response_ip = {}".format(request_url, remote_ip, response_ip, response_ip))
            conn.commit()
        except Exception, e:
            print str(e)
            return False

    def search_dns(self, request_url, remote_ip):
        try:
            conn = self.conn()
            cur = conn.cursor()
            cur.execute("SELECT response_ip FROM session WHERE request_url = {} and remote_ip = {}".format(request_url, remote_ip))
            row = cur.fetchone()
            if row is not None:
                return row[0]
            else:
                return False
        except Exception, e:
            print str(e)
            return False
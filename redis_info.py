#!/usr/bin/env python
# -*- coding:utf-8 -*-
# __author__ = '40kuai'
# 2018/8/13

"""
1. redis 慢查询显示
"""

import redis
import time


class RedisInfo(object):
    def __init__(self, host='localhost', port=6379, db=0, password=None, **kwargs):
        rds_pool = redis.ConnectionPool(host=host, port=port, db=db, password=password,**kwargs)
        self.rds_obj = redis.Redis(connection_pool=rds_pool)

    def slow_log(self, sort_type='duration'):
        """获取所有慢查询，根据sort_type排序后返回数据"""
        if sort_type not in ['duration', 'start_time']:
            raise KeyError('sore_type error!')
        slowlog_list = self.rds_obj.slowlog_get(self.rds_obj.slowlog_len())
        return sorted(slowlog_list, key=lambda x: x[sort_type], reverse=True)

    def inactive_key(self):
        obj = self.rds_obj
        pass

    def big_key(self):
        """查找big key"""
        pipe_obj = self.rds_obj.pipeline()
        type_func = {
            "string": pipe_obj.strlen,
            "hash": pipe_obj.hlen,
            "list": pipe_obj.llen,
            "set": pipe_obj.scard,
            "zset": pipe_obj.zcard
        }
        cursor = '0'
        while cursor != 0:
            cursor, data = self.rds_obj.scan(cursor=cursor, count=20000)
            for item in data:
                pipe_obj.type(item)
            tempA = pipe_obj.execute()
            for index, data_type in enumerate(tempA):
                func = type_func[data_type]
                func(data[index])
            tempB = pipe_obj.execute()
            for index, size in enumerate(tempB):
                if size > 1024:
                    print 'key:%s\t size:%s' % (data[index], size)


if __name__ == '__main__':
    obj = RedisInfo(host='192.168.184.131', password='123456')
    print time.asctime()
    obj.big_key()
    print time.asctime()
    obj.slow_log()
    print time.asctime()
    # for i in obj.slow_log(): print i

import redis
import time
import gevent
import os

class RedisReadWriteLock(object):
    def __init__(self, key):
        self._conn = redis.Redis(host='localhost', port=6379, db=0)
        self._key = key + ':rwlock'
        self._write_lock = key + ':write'
        self._read_lock = key + ':read'
        self._watchdog = None
        self._pid = str(os.getpid())

    def _keep_alive(self, lock, expire):
        while self._conn.hexists(lock, self._pid):
            self._conn.expire(lock, expire)
            gevent.sleep(expire / 2)

    def acquire_read(self, timeout=5, expire=10):
        lua = """
        if redis.call('exists', KEYS[2]) == 1 and redis.call('hexists', KEYS[2], ARGV[2]) == 0 then
            return 0
        end
        local counter = redis.call('hincrby', KEYS[1], ARGV[2], 1)
        if counter == 1 then
            redis.call('expire', KEYS[1], ARGV[1])
        end
        return 1
        """
        acquire_read = self._conn.register_script(lua)
        end_time = time.time() + timeout
        while time.time() < end_time:
            if acquire_read(keys=[self._read_lock, self._write_lock], args=[expire, self._pid]):
                self._watchdog = gevent.spawn(self._keep_alive, self._read_lock, expire)
                return True
            gevent.sleep(0.01)  # avoid busy waiting
        return False  # timeout

    def release_read(self):
        lua = """
        local counter = redis.call('hincrby', KEYS[1], ARGV[1], -1)
        if counter <= 0 then
            redis.call('hdel', KEYS[1], ARGV[1])
        end
        """
        release_read = self._conn.register_script(lua)
        release_read(keys=[self._read_lock], args=[self._pid])
        if self._watchdog:
            self._watchdog.kill()

    def acquire_write(self, timeout=5, expire=10):
        lua = """
        if redis.call('exists', KEYS[1]) == 1 or (redis.call('exists', KEYS[2]) == 1 and redis.call('hexists', KEYS[2], ARGV[2]) == 0) then
            return 0
        end
        redis.call('hset', KEYS[2], ARGV[2], 1)
        redis.call('expire', KEYS[2], ARGV[1])
        return 1
        """
        acquire_write = self._conn.register_script(lua)
        end_time = time.time() + timeout
        while time.time() < end_time:
            if acquire_write(keys=[self._read_lock, self._write_lock], args=[expire, self._pid]):
                self._watchdog = gevent.spawn(self._keep_alive, self._write_lock, expire)
                return True
            gevent.sleep(0.01)  # avoid busy waiting
        return False  # timeout

    def release_write(self):
        self._conn.hdel(self._write_lock, self._pid)
        if self._watchdog:
            self._watchdog.kill()

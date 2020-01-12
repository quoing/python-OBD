# -*- coding: utf-8 -*-

########################################################################
# asynchronous OBD readings using redis (to store values and keep list 
# of watched commands) and RQ library to call callback functions
# 
# Note: External RQ worker needs to be started.
#

import time
import threading
import logging
from .OBDResponse import OBDResponse
from .obd import OBD
import obd
from redis import Redis
from redis.exceptions import LockError, LockNotOwnedError
#from rq import Queue
import json
logger = logging.getLogger(__name__)

def OBDResponse_to_json(data):
    out = { 
            "command": {
                        "name": data.command.name,
                        "mode": data.command.mode,
                        "pid": data.command.pid,
                        },
            "value": {
                       "magnitude": data.value.magnitude,
                       "unit": str(data.value.units),
                     },
            "time": data.time,
          }
    return json.dumps(out)

def OBDCommand_to_json(data):
    out = {
                        "name": data.name,
                        "desc": data.desc,
                        "mode": data.mode,
                        "pid": data.pid,
          }
    #return json.dumps(out)
    return out

class RedisOBDCommand():
    def __init__(self, *args, **kwargs):
        super(RedisOBDCommand, self).__init__(*args, **kwargs)
    def to_redis(self):
        # convert object to redis compatible field
        pass
    def from_redis(self, data):
        pass


class RedisOBD(OBD):
    def __init__(self, portstr=None, baudrate=None, protocol=None, fast=True,
                 timeout=0.1, check_voltage=True, start_low_power=False,
                 delay_cmds=0.25, connect=True):
        super(RedisOBD, self).__init__(portstr, baudrate, protocol, fast,
                                    timeout, check_voltage, start_low_power, connect)
        # initialize redis connection
        #self.redis = Redis(host='localhost', port=6379, db=0)
        self.redis = Redis()

        self.__thread = None
        self.__running = False
        self.__delay_cmds = delay_cmds

    @property
    def running(self):
        return self.__running

    def start(self):
        """ Starts the async update loop """
        if not self.is_connected():
            logger.info("Async thread not started because no connection was made")
            return

        out = {}
        for cmd in self.supported_commands:
            mode = cmd.mode
            pid = cmd.pid
            if not "Mode_"+str(mode) in out:
                out["Mode_"+str(mode)] = {}
            out["Mode_"+str(mode)][pid] = OBDCommand_to_json(cmd)
        self.redis.set('supported_commands', json.dumps(out))

        if self.__thread is None:
            logger.info("Starting async thread")
            self.__running = True
            self.__thread = threading.Thread(target=self.run)
            self.__thread.daemon = True
            self.__thread.start()

    def stop(self):
        """ Stops the async update loop """
        if self.__thread is not None:
            logger.info("Stopping async thread...")
            self.__running = False
            self.__thread.join()
            self.__thread = None
            logger.info("Async thread stopped")

    def close(self):
        """ Closes the connection """
        self.stop()
        super(RedisOBD, self).close()

    def watch(self, c, callback=None, force=False):
        """
            Subscribes the given command for continuous updating. Once subscribed,
            query() will return that command's latest value. Optional callbacks can
            be given, which will be fired upon every new value.
        """
        pid = c.pid
        mode = c.mode
        try:
            with self.redis.lock('watch_updating', blocking_timeout=1) as lock:
                # code you want executed only after the lock has been acquired
                if not force and not self.test_cmd(c):
                    # self.test_cmd() will print warnings
                   return

                # new command being watched, store the command
                logger.info("Watching command PID: %s" % str(pid))
                self.redis.sadd('watch:'+str(mode)+':'+str(pid), 'local')

        except LockError:
            # the lock wasn't acquired
            logger.warning("Can't aquire lock in watch()")

    def unwatch(self, c, callback=None):
        pid = c.pid
        mode = c.mode
        try:
            with self.redis.lock('watch_updating', blocking_timeout=1) as lock:
                self.redis.srem('watch:'+str(mode)+':'+str(pid), 'local')
                pass
        except LockError:
            # the lock wasn't acquired
            logger.warning("Can't aquire lock in unwatch()")

    def unwatch_all(self):
         try:
            with self.redis.lock('watch_updating', blocking_timeout=1) as lock:
                watches = self.redis.keys('watch:*')
                for key in watches:
                    self.redis.srem(key, "local")
         except LockError:
            # the lock wasn't acquired
            logger.warning("Can't aquire lock in unwatch()")

    def query(self, c, force=False):
        pid = c.pid
        mode = c.mode
        return self.redis.get('value:'+str(mode)+':'+str(pid))

    def run(self):
        """ Daemon thread """

        # loop until the stop signal is received
        while self.__running:
            try:
                with self.redis.lock('watch_updating', blocking_timeout=5) as lock:
                    pids = self.redis.keys('watch:*')
                    pass
            except LockError:
                # the lock wasn't acquired
                logger.warning("Can't aquire lock in unwatch()")


            if len(pids) > 0:
                t = time.perf_counter()
                # loop over the requested commands, send, and collect the response
                for watch_pid in pids:
                    watch = watch_pid.split(b':')
                    logger.info('watch: {}'.format(watch))
                    mode = int(watch[1])
                    pid = int(watch[2])
                    logger.info("mode: {}, pid {}".format(mode, pid))
                    if not self.is_connected():
                        logger.info("Async thread terminated because device disconnected")
                        self.__running = False
                        self.__thread = None
                        return

                    # force, since commands are checked for support in watch()
                    r = super(RedisOBD, self).query(obd.commands[mode][pid], force=True)

                    # store the response
                    response = OBDResponse_to_json(r)
                    self.redis.set('value:'+str(mode)+':'+str(pid), response)
                    self.redis.publish('value:'+str(mode)+':'+str(pid), response)

                    # fire the callbacks, if there are any
                    #for callback in self.__callbacks[c]:
                     #   callback(r)
                self.redis.publish('collect_time', time.perf_counter() - t)
                time.sleep(self.__delay_cmds)

            else:
                time.sleep(0.25)  # idle


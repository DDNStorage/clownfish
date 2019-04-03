# Copyright (c) 2018 DataDirect Networks, Inc.
# All Rights Reserved.

"""
rwlock library for python
"""
import threading
import time
import os

from pylcommon import clog

#
# _RWLOCK_SRC_FILE is used when walking the stack to check when we've got the
# first caller stack frame.
#
if __file__[-4:].lower() in ['.pyc', '.pyo']:
    _RWLOCK_SRC_FILE = __file__[:-4] + '.py'
else:
    _RWLOCK_SRC_FILE = __file__
_RWLOCK_SRC_FILE = os.path.normcase(_RWLOCK_SRC_FILE)


class RWLockHandle(object):
    """
    Each lock acquirement will create a object of this type
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, lock, is_read):
        self.rwh_is_read = is_read
        self.rwh_lock = lock
        self.rwh_dequeued = False
        self.rwh_filename, self.rwh_lineno, self.rwh_func = clog.find_caller(_RWLOCK_SRC_FILE)
        self.rwh_filename = os.path.basename(self.rwh_filename)

    def rwh_release(self):
        """
        Release the lock
        """
        lock = self.rwh_lock
        lock.rwl_condition.acquire()
        if self.rwh_is_read:
            lock.rwl_read_handles.remove(self)
        else:
            assert lock.rwl_write_handle == self
            lock.rwl_write_handle = None

        while len(lock.rwl_waiting_handles) > 0:
            handle = lock.rwl_waiting_handles[0]
            if handle.rwh_is_read:
                if lock.rwl_write_handle is not None:
                    break
                lock.rwl_read_handles.append(handle)
            else:
                if lock.rwl_write_handle is not None:
                    break
                if len(lock.rwl_read_handles) > 0:
                    break
                lock.rwl_write_handle = handle
            handle.rwh_dequeued = True
            lock.rwl_waiting_handles.pop(0)

        lock.rwl_condition.notifyAll()
        lock.rwl_condition.release()


class RWLock(object):
    """Synchronization object used in a solution of so-called second
    readers-writers problem. In this problem, many readers can simultaneously
    access a share, and a writer has an exclusive access to this share.
    Additionally, the following constraints should be met:
    1) no reader should be kept waiting if the share is currently opened for
        reading unless a writer is also waiting for the share,
    2) no writer should be kept waiting for the share longer than absolutely
        necessary.
    """

    def __init__(self, wait_timeout=10):
        self.rwl_read_handles = []
        self.rwl_write_handle = None
        self.rwl_waiting_handles = []
        self.rwl_condition = threading.Condition()
        self.rwl_wait_timeout = wait_timeout

    def rwl_dump(self, log, waiting_seconds):
        """
        Dump the status of this lock to error log
        """
        output = "acquiring lock is slow (%ss):\n" % waiting_seconds
        if self.rwl_write_handle is None:
            output += "write handle: None\n"
        else:
            output += ("write handle: %s:%s:%s\n" %
                       (self.rwl_write_handle.rwh_filename,
                        self.rwl_write_handle.rwh_lineno,
                        self.rwl_write_handle.rwh_func))

        for handle in self.rwl_read_handles:
            output += ("read handle: %s:%s:%s\n" %
                       (handle.rwh_filename,
                        handle.rwh_lineno,
                        handle.rwh_func))
        for waiting in self.rwl_waiting_handles:
            if waiting.rwh_is_read:
                read_write = "read"
            else:
                read_write = "write"
            output += ("waiting %s handle: %s:%s:%s\n" %
                       (read_write,
                        waiting.rwh_filename,
                        waiting.rwh_lineno,
                        waiting.rwh_func))
        log.cl_warning(output)

    def rwl_acquire(self, log, is_read, warning_time):
        """
        acquire read lock
        """
        if log.cl_abort:
            return None

        time_start = time.time()
        handle = RWLockHandle(self, is_read)
        self.rwl_condition.acquire()
        if ((self.rwl_write_handle is not None) or
                (len(self.rwl_waiting_handles) > 0) or
                ((not is_read) and len(self.rwl_read_handles) > 0)):
            self.rwl_waiting_handles.append(handle)
            while not handle.rwh_dequeued:
                if log.cl_abort:
                    self.rwl_waiting_handles.remove(handle)
                    self.rwl_condition.release()
                    return None
                time_now = time.time()
                if time_now > time_start + warning_time:
                    time_diff = time_now - time_start
                    self.rwl_dump(log, time_diff)
                self.rwl_condition.wait(self.rwl_wait_timeout)
        elif is_read:
            self.rwl_read_handles.append(handle)
        else:
            self.rwl_write_handle = handle
        self.rwl_condition.release()
        return handle

    def rwl_reader_acquire(self, log, warning_time=60):
        """
        acquire read lock
        """
        return self.rwl_acquire(log, True, warning_time)

    def rwl_writer_acquire(self, log, warning_time=60):
        """
        acquire read lock
        """
        return self.rwl_acquire(log, False, warning_time)

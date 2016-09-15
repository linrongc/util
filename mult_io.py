#! /usr/bin/env python
# -*- coding: utf-8 -*- 
"""
@time = 9/15/2016 10:44 AM
@author = Rongcheng
"""
import time, Queue, threading

class MultIO(object):

    def __init__(self, function, max_thread=5, queue_size=100, in_order=False):
        """
        :param function: target function
        :param max_thread: thread number
        :param queue_size: largest size of queue
        :param in_order: return the result in order or not
        """

        self._function = function
        self._thread_pool = [threading.Thread(target=self._work) for i in range(max_thread)]
        self._par_queue = Queue.Queue()
        self._queue_size = queue_size
        self._close_flag = False
        self._return_queue = Queue.Queue()
        self._in_order = in_order
        self._return_count = 0
        self._count = 0
        self._par_gtr = None

    def __iter__(self):
        return self

    def _work(self):
        while not self._close_flag or not self._par_queue.empty():
            try:
                if self._in_order:
                    count, par = self._par_queue.get(block=False)
                    result = self._function(par)
                    while self._return_count != count:
                        time.sleep(1e-4)
                    self._return_queue.put(result)
                    self._return_count += 1
                else:
                    par = self._par_queue.get(block=False)
                    result = self._function(par)
                    self._return_queue.put(result)
            except Queue.Empty:
                time.sleep(0.001)

    def dump(self, data, path):
        """
        asynchronized version for dump data
        :param data: data to dump
        :param path: target path
        :return: None
        """
        while self._par_queue.qsize() > self._queue_size:
            time.sleep(0.001)
        if self._in_order:
            self._par_queue.put([self._count, [data, path]])
            self._count += 1
        else:
            self._par_queue.put([data, path])

    def open(self, par_gtr):
        """
        :param par_gtr: iterable object
        :return: None
        """
        self._par_gtr = iter(par_gtr)
        self._close_flag = False
        while not self._close_flag and self._par_queue.qsize() < self._queue_size:
            self._insert_next_par()
        self._start()

    def _insert_next_par(self):
        try:
            par = self._par_gtr.next()
        except StopIteration:
            self._close_flag = True
            return
        if self._in_order:
            self._par_queue.put([self._count, par])
            self._count += 1
        else:
            self._par_queue.put(par)

    def _start(self):
        for thread in self._thread_pool:
            thread.start()

    def close(self):
        """
        close the threads, to use when no further dump jobs
        """
        self._close_flag = True
        for thread in self._thread_pool:
            thread.join()

    def next(self):
        self._insert_next_par()
        while True:
            if not self._return_queue.empty():
                return self._return_queue.get()
            else:
                if self._close_flag and self._par_queue.empty():
                    for thread in self._thread_pool:
                        thread.join()
                    if self._return_queue.empty():
                        raise StopIteration
                else:
                    time.sleep(1e-4)

if __name__ == "__main__":
    from settings import Settings
    import os
    import cv2

    file_list = os.listdir(Settings.train_folder)
    def read_image(folder, name):
        return cv2.imread(os.path.join(folder, name))
    stream = MultIO(lambda x: read_image(Settings.train_folder, x), in_order=True)
    stream.open(file_list)
    start = time.time()
    count = 0
    for img in stream:
        count += 1
    end = time.time()
    print end - start

    start = time.time()
    count = 0
    for name in file_list:
        read_image(Settings.train_folder, name)
        count += 1
    end = time.time()
    print end - start



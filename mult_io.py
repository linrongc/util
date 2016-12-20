#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
@time = 9/15/2016 10:44 AM
@author = Rongcheng
"""
import Queue
import sys
import threading
import time


class MultIO(object):

    def __init__(self, function, max_thread=5, queue_size=100, in_order=False, name=""):
        """
        :param function: target function
        :param max_thread: thread number
        :param queue_size: largest size of queue
        :param in_order: return the result in order or not
        """
        self.name = name  # use for output
        self._max_thread = max_thread
        self._function = function
        self._thread_pool = [threading.Thread(target=self._work) for i in range(max_thread)]
        self._par_queue = Queue.Queue()
        self._queue_size = queue_size
        self._close_flag = False
        self._return_queue = Queue.Queue()
        self._in_order = in_order
        self._return_count, self._par_count = 0, 0
        self._return_lock, self._par_lock = threading.Lock(), threading.Lock()
        self._par_gtr = None
        self._start()

    def __iter__(self):
        return self

    def _increase_return_count(self):
        self._return_lock.acquire()
        try:
            self._return_count += 1
            if self._return_count % 1000 == 0:
                print self.name + " complete item: ", self._return_count
                sys.stdout.flush()
        except:
            raise RuntimeError("Synchronization error!")
        finally:
            self._return_lock.release()

    def _work(self):
        while not self._close_flag or not self._par_queue.empty():
            try:
                count, par = self._par_queue.get(block=False)
                result = self._function(par)
                if result is not None:
                    if self._in_order:
                        while self._return_count != count:
                            time.sleep(1e-4)
                    self._return_queue.put(result)
                self._increase_return_count()
            except Queue.Empty:
                time.sleep(0.0001)

    def dump(self, data, path):
        """
        asynchronized version for dump data
        :param data: data to dump
        :param path: target path
        :return: None
        """
        while self._par_queue.qsize() > self._queue_size:
            time.sleep(0.001)
        self._put_par_and_increase([data, path])

    def _put_par_and_increase(self, par):
        self._par_lock.acquire()
        try:
            self._par_queue.put([self._par_count, par])
            self._par_count += 1
        except:
            raise RuntimeError("Synchronization error!")
        finally:
            self._par_lock.release()

    def _refresh(self):
        dead = 0
        for thread in self._thread_pool:
            if not thread.isAlive():
                dead += 1
        if dead == 0:
            return
        elif dead == self._max_thread:
            self._thread_pool = [threading.Thread(target=self._work) for i in range(self._max_thread)]
            self._start()
        else:
            raise Exception("previous threads not complete!")

    def open(self, par_gtr):
        """
        :param par_gtr: iterable object
        :return: None
        """
        self._par_gtr = iter(par_gtr)
        self._close_flag = False
        self._par_count = 0
        self._return_count = 0
        count = 0
        self._refresh()
        while not self._close_flag and count < self._queue_size:
            self._insert_next_par()
            count += 1

    def _insert_next_par(self):
        try:
            par = self._par_gtr.next()
        except StopIteration:
            self._close_flag = True
            return
        self._put_par_and_increase(par)

    def _start(self):
        for thread in self._thread_pool:
            thread.start()

    def close(self):
        """
        close the threads, to use when no further dump jobs or stop reading jobs
        """
        self._close_flag = True
        for thread in self._thread_pool:
            thread.join()
        self._par_queue.queue.clear()

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
    import skimage.io

    test_folder = "../data/test_output/"
    file_list = os.listdir(Settings.test_folder)

    def read_image(folder, name):
        return skimage.io.imread(os.path.join(folder, name))

    def dump_image(folder, par):
        data, path = par
        skimage.io.imsave(os.path.join(folder, path), data)

    stream = MultIO(lambda x: read_image(Settings.test_folder, x), in_order=False, name="input stream")
    stream.open(file_list)
    out_stream = MultIO(lambda x: dump_image(test_folder, x), max_thread=5, queue_size=100, in_order=False, name="output stream")
    start = time.time()
    count = 0
    for img in stream:
        out_stream.dump(img, file_list[count])
        count += 1
    end = time.time()
    print end - start
    out_stream.close()
    start = time.time()
    count = 0
    for name in file_list:
        image = read_image(Settings.test_folder, name)
        dump_image(test_folder, [image, name])
        count += 1
    end = time.time()
    print end - start



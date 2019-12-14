#coding:utf-8
import time
from multiprocessing.managers import BaseManager

from HTMLDownloader import HTMLDownloader
from HTMLParser import HTMLParser

class Worker(object):
    def __init__(self):
        # Initialize the worker node
        # Using BaseManager to register Queue's method and name
        BaseManager.register('get_task_queue')
        BaseManager.register('get_result_queue')
        # Connect to the server (master) node
        server_addr = '127.0.0.1'
        print('Connect to server %s...' % server_addr)
        # verify the port and authkey and connect through internet
        self.m = BaseManager(address=(server_addr, 8001), authkey='wiki'.encode('utf-8'))
        self.m.connect()
        # Get the queue object
        self.task = self.m.get_task_queue()
        self.result = self.m.get_result_queue()
        # Initialize the HTMLDownloader and HTMLParser
        self.downloader = HTMLDownloader()
        self.parser = HTMLParser()
        print('init finish')

    def crawl(self):
        cnt = 0
        while(True):
            try:
                if not self.task.empty():
                    url = self.task.get()
                    if url =='end':
                        print('Master node notify worker stop crawling')
                        self.result.put({'new_urls':'end','data':'end'})
                        return
                    print('worker node is working on URL: %s'%url.encode('utf-8'))
                    content = self.downloader.download(url)
                    new_urls,data = self.parser.parser(url,content)
                    self.result.put({"new_urls":new_urls,"data":data})
                    cnt += 1
                else:
                    if cnt >= 2:
                        break

            except EOFError as e:
                print("Connection to worker node fail")
                return
            except Exception as e:
                print(e)
                print('Crawl fail!')


if __name__=="__main__":
    spider = Worker()
    spider.crawl()
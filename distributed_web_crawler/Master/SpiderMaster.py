#coding:utf-8

from multiprocessing.managers import BaseManager
import time
from multiprocessing import Process, Queue

from URLManager import URLManager
from FileSaver import FileSaver



class SpiderMaster(object):

    def start_Manager(self, url_q, result_q):
        '''
        Create a manager of workers
        :param url_q: url queue: The queue provides worker nodes new URL
        :param result_q: result queue: The queue that worker nodes return result data
        :return:
        '''
        # Register two queues online, using register method, callable parameter is bind to Queue object
        # Queue objects are exposed to the network
        BaseManager.register('get_task_queue', callable=lambda: url_q)
        BaseManager.register('get_result_queue', callable=lambda: result_q)
        # Bind to port 8001. Set validation authkey 'wiki'. Initialize the manager object
        manager = BaseManager(address=('', 8001), authkey='wiki'.encode('utf-8'))
        # return manager object
        return manager


    def url_manager_proc(self, url_q, conn_q, root_url):
        '''
        URL Manager process get new URL from conn_q and submit it to URL manager. After remove duplication, get URL and
        put it into url_queue to send to worker node
        :param url_q: url queue: The queue provides worker nodes new URL
        :param conn_q: connection queue: The queue that result solve process send new url get from data to the URL manager
        :param root_url: root url: The start point of the url to be crawled
        :return:
        '''
        url_manager = URLManager()
        url_manager.add_new_url(root_url)
        while True:
            while(url_manager.has_new_url()):
                # Get new URL from URL Manager
                new_url = url_manager.get_new_url()
                # Send the new url to worker nodes
                url_q.put(new_url)
                print('old_url=',url_manager.old_url_size())
                # When crawl 2000 URLs, we stop and save the progress
                if url_manager.old_url_size()>20:
                    # Notify the worker node stop crawling
                    url_q.put('end')
                    print('Control nodes send stop work notification')
                    # Stop manager node. Store the set state to the file
                    url_manager.save_progress('new_urls.txt', url_manager.new_urls)
                    url_manager.save_progress('old_urls.txt', url_manager.old_urls)
                    return
            # Add urls getting from result_solve_proc to the URL manager
            try:
                urls = conn_q.get()
                url_manager.add_new_urls(urls)
            except BaseException as e:
                time.sleep(0.1)



    def result_solve_proc(self,result_q,conn_q,store_q):
        '''
        Result solve process read data from result queue, get new URLs from data and add to conn_q to submit to URL
        manager process. It also get the content data and submit it to the store queue, letting store process store it.
        :param result_q: result queue: The queue that worker nodes return result data
        :param conn_q: connection queue: The queue that result solve process send new url getting from HTML
        to the URL manager
        :param store_q: store queue: The queue that result solve process send data getting from HTML to the
        file saver
        :return:
        '''
        while(True):
            try:
                if not result_q.empty():
                    #Queue.get(block=True, timeout=None)
                    content = result_q.get(True)
                    if content['new_urls'] == 'end':
                        print('Result analyzing...')
                        store_q.put('end')
                        return
                    conn_q.put(content['new_urls'])
                    store_q.put(content['data'])
                else:
                    time.sleep(0.1)
            except BaseException as e:
                time.sleep(0.1)


    def store_proc(self,store_q):
        '''
        Store process read data form store_q and using file saver to store the data into file
        :param store_q: store queue: The queue that result solve process send data getting from HTML to the
        file saver
        :return:
        '''
        output = FileSaver()
        while True:
            if not store_q.empty():
                data = store_q.get()
                if data=='end':
                    print('File saving...')
                    output.ouput_end(output.filepath)

                    return
                output.store_data(data)
            else:
                time.sleep(0.1)
        pass


if __name__=='__main__':
    # Initialize four queues
    url_q = Queue()
    result_q = Queue()
    store_q = Queue()
    conn_q = Queue()
    # Create node manager
    node = SpiderMaster()
    manager = node.start_Manager(url_q,result_q)
    # Create URL manager and result solve and file saver
    url_manager_proc = Process(target=node.url_manager_proc, args=(url_q,conn_q,'https://en.wikipedia.org/wiki/Google',))
    result_solve_proc = Process(target=node.result_solve_proc, args=(result_q,conn_q,store_q,))
    store_proc = Process(target=node.store_proc, args=(store_q,))
    # Start three progress and manager
    url_manager_proc.start()
    result_solve_proc.start()
    store_proc.start()
    manager.get_server().serve_forever()
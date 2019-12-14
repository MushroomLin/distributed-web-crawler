# coding:utf-8

import hashlib
import pickle
import re

class URLManager(object):
    def __init__(self):
        # self.r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        # Initialize new urls and old urls from file
        self.new_urls = self.load_progress('new_urls.txt')
        # for url in new_urls:
        #     self.r.sadd("new_urls", url)
        self.old_urls = self.load_progress('old_urls.txt')
        # for url in old_urls:
        #     self.r.sadd("old_urls", url)

    def has_new_url(self):
        '''
        If there is new url need to be crawled
        :return:
        '''
        return self.new_url_size() != 0

    def get_new_url(self):
        '''
        Get a new url to be crawled
        :return:
        '''
        new_url = self.new_urls.pop()
        m = hashlib.md5()
        m.update(new_url.encode('utf-8'))
        self.old_urls.add(m.hexdigest())
        return new_url

    def add_new_url(self, url):
        '''
        Add a url we get from HTML to the new_urls set
        :param url: a URL
        :return:
        '''
        if url is None:
            return
        m = hashlib.md5()
        m.update(url.encode('utf-8'))
        url_md5 = m.hexdigest()
        if url not in self.new_urls and url_md5 not in self.old_urls and re.match('.*en.wikipedia.org/wiki/.*', url):
            self.new_urls.add(url)

    def add_new_urls(self, urls):
        '''
        Add all URLs from URL list to the new_urls set
        :param urls: url list
        :return:
        '''
        if urls is None or len(urls) == 0:
            return
        for url in urls:
            self.add_new_url(url)

    def new_url_size(self):
        '''
        Get the size of new urls set
        :return:
        '''
        return len(self.new_urls)

    def old_url_size(self):
        '''
        Get the size of old urls set
        :return:
        '''
        return len(self.old_urls)

    def save_progress(self, path, data):
        '''
        Save the crawler progress to a local file
        :param path: File path
        :param data: Data to be save
        :return:
        '''
        with open(path, 'wb') as f:
            pickle.dump(data, f)

    def load_progress(self, path):
        '''
        Load the crawler progress to a local file
        :param path: File path
        :return: a python set with data URL
        '''
        print ('Loading progress from %s...' % path)
        try:
            with open(path, 'rb') as f:
                tmp = pickle.load(f)
                return tmp
        except:
            print('No progress file, create file at: %s' % path)
        return set()
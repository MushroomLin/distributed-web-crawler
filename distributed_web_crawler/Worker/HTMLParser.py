#coding:utf-8
import re
import urllib.parse
from bs4 import BeautifulSoup
# We use beautiful soap library to analyze HTML page

class HTMLParser(object):

    def parser(self, page_url, html_content):
        '''
        Analyze the HTML page and get the URL and data
        :param page_url: Download page base URL
        :param html_content: Download HTML page content
        :return: new urls and data in the HTML page
        '''
        if page_url is None or html_content is None:
            return
        soup = BeautifulSoup(html_content, 'html.parser', from_encoding='utf-8')
        new_urls = self._get_new_urls(page_url, soup)
        new_data = self._get_new_data(page_url, soup)
        return new_urls, new_data

    def _get_new_urls(self, page_url, soup):
        '''
        TODO: Rewrite this method can specify the URL rules we want to crawl
        Get the new set of urls
        :param page_url: Download page base URL
        :param soup:soup
        :return: new urls set
        '''
        new_urls = set()
        # Get the urls that satisfy the rules
        links = soup.find_all('a',href=re.compile(r'/wiki/.*'))
        for link in links:
            # get href property
            new_url = link['href']
            # connect the base url to the whole url
            new_full_url = urllib.parse.urljoin(page_url,new_url)
            new_urls.add(new_full_url)
        return new_urls

    def _get_new_data(self, page_url, soup):
        '''
        TODO: Rewrite this method can specify the data we want to crawl
        Get the data we need
        :param page_url: Download page base URL
        :param soup:
        :return: data we need
        '''
        data = {}
        data['url'] = page_url
        title = soup.find('h1',class_='firstHeading',id='firstHeading')
        print(title.get_text())
        data['title'] = title.get_text()
        summary = soup.p
        data['summary'] = summary.get_text()
        return data
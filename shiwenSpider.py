#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2016-04-10 13:52:03
# Project: weijinshiwen

from pyspider.libs.base_handler import *
import re
from bs4 import BeautifulSoup
from bs4 import NavigableString
from bs4 import Tag

class Handler(BaseHandler):
    crawl_config = {
    }

    seedUrl = 'http://so.gushiwen.org/type.aspx?c=%E9%AD%8F%E6%99%8B'

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl(self.seedUrl, callback=self.index_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        pageText = list(response.doc('.pages > span').items())[1].text()
        posts = int(re.search('[0-9]+', pageText).group())
        if posts % 10 == 0:
            pages = posts /10
        else:
            pages = posts /10 + 1
        for i in range(pages):
            crawlUrl = self.seedUrl + '&p=' + str(i + 1)
            self.crawl(crawlUrl, callback=self.list_page)

    def list_page(self, response):
        postsUrls = []
        for each in response.doc('.sons > p:nth-child(2)').items():
            if len(each('a')) is not 0:
                postsUrls.append(each('a'))
            else:
                postsUrls.append(each.prev()('a'))
        for marker in postsUrls:
            self.crawl(marker.attr.href , callback=self.detail_page)

    @config(priority=2)
    def detail_page(self, response):
        # 简单数据搜集
        originalWebContent = str(response.doc('html'))
        soup = BeautifulSoup(originalWebContent, "lxml")
        title = soup.find_all('div', attrs={'class': 'son1'})[1].find('h1').get_text()
        url = response.url
        mainContentDiv = soup.find_all('div', attrs={'class': 'son2'})[1]
        authorMarker = mainContentDiv.find_all('p')[1].find('a')
        author = authorMarker.get_text()
        originalTextMarker = mainContentDiv.find_all('p')[2]
        if authorMarker is not None:
            authorUrl = authorMarker.get('href')
        else:
            # 针对于佚名的情况
            authorUrl = ''
        originalText = ''
        for sibling in originalTextMarker.next_siblings:
            if isinstance(sibling, NavigableString):
                originalText += sibling.string.strip()
            elif isinstance(sibling, Tag):
                originalText += sibling.get_text()

        fanyiUrls, shangxiUrls = [], []
        for element in soup.find_all('div', attrs={'class': 'son5'}):
            url = element.find('a').get('href')
            if re.search('fanyi', url) is not None:
                fanyiUrls.append(url)
            elif re.search('shangxi', url) is not None:
                shangxiUrls.append(url)

        return {
            "url": url,
            "title": title,
            "author": author,
            "authorUrl": authorUrl,
            "originalText": originalText,
            "fanyiUrls": fanyiUrls,
            "shangxiUrls": shangxiUrls
        }

# /usr/bin/env python
# -*- encoding: utf-8 -*-
""" 完善古诗词抓取的数据信息脚本 """

import requests
import json
import re
from bs4 import BeautifulSoup
from bs4 import NavigableString
from bs4 import Tag
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

HOST = "192.168.10.10"
MySQL_USER = "homestead"
USER_PWD = "secret"
RESULTDB = 'resultdb'
DATADB = "gushiwen"

POSTTYPE = 1 #第一步抓取诗文,再兼容古文

Base = declarative_base()

# 定义抓取表对象:
class ResultTable(Base):
    __tablename__ = 'shiwen'

    # 表的结构:
    taskid = Column(String, primary_key=True)
    url = Column(String)
    result = Column(BLOB)
    updatetime = Column(Float)


# 定义authors表
class authors(Base):
    __tablename__ = 'authors'

    # 表的结构:
    id = Column(INT, primary_key=True)
    name = Column(String)
    birthYear = Column(INT)
    deathYear = Column(INT)
    years = Column(INT)
    briefIntro = Column(TEXT)
    dynastyId = Column(INT)
    authorImg = Column(String)
    authorUrl = Column(String)

# 定义dynasty表
class dynasty(Base):
    __tablename__ = 'dynasty'

    # 表的结构:
    id = Column(INT, primary_key=True)
    dynastyName = Column(String)
    foundYear = Column(INT)
    destructedYear = Column(INT)
    capital = Column(String)
    type = Column(INT)

# 定义authorInfo表
class authorInfo(Base):
    __tablename__ = 'authorInfo'

    # 表的结构:
    id = Column(INT, primary_key=True)
    infoUrl = Column(String)
    authorId = Column(INT)
    infoText = Column(TEXT)
    infoTopic = Column(String)

# 定义posts表
class posts(Base):
    __tablename__ = 'posts'

    # 表的结构:
    id = Column(INT, primary_key=True)
    title = Column(String)
    originText = Column(TEXT)
    authorId = Column(INT)
    albumId = Column(INT)
    createTime = Column(String)

# 定义translatePosts表
class translatePosts(Base):
    __tablename__ = 'translatePosts'

    # 表的结构:
    id = Column(INT, primary_key=True)
    postId = Column(INT)
    translateUrl = Column(String)
    translateText = Column(TEXT)
    rate = Column(INT)

# 定义translatePosts表
class appreciationPosts(Base):
    __tablename__ = 'appreciationPosts'

    # 表的结构:
    id = Column(INT, primary_key=True)
    postId = Column(INT)
    appreciationUrl = Column(String)
    appreciationText = Column(TEXT)
    rate = Column(INT)


resultDBEngine = create_engine('mysql+mysqlconnector://%s:%s@%s:3306/%s' % (MySQL_USER, USER_PWD, HOST, RESULTDB))
dataDBEngine = create_engine('mysql+mysqlconnector://%s:%s@%s:3306/%s' % (MySQL_USER, USER_PWD, HOST, DATADB))
resultDBSession = sessionmaker(bind=resultDBEngine)
dataDBSession = sessionmaker(bind=dataDBEngine)


def beginTranslate():

    print " 开始执行古诗文完善程序 "
    resultSession = resultDBSession()
    for record in resultSession.query(ResultTable).all():
        result = json.loads(record.result)
        authorUrl, shangxiUrls, originalText, title, url, author, fanyiUrls = result['authorUrl'], result['shangxiUrls'], \
                                                    result['originalText'], result['title'], result['url'], result['author'], result['fanyiUrls']
        dataSession = dataDBSession()

        authorId = compeletAuthorInfo(dataSession, authorUrl)
        # authorId不为0,则继续补充posts资料
        if authorId is not 0:
            post = posts(title = title, originText = originalText, authorId = authorId)
            dataSession.add(post)
            dataSession.commit()
            postId = post.id
            attachTranslateAndAppreciation(dataSession, postId, shangxiUrls, fanyiUrls)


def compeletAuthorInfo( dataSession, authorUrl ):
    """
        根据作者链接完成作者简介抓取,现在只从古诗词网读取,以后可以考虑百科
    """
    print authorUrl
    if re.search('author', authorUrl) is not None:
        # 先检查authorUrl存不存在
        check = dataSession.query(authors).filter(authors.authorUrl == authorUrl).first()
        if check is None:
            html = requests.get(authorUrl).content
            soup = BeautifulSoup(html, "lxml")
            authorName = soup.find_all('div', attrs={'class': 'son1'})[1].find('h1').string.strip()
            briefDiv = soup.find_all('div', attrs={'class': 'son2'})[1]
            if briefDiv.find('div') is not None:
                authorImg = briefDiv.find('div').find('img').get('src')
            else:
                authorImg = ''
            birthYear, deathYear, years, briefIntro = 0, 0, 0, ''
            if briefDiv.find('div') is not None:
                briefIntro = briefDiv.find('div').next_sibling.string.strip()
                if len(briefDiv.find('div').next_sibling.string.strip()) is 0:
                    briefIntro = briefDiv.find('p').string.strip()
            else:
                briefIntro = briefDiv.get_text().strip()

            ages = re.findall('[0-9]+', briefIntro)[:2]
            if len(ages) == 2:
                birthYear, deathYear, years = ages[0], ages[1], (int(ages[1])-int(ages[0]))
            elif len(ages) == 1:
                birthYear, deathYear, years = int(ages[0]) - 40, ages[0], 40
            elif len(ages) == 0:
                birthYear, deathYear, years = 0, 0, 0
            # 以出生日期查询作者所属的朝代
            dynastyId = dataSession.query(dynasty).filter(text(":birthYear >= foundYear  and :birthYear <= destructedYear and type = :type")).\
                 params(birthYear = birthYear, type = 1).first().id

            author = authors(name = authorName, birthYear = birthYear, deathYear = deathYear, years = years,
                             briefIntro = briefIntro, dynastyId = dynastyId, authorImg = authorImg, authorUrl = authorUrl)
            dataSession.add(author)
            dataSession.commit()
            authorId = author.id
            authorInfoList = ['http://so.gushiwen.org' + element.find('p').find('a').get('href') for element in soup.find_all('div', attrs={'class': 'son5'})]
            addAuthorInfo(dataSession, authorInfoList, authorId)
        else:
            authorId = check.id

        return authorId
    return 0


def addAuthorInfo(dataSession, authorInfoList, authorId):
    for authorInfoUrl in authorInfoList:
        html = requests.get(authorInfoUrl).content
        soup = BeautifulSoup(html, "lxml")
        infoTopic = soup.find_all('div', attrs={'class': 'son1'})[1].find('h1').string.strip()
        contentList = soup.find('div', attrs={'class': 'shangxicont'}).find_all('p')[1:-1]
        infoText = ''
        if len(contentList) is not 0:
             for contentElement in contentList:
                infoText += contentElement.get_text()
        else:
             for contentElement in soup.find('div', attrs={'class': 'shangxicont'}).find('p').next_siblings:
                if isinstance(contentElement, NavigableString):
                    infoText += contentElement
                elif isinstance(contentElement, Tag):
                    infoText += contentElement.get_text()
        authorAttach = authorInfo(infoUrl = authorInfoUrl, authorId = authorId, infoText = infoText.strip(), infoTopic = infoTopic)
        dataSession.add(authorAttach)
        dataSession.commit()

def attachTranslateAndAppreciation(dataSession, postId, shangxiUrls, fanyiUrls):
    # 首先添加译文信息
    if len(fanyiUrls) is not 0:
        for i, translateUrl in enumerate(fanyiUrls):
            html = requests.get(translateUrl).content
            soup = BeautifulSoup(html, "lxml")
            contentList = soup.find('div', attrs={'class': 'shangxicont'}).find_all('p')[1:-1]
            translateText = ''
            for contentElement in contentList:
                translateText += contentElement.get_text()
            if len(translateText) is not 0:
                translatePost = translatePosts(postId = postId, translateUrl = translateUrl, translateText = translateText, rate = i + 1)
                dataSession.add(translatePost)
                dataSession.commit()
    # 再添加赏析内容
    if len(shangxiUrls) is not 0:
        for i, appreciationUrl in enumerate(shangxiUrls):
            html = requests.get(appreciationUrl).content
            soup = BeautifulSoup(html, "lxml")
            contentList = soup.find('div', attrs={'class': 'shangxicont'}).find_all('p')[1:-1]
            appreciationText = ''
            for contentElement in contentList:
                appreciationText += contentElement.get_text()
            if len(appreciationText) is not 0:
                appreciationPost = appreciationPosts(postId = postId, appreciationUrl = appreciationUrl, appreciationText = appreciationText, rate = i + 1)
                dataSession.add(appreciationPost)
                dataSession.commit()


# entrance
if __name__ == '__main__':
    beginTranslate()



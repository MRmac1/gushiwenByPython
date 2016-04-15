# /usr/bin/env python
# -*- encoding: utf-8 -*-
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
class ResultTable(Base):
    # 表的名字:
    __tablename__ = 'shiwen'

    # 表的结构:
    taskid = Column(String, primary_key=True)
    url = Column(String)
    result = Column(BLOB)
    updatetime = Column(Float)

    def __repr__(self):
        return "<User(taskid='%s', url='%s', result='%s')>" % (
                             self.taskid, self.url, self.result)
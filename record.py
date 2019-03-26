# coding=utf-8

__author__ = 'kai_k_000'


import time
import os
import csv
import json
from enum import Enum , unique
import hashlib

import re

@unique
class insertStatus(Enum):
    SomeError = 0
    Success = 1
    Exist = 2


class recordError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class recordfile:
    def __del__(self):
        print("__del__")

    def __init__(self, filePath, header):
        if None == filePath:
            raise recordError("None == path")
        if len(header) == 0:
            raise recordError("header error")

        self.filePath = filePath
        self.header = header
        self.__checkFile(self.filePath, self.header)
        self.hashIndex_dic = self.__loadHashIndex()

    def __checkFile(self, filePath, header):
        if not os.path.isdir(os.path.dirname(filePath)):
            os.mkdir(os.path.dirname(filePath))

        if not os.path.isfile(filePath):
            with open(filePath, 'w')  as f:
                writer = csv.DictWriter(f, fieldnames=header)
                writer.writeheader()


    def __loadHashIndex(self):
        with open(self.filePath, 'r')  as file:
            itemList = csv.DictReader(file, self.header)
            dictionary = {}
            for i, item in enumerate(itemList):
                if i == 0:
                    continue
                hash = item[self.header[0]]
                dictionary[hash] = 1 #item[self.header[1]] + item[self.header[2]]
        return dictionary

    def __loadCSVWriter(self, file, header):
        return csv.DictWriter(file, fieldnames=header)


    def addARows(self, items, istest=False):
        if  0 == len(items) or type(items)!=list:
            return [insertStatus.SomeError]

        res = []
        for item in items:
            hashIndex = item[self.header[0]]
            if self.checkDuplicate(hashIndex):
                print("is Exist:", item[self.header[1]], item[self.header[2]], item[self.header[3]])
                res.append(insertStatus.Exist)
                continue

            if istest == False:
                with open(self.filePath, 'a+') as file:
                    csvWriter = self.__loadCSVWriter(file, self.header)
                    csvWriter.writerow(item)

            self.hashIndex_dic[hashIndex]  = 1 #str(item[self.header[1]]) + item[self.header[2]]
            res.append(insertStatus.Success)
        return res


    def getJsonObject(self, orgstr, timeSeed):
        # print("orgstr:",orgstr)
        jsonstr = orgstr

        if timeSeed != None:
            jsonstr = jsonstr.replace("try{%s(" % timeSeed, '')
            jsonstr = jsonstr.replace(");}catch(e){};", '')

        try:
            result = json.loads(jsonstr)
        except Exception as e:
            print (jsonstr)
            raise recordError("json error:%s" % e.message)
        return result

    def getHTMLObject(self, orgstr):
        # print("orgstr:",orgstr)
        rc = re.compile(r'<li><span class="liTime">(.*?)</span><div>(.*?)</div>.*?</li>')
        return rc.findall(orgstr)

    def checkDuplicate(self, hashIndex):
        print("hashIndex",hashIndex)
        return None != self.hashIndex_dic.get(hashIndex)

    def getMD5(self, str):
        md5Hasher = hashlib.md5()
        md5Hasher.update(str.encode('utf-8'))
        return md5Hasher.hexdigest()
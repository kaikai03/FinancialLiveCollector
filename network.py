# coding=utf-8
__author__ = 'kai_k_000'

__COOKIE__ = 'cookie.txt' # 设置保存cookie的文件，同级目录下的cookie.txt

__RETRY_TIME__ = 2
retryTime = __RETRY_TIME__


import requests
import json
import time
# import httplib
# httplib.HTTPConnection._http_vsn = 10
# httplib.HTTPConnection._http_vsn_str = 'HTTP/1.0'

class networkError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


class net:
    def __init__(self):
        self.cookie = None
        self.session = requests.Session()
        self.header = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                                "Accept-Encoding": "gzip, deflate, br",
                                "Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
                                "Connection": "keep-alive",
                                "Upgrade-Insecure-Requests": "1",
                                "Content-Type": "application/x-www-form-urlencoded",
                                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:56.0) Gecko/20100101 Firefox/56.0"
                       }


    def __getCookie(self, page):
        response = self.session.get(page, headers=self.header)
        cookiejar = response.cookies
        # 保存cookie到文件
        cookies = json.dumps(cookiejar.get_dict())
        print("cookiejar.get_dict():",cookiejar.get_dict())
        print("cookies:",cookies)
        with open(__COOKIE__,'w') as f:
            f.write(cookies)
        return cookiejar

    def loadCookie(self, page):
        try:
            with open(__COOKIE__,'r') as f:
                cookiej = json.load(f)
                self.cookie = requests.utils.cookiejar_from_dict(cookiej, cookiejar=None, overwrite=True)
        except Exception as e:
            print('loginByCookie : cookie not found,run again', e)
            self.cookie = self.__getCookie(page)

        if None == self.cookie:
            self.cookie = self.__getCookie(page)

        self.session.cookies = self.cookie

        return self.cookie

    def loginTest(self, page):
        if None == self.cookie:
            raise networkError('no cookie, loadCookie() first')

        response = self.session.get(page)
        print(response.text)
        return response.status_code

    def getResponseData(self, addr, payload=None):
        if None == self.cookie:
            raise networkError('no cookie, loadCookie() first')

        for i in range(0, 1 + __RETRY_TIME__):
            try:
                if payload is None:
                    response = self.session.get(addr)
                else:
                    response = self.session.post(addr, data=payload)

                if 200 != response.status_code:
                    print("getResponseData error:", response.status_code)
                    print(response.text)
                    if i == __RETRY_TIME__:
                        return None
                    print("retry again")
                    continue
                break
            except Exception as e:
                print("getResponseData error")
                print("retry %d time" % i)
                if i == __RETRY_TIME__:
                    print("error quit")
                    return None
                time.sleep(240)
                continue
        return response.text

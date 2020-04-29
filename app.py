from tornado.gen import coroutine
from tornado.web import Application, RequestHandler, MissingArgumentError
from tornado.ioloop import IOLoop
import asyncio
from werkzeug.security import generate_password_hash, check_password_hash

import datetime
import pandas as pd

from db import DBConnection
from instance import SeleniumInstance
from utils import get_regex, push_tele, generate_string
from accounts import *
import globals


globals.initialize()
dbconn = DBConnection()


class Ping(RequestHandler):
    @coroutine
    def get(self):
        try:
            userEmail = self.get_body_argument('email')
            globals.active_users[userEmail].ping = datetime.datetime.now()
            self.write({'message':'Pinging'})
        except MissingArgumentError:
            self.write({'message':'Missing Arguments'})


class DemoScreen(RequestHandler):
    def get(self):
        self.write({'message':'Hello World!'})


class LoginHandler(RequestHandler):
    @coroutine
    def get(self):
        try:
            userEmail = self.get_body_argument('email')
            userPassword = self.get_body_argument('password')
            userVersion = self.get_body_argument('version')
            if userEmail not in globals.active_users:
                version_is_valid = userVersion in versionAvailable
                if version_is_valid:
                    email, password = dbconn.get_user(userEmail)
                    if password == '1':
                        is_valid = userPassword == password
                    else:
                        is_valid = check_password_hash(password, userPassword)

                    if is_valid:
                        token = generate_string()
                        globals.active_users[userEmail] = SeleniumInstance(userEmail, dbconn, generate_string(6), token, datetime.datetime.now())
                        self.write({'token':token})
                    else:
                        self.write({'token':False, 'message':'Wrong Emaill/Password'})
                else:
                    self.write({'token':False, 'message':'Wrong Version. Please Re-download'})
            else:
                self.write({'token':False, 'message':'This account has already signed in'})
        except:
            self.write({'token':False, 'message':'Cannot Connect To Server'})


class ChangePassword(RequestHandler):
    @coroutine
    def put(self):
        try:
            userEmail = self.get_body_argument('email')
            userToken = self.get_body_argument('token')
            userNewPassword = self.get_body_argument('newPassword')
            userNewPassword = generate_password_hash(userNewPassword, 'sha256')
            if userToken == globals.active_users[userEmail].token:
                dbconn.upsert_user(userEmail, userNewPassword)
                self.write({'message':'Password Updated'})
                dbconn.insert_app_event((globals.active_users[userEmail].session, userEmail, datetime.datetime.now(), 'change_password', None, None), transform=False)
            else:
                self.write({'message':'Token invalid'})
        except MissingArgumentError:
            self.write({'message':'Missing Email/Password'})


class ScrapeAds(RequestHandler):
    @coroutine
    def post(self):
        try:
            userEmail = self.get_body_argument('email')
            userToken = self.get_body_argument('token')
            fb_email = self.get_body_argument('fb_email')
            fb_pass = self.get_body_argument('fb_pass')
            teleId = self.get_body_argument('teleId')
            keywords = self.get_body_argument('keywords')
            blacklistKeywords = self.get_body_argument('blacklistKeywords')

            if userToken == globals.active_users[userEmail].token:
                if userEmail not in globals.active_users:
                    self.write({'message':'Request Accepted'})
                    globals.active_users[userEmail] = SeleniumInstance(userEmail, dbconn, generate_string(6), datetime.datetime.now())
                    globals.active_users[userEmail].start('ads', fb_email, fb_pass, teleId, keywords, blacklistKeywords)

                elif globals.active_users[userEmail].runAds == False:
                    self.write({'message':'Request Accepted'})
                    globals.active_users[userEmail].start('ads', fb_email, fb_pass, teleId, keywords, blacklistKeywords)

                elif globals.active_users[userEmail].runAds == True:
                    self.write({'message':'Session Existed'})
            else:
                self.write({'message':'Wrong Token'})
        except MissingArgumentError:
            self.write({'message':'Missing Arguments'})


class ScrapeGroups(RequestHandler):
    @coroutine
    def post(self):
        global user_ping
        try:
            userEmail = self.get_body_argument('email')
            userToken = self.get_body_argument('token')
            fb_email = self.get_body_argument('fb_email')
            fb_pass = self.get_body_argument('fb_pass')
            teleId = self.get_body_argument('teleId')
            keywords = self.get_body_argument('keywords')
            blacklistKeywords = self.get_body_argument('blacklistKeywords')
            groupIdList = self.get_body_argument('groupIdList')

            if userToken == globals.active_users[userEmail].token:
                if userEmail not in globals.active_users:
                    self.write({'message':'Request Accepted'})
                    globals.active_users[userEmail] = SeleniumInstance(userEmail, dbconn, generate_string(6), datetime.datetime.now())
                    user_ping[userEmail] = datetime.datetime.now()
                    globals.active_users[userEmail].start('groups', fb_email, fb_pass, teleId, keywords, blacklistKeywords, groupIdList)

                elif globals.active_users[userEmail].runGroups == False:
                    self.write({'message':'Request Accepted'})
                    globals.active_users[userEmail].start('groups', fb_email, fb_pass, teleId, keywords, blacklistKeywords, groupIdList)

                elif globals.active_users[userEmail].runGroups == True:
                    self.write({'message':'Session Existed'})
            else:
                self.write({'message':'Wrong Token'})
        except MissingArgumentError:
            self.write({'message':'Missing Arguments'})


class StopScrape(RequestHandler):
    @coroutine
    def post(self):
        try:
            userEmail = self.get_body_argument('email')
            userToken = self.get_body_argument('token')
            stopType = self.get_body_argument('type')
            email = self.get_body_argument('fb_email')
            email2 = self.get_body_argument('fb_email2')
            for i in (email, email2):
                if i == '':
                    i = None

            if userToken == globals.active_users[userEmail].token:
                try:
                    globals.active_users[userEmail].stop(stopType, email, email2)
                    self.write({'message':'Session Stopped'})
                except:
                    self.write({'message':'No Session Found'})
                
            else:
                self.write({'message':'Wrong token'})
        except MissingArgumentError:
            self.write({'message':'Missing Arguments'})


class ExtractPosts(RequestHandler):
    @coroutine
    def get(self):
        global globals
        try:
            userEmail = self.get_body_argument('email')
            userToken = self.get_body_argument('token')
            extractType = self.get_body_argument('type')
            fromTime = self.get_body_argument('fromTime')
            toTime = self.get_body_argument('toTime')

            if userToken == globals.active_users[userEmail].token:
                data = dbconn.get_posts(userEmail, extractType, fromTime, toTime)
                if data != '[]':
                    self.write({'message': data})
                    dbconn.insert_app_event((globals.active_users[userEmail].session, userEmail, datetime.datetime.now(),
                                            f"extract_{extractType}_posts", None, None), transform=False)
                else:
                    self.write({'message':'No data'})
            else:
                self.write({'message':'Connection expired. Please sign-in again'})

        except MissingArgumentError:
            self.write({'message':'Missing Arguments'})


class CloseApp(RequestHandler):
    @coroutine
    def post(self):
        global user_ping, globals
        try:
            userEmail = self.get_body_argument('email')
            userToken = self.get_body_argument('token')
            email = self.get_body_argument('fb_email')
            email2 = self.get_body_argument('fb_email2')
            groupIdList = self.get_body_argument('group_id_list')

            if userToken == globals.active_users[userEmail].token:
                self.write({'message':''})
                globals.active_users[userEmail].stop('both', email, email2, groupIdList)
            else:
                pass

        except Exception as e:
            if type(e).__name__ == 'MissingArgumentError':
                self.write({'message':'Missing Arguments'})
            else:
                pass


def make_app(debug=False, autoreload=False):
    urls = [
        ("/login", LoginHandler),
        ('/api/scrape-ads', ScrapeAds),
        ('/api/scrape-groups', ScrapeGroups),
        ('/ping', Ping),
        ('/api/stop-scrape', StopScrape),
        ('/api/extract-posts', ExtractPosts),
        ('/change-password', ChangePassword),
        ('/close-app', CloseApp),
        ('/', DemoScreen)
    ]
    return Application(urls, debug=debug, autoreload=autoreload)


if __name__ == '__main__':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    app = make_app()
    app.listen(8080)
    IOLoop.instance().start()
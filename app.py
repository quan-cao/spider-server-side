import sys
sys.path.append(sys.path[0]+'\\venv\\Lib\\site-packages') # For Task Schedule

from tornado.web import Application, RequestHandler
from tornado.ioloop import IOLoop
from tornado.gen import coroutine
import asyncio
from werkzeug.security import generate_password_hash, check_password_hash

import datetime, threading, time
import pandas as pd

from db import DBConnection
from instance import SeleniumInstance
from utils import get_regex, push_tele, generate_string
from accounts import versionAvailable
import globals


globals.initialize()
dbconn = DBConnection()

def clear():
    global globals
    while True:
        time.sleep(60)
        for user in globals.active_users:
            if datetime.datetime.now() - globals.active_users[user].ping > datetime.timedelta(seconds=180):
                dbconn.insert_app_event((globals.active_users[user].session, globals.active_users[user].userEmail, datetime.datetime.now(), 'session_timeout', None, None), transform=False)
                del globals.active_users[user]
        

class Ping(RequestHandler):
    @coroutine
    def get(self):
        try:
            userEmail = self.get_body_argument('email')
            globals.active_users[userEmail].ping = datetime.datetime.now()
            self.write({'message':'Pinging'})
        except:
            pass


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
                if userVersion in versionAvailable:
                    _, password = dbconn.get_user(userEmail)
                    if password == '1':
                        isValid = userPassword == password
                    else:
                        isValid = check_password_hash(password, userPassword)

                    if isValid:
                        token = generate_string()
                        globals.active_users[userEmail] = SeleniumInstance(userEmail, dbconn, token, generate_string(6), datetime.datetime.now())
                        self.write({'token':token})
                        dbconn.insert_app_event((globals.active_users[userEmail].session, userEmail, datetime.datetime.now(), 'logged_in', None, None), transform=False)
                    else:
                        self.set_status(401)
                        self.write({'token':False, 'message':'Wrong Emaill/Password'})
                        dbconn.insert_app_event(('000000', userEmail, datetime.datetime.now(), 'wrong_password', None, None), transform=False)
                else:
                    self.set_status(403)
                    self.write({'token':False, 'message':'Wrong version. Please re-download'})
                    dbconn.insert_app_event(('000000', userEmail, datetime.datetime.now(), 'wrong_version', None, None), transform=False)
            else:
                self.set_status(409)
                self.write({'token':False, 'message':'This account has already signed in'})
                dbconn.insert_app_event(('000000', userEmail, datetime.datetime.now(), 'existed_session', None, None), transform=False)
        except:
            self.set_status(400)
            self.write({'token':False, 'message':'Cannot Connect To Server'})
            dbconn.insert_app_event(('000000', userEmail, datetime.datetime.now(), 'login_error', None, None), transform=False)


class ChangePassword(RequestHandler):
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
                self.set_status(403)
                self.write({'message':'Token invalid'})
                dbconn.insert_app_event(('000000', userEmail, datetime.datetime.now(), 'token_error', None, None), transform=False)
        except:
            self.set_status(400)
            self.write({ 'message':'Bad Request'})
            dbconn.insert_app_event(('000000', userEmail, datetime.datetime.now(), 'change_password_error', None, None), transform=False)


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
                    self.set_status(202)
                    self.write({'message':'Request Accepted'})
                    globals.active_users[userEmail] = SeleniumInstance(userEmail, dbconn, generate_string(6), generate_string(), datetime.datetime.now())
                    globals.active_users[userEmail].start('ads', fb_email, fb_pass, teleId, keywords, blacklistKeywords)

                elif globals.active_users[userEmail].runAds == False:
                    self.set_status(202)
                    self.write({'message':'Request Accepted'})
                    globals.active_users[userEmail].start('ads', fb_email, fb_pass, teleId, keywords, blacklistKeywords)

                elif globals.active_users[userEmail].runAds == True:
                    self.set_status(409)
                    self.write({'message':'Session Existed'})
                    dbconn.insert_app_event((globals.active_users[userEmail].session, userEmail, datetime.datetime.now(), 'existed_ads_instance', None, None), transform=False)
            else:
                self.set_status(403)
                self.write({'message':'Wrong Token'})
                dbconn.insert_app_event(('000000', userEmail, datetime.datetime.now(), 'token_error', None, None), transform=False)
        except:
            self.set_status(400)
            self.write({'message':'Bad Request'})
            dbconn.insert_app_event(('000000', userEmail, datetime.datetime.now(), 'ads_request_error', None, None), transform=False)


class ScrapeGroups(RequestHandler):
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
            groupIdList = self.get_body_argument('groupIdList')

            if userToken == globals.active_users[userEmail].token:
                if userEmail not in globals.active_users:
                    self.set_status(202)
                    self.write({'message':'Request Accepted'})
                    globals.active_users[userEmail] = SeleniumInstance(userEmail, dbconn, generate_string(6), generate_string(), datetime.datetime.now())
                    globals.active_users[userEmail].start('groups', fb_email, fb_pass, teleId, keywords, blacklistKeywords, groupIdList)

                elif globals.active_users[userEmail].runGroups == False:
                    self.set_status(202)
                    self.write({'message':'Request Accepted'})
                    globals.active_users[userEmail].start('groups', fb_email, fb_pass, teleId, keywords, blacklistKeywords, groupIdList)

                elif globals.active_users[userEmail].runGroups == True:
                    self.set_status(409)
                    self.write({'message':'Session Existed'})
                    dbconn.insert_app_event((globals.active_users[userEmail].session, userEmail, datetime.datetime.now(), 'existed_groups_instance', None, None), transform=False)
            else:
                self.set_status(403)
                self.write({'message':'Wrong Token'})
                dbconn.insert_app_event(('000000', userEmail, datetime.datetime.now(), 'token_error', None, None), transform=False)
        except:
            self.set_status(400)
            self.write({'message':'Bad Request'})
            dbconn.insert_app_event(('000000', userEmail, datetime.datetime.now(), 'groups_request_error', None, None), transform=False)


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
                    self.set_status(202)
                    self.write({'message':'Session Stopped'})
                except:
                    self.set_status(409)
                    self.write({'message':'No Session Found'})
                
            else:
                self.set_status(403)
                self.write({'message':'Wrong token'})
                dbconn.insert_app_event(('000000', userEmail, datetime.datetime.now(), 'token_error', None, None), transform=False)
        except:
            self.set_status(400)
            self.write({'message':'Bad Request'})
            dbconn.insert_app_event(('000000', userEmail, datetime.datetime.now(), 'stop_request_error', None, None), transform=False)


class ExtractPosts(RequestHandler):
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
                self.set_status(400)
                self.write({'message':'Wrong token'})
                dbconn.insert_app_event((globals.active_users[userEmail].session, userEmail, datetime.datetime.now(), 'token_error', None, None), transform=False)

        except:
            self.set_status(400)
            self.write({'message':'Bad Request'})
            dbconn.insert_app_event(('000000', userEmail, datetime.datetime.now(), 'stop_request_error', None, None), transform=False)


class CloseApp(RequestHandler):
    @coroutine
    def post(self):
        global globals
        try:
            userEmail = self.get_body_argument('email')
            email = self.get_body_argument('fb_email')
            email2 = self.get_body_argument('fb_email2')
            groupIdList = self.get_body_argument('group_id_list')

            self.write({'message':''})
            globals.active_users[userEmail].stop('both', email, email2, groupIdList)

        except:
            self.set_status(400)
            self.write({'message':'Bad Request'})


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
    threading.Thread(target=clear, daemon=True).start()
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    app = make_app()
    app.listen(8080)
    IOLoop.instance().start()

import sys
sys.path.append(sys.path[0]+'\\venv\\Lib\\site-packages') # For Task Schedule

from tornado.web import Application, RequestHandler
from tornado.ioloop import IOLoop
from tornado.gen import coroutine
import asyncio
from werkzeug.security import generate_password_hash, check_password_hash

import datetime, threading, time, json, re, pickle
import pandas as pd

from telegramBot import TelegramBot
from db import DBConnection
from instance import SeleniumInstance
from utils import get_regex, generate_string
from versions import versionAvailable
import globals


dbconn = DBConnection('config.ini')
bot = TelegramBot('config.ini', '/telegram-message')
serverStartTime = int(time.mktime(datetime.datetime.now().timetuple()))

def clear():
    while True:
        try:
            for user in globals.active_users:
                if datetime.datetime.now() - globals.active_users[user].ping > datetime.timedelta(seconds=180):
                    dbconn.insert_app_event((globals.active_users[user].session, globals.active_users[user].userEmail, datetime.datetime.now(), 'session_timeout', None, None), transform=False)
                    del globals.active_users[user]
            time.sleep(60)
        except:
            time.sleep(5)
            continue

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
                        globals.active_users[userEmail] = SeleniumInstance(userEmail, dbconn)
                        self.write({'token':globals.active_users[userEmail].token})
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
                    globals.active_users[userEmail] = SeleniumInstance(userEmail, dbconn)
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
        try:
            userEmail = self.get_body_argument('email')
            email = self.get_body_argument('fb_email')
            email2 = self.get_body_argument('fb_email2')
            groupIdList = self.get_body_argument('group_id_list')

            self.write({'message':'App closed'})
            try:
                globals.active_users[userEmail].stop('both', email, email2, groupIdList)
            except:
                pass

        except:
            self.set_status(400)
            self.write({'message':'Bad Request'})


class ForceClose(RequestHandler):
    def post(self):
        try:
            userEmail = self.get_body_argument('email')
            self.write({'message':'Force Close'})
            globals.active_users[userEmail].stop('both', None, None, None)
        except:
            self.set_status(400)
            self.write({'message':'Bad Request'})


class TelegramMessage(RequestHandler):
    @coroutine
    def post(self):
        self.set_status(200)
        self.write({'message':'OK'})

        try:
            json.loads(self.request.body.decode())["edited_message"]
            return None
        except:
            pass
        
        message = json.loads(self.request.body.decode())["message"]
        if message["date"] < serverStartTime:
            return None
        
        telegramId = message["from"]["id"]
        text = message["text"]
        if text.startswith('/c'):
            try:
                user_email, growth_staff, hubspot_owner_id = dbconn.get_staff(telegramId)
                if growth_staff is None or hubspot_owner_id is None:
                    bot.send_message('Tạo contact thất bại', telegramId)
                    return None

                textSplit = text.split(' ')
                if len(textSplit) < 3:
                    bot.send_message('Câu lệnh không hợp lệ', telegramId)
                    dbconn.insert_telegram_command((datetime.datetime.now(), text, False, user_email), transform=False)
                    return None

                phone = re.sub(r'\D', '', textSplit[1])
                phone = re.sub(r'^(0|\+84)', '84', phone)
                if re.search(r'\b(84[-.\s]?\d{9})\b', phone) is None:
                    bot.send_message('Tạo contact thất bại: Số điện thoại không hợp lệ', telegramId)
                    dbconn.insert_telegram_command((datetime.datetime.now(), text, False, user_email), transform=False)
                    return None

                email = phone + "@gmail.com"
                if textSplit[-1].lower() in ['t', 'tool', 'tools']:
                    growth_source = "Tool"
                elif textSplit[-1].lower() in ['d', 'direct', 'direct sale', 'direct sales', 'directs']:
                    growth_source = "Direct Sale"
                else:
                    bot.send_message('Tạo contact thất bại: Thiếu nguồn khách hàng', telegramId)
                    dbconn.insert_telegram_command((datetime.datetime.now(), text, False, user_email), transform=False)
                    return None

                if len(textSplit) > 3:
                    firstname = ' '.join([part for part in textSplit[2:-1] if part.find("@gmail.com") == -1]).strip()
                    if not firstname:
                        firstname = phone
                else:
                    firstname = phone

                aha_email = (textSplit[-2] if textSplit[-2].find("@gmail.com") != -1 else None)

                with open('C:\\Works\\repos\\playground\\hubspot_contact', 'rb') as f:
                    users = pickle.load(f)
                data = users[users.id == phone]

                if len(data) != 0:
                    bot.send_message('Tạo contact thất bại: Contact đã tồn tại', telegramId)
                    return None

                if aha_email:
                    response = bot.create_hubspot_contact(email=email, firstname=firstname, growth_staff=growth_staff, growth_source=growth_source, aha_email=aha_email, hubspot_owner_id=hubspot_owner_id)
                else:
                    response = bot.create_hubspot_contact(email=email, firstname=firstname, growth_staff=growth_staff, growth_source=growth_source, hubspot_owner_id=hubspot_owner_id)
                    
                if response.status_code == 409:
                    bot.send_message('Tạo contact thất bại: Contact đã tồn tại', telegramId)
                    dbconn.insert_telegram_command((datetime.datetime.now(), text, False, user_email), transform=False)
                elif response.status_code == 200:
                    bot.send_message('Tạo contact thành công', telegramId)
                    dbconn.insert_telegram_command((datetime.datetime.now(), text, True, user_email), transform=False)
                else:
                    bot.send_message('Tạo contact thất bại', telegramId)
                    bot.send_message(f'Failed from {user_email}\nSyntax: {text}\nReason:{response.content.decode()}', '807358017')
                    dbconn.insert_telegram_command((datetime.datetime.now(), text, False, user_email), transform=False)
                return None
            except Exception as e:
                bot.send_message('Tạo contact thất bại', telegramId)
                bot.send_message(f'Failed from {user_email}\nReason: {str(e)}', '807358017')
            finally:
                self.set_status(200)
                self.write({'message':'OK'})
                return None

        elif text.startswith('/i'):
            try:
                user_email, growth_staff, hubspot_owner_id = dbconn.get_staff(telegramId)
                if growth_staff is None or hubspot_owner_id is None:
                    bot.send_message('Tra cứu thất bại', telegramId)
                    return None

                textSplit = text.split(' ')
                if len(textSplit) > 2 or len(textSplit) == 1:
                    bot.send_message("Sai cú pháp", telegramId)
                    dbconn.insert_telegram_command((datetime.datetime.now(), text, False, user_email), transform=False)
                else:
                    user_id = re.sub(r'\D', '', textSplit[1])
                    user_id = re.sub(r'^(0|\+84)', '84', user_id)
                    if re.search(r'\b(84[-.\s]?\d{9})\b', user_id) is None:
                        bot.send_message('Tra cứu thất bại: Số điện thoại không hợp lệ', telegramId)
                        dbconn.insert_telegram_command((datetime.datetime.now(), text, False, user_email), transform=False)
                        return None

                    with open('C:\\Works\\repos\\playground\\hubspot_contact', 'rb') as f:
                        users = pickle.load(f)
                    data = users[users.id == user_id]

                    if len(data) == 0:
                        bot.send_message('User không tồn tại', telegramId)
                        return None

                    create_time = data.create_time.iloc[0]
                    last_order = data.last_order.iloc[0].ceil(freq='s')
                    growth_staff = data.growth_staff.iloc[0]

                    if (last_order is pd.NaT) and ((datetime.datetime.now() - create_time).days > 2):
                        transferable = True
                    else:
                        transferable = False

                    if (datetime.datetime.now() - last_order).days > 56:
                        reactivatable = True
                    else:
                        reactivatable = False

                    text = f"""
<b>User ID:</b> {user_id}
<b>Transferable:</b> {transferable}
<b>Reactivatable:</b> {reactivatable}

<b>Growth staff:</b> {growth_staff if growth_staff is not pd.NA else 'None'}
<b>Create time:</b> {create_time}
<b>Last order:</b> {last_order}"""
                    bot.send_message(text, telegramId)
            except Exception as e:
                bot.send_message('Tra cứu thất bại', telegramId)
                bot.send_message(f'Failed from {user_email}\nReason: {str(e)}', '807358017')
            finally:
                self.set_status(200)
                self.write({'message':'OK'})
                return None

class WhoRun(RequestHandler):
    def get(self):
        dictWhoRun = {}
        for user, instance in globals.active_users.items():
            data = {
                "run-ads": instance.runAds,
                "run-groups": instance.runGroups
            }
            dictWhoRun[user] = data
        self.write(dictWhoRun)


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
        ('/force-close', ForceClose),
        ('/telegram-message', TelegramMessage),
        ('/who-run', WhoRun),
        ('/', DemoScreen)
    ]
    return Application(urls, debug=debug, autoreload=autoreload)


if __name__ == '__main__':
    threading.Thread(target=clear, daemon=True).start()
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    app = make_app()
    app.listen(8080)
    IOLoop.instance().start()

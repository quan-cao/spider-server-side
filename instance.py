from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By

from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem

from utils import get_regex, push_tele
from accounts import telegramBotToken
import globals

import pandas as pd
import time, datetime, threading, re, json, requests, random


class SeleniumInstance:
    def __init__(self, userEmail, dbconn, token, session, ping):
        self.runAds = False
        self.runGroups = False
        self.userEmail = userEmail
        self.dbconn = dbconn
        self.token = token
        self.session = session
        self.ping = ping

        self.hubspot_contact_path = 'C:\\Works\\repos\\playground\\hubspot_contact'

        software_names = [SoftwareName.CHROME.value]
        operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value, OperatingSystem.MAC.value]   
        self.user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=100)

        self.user_agent = self.user_agent_rotator.get_random_user_agent()

        self.options = webdriver.ChromeOptions()
        self.options.add_argument("--disable-notifications")
        self.options.add_argument("--disable-infobars")
        self.options.add_argument("--mute-audio")
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--silent')
        self.options.add_argument('--log-level=OFF')
        self.options.add_argument(f'--user-agent={self.user_agent}')


    @staticmethod
    def standby():
        time.sleep(random.uniform(1.0, 3.0))


    def start(self, _type, email, password, teleId, keywords, blacklistKeywords, groupIdList=None):
        if _type == 'ads':
            threading.Thread(target=self.scrape_ads, args=(email, password, teleId, keywords, blacklistKeywords,), daemon=True).start()
        elif _type == 'groups':
            threading.Thread(target=self.scrape_groups, args=(email, password, teleId, keywords, blacklistKeywords, groupIdList,), daemon=True).start()


    def stop(self, _type, email, email2, groupIdList=None):
        global globals
        if _type == 'ads':
            self.runAds = False
            self.driverAds.close()
            self.dbconn.insert_app_event((self.session, self.userEmail, datetime.datetime.now(), 'stop_scrape_ads', email, None), transform=False)
        elif _type == 'groups':
            self.runGroups = False
            self.driverGroups.close()
            self.dbconn.insert_app_event((self.session, self.userEmail, datetime.datetime.now(), 'stop_scrape_groups', email2, groupIdList), transform=False)
        elif _type == 'both':
            if self.runAds == True:
                self.runAds = False
                self.driverAds.close()
                self.dbconn.insert_app_event((self.session, self.userEmail, datetime.datetime.now(), 'stop_scrape_ads', email, None), transform=False)
            if self.runGroups == True:
                self.runGroups = False
                self.driverGroups.close()
                self.dbconn.insert_app_event((self.session, self.userEmail, datetime.datetime.now(), 'stop_scrape_groups', email2, groupIdList), transform=False)

            del globals.active_users[self.userEmail]
            self.dbconn.insert_app_event((self.session, self.userEmail, datetime.datetime.now(), 'close_connection', None, None), transform=False)


    def log_in_facebook(self, driver, email, password):
        try:
            driver.get('https://www.facebook.com')
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.NAME, 'email'))).send_keys(email)
            time.sleep(2)
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.NAME, 'pass'))).send_keys(password)
            time.sleep(1)
            driver.switch_to.active_element.send_keys(Keys.RETURN)
            return True
        except:
            return False


    def scrape_groups(self, email, password, teleId, keywords, blacklistKeywords, groupIdList):
        self.runGroups = True

        self.dbconn.insert_app_event((self.session, self.userEmail, datetime.datetime.now(), 'start_scrape_groups', email, groupIdList), transform=False)

        kwRegex = get_regex(keywords)
        blacklistKwRegex = get_regex(blacklistKeywords, blacklist=True)
        groupIdList = groupIdList.split(',')

        self.driverGroups = webdriver.Chrome(options=self.options)
        login = self.log_in_facebook(self.driverGroups, email, password)
        if login:

            try:
                WebDriverWait(self.driverGroups, 20).until(EC.presence_of_all_elements_located((By.CLASS_NAME, '_4-u2')))
            except TimeoutException:
                self.dbconn.insert_app_event((self.session, self.userEmail, datetime.datetime.now(), 'cannot_log_in', email, None), transform=False)
                self.runAds = False
                self.driverGroups.close()

            oldUsers = pd.read_pickle(self.hubspot_contact_path).id.tolist()
            dataframe = pd.DataFrame(columns=['imported_time', 'type', 'profile', 'post', 'phone', 'content', 'group', 'user_email'])

            while self.runGroups:
                if datetime.datetime.now() - self.ping > datetime.timedelta(seconds=30):
                    self.stop('both', email, None)
                else:
                    try:
                        for groupId in groupIdList:
                            self.driverGroups.get(f'https://facebook.com/groups/{groupId.strip()}?sorting_setting=CHRONOLOGICAL')
                            posts = WebDriverWait(self.driverGroups, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'userContentWrapper')))
                            for p in posts:
                                if (p.find_element_by_class_name('_5ptz')
                                    .find_element_by_class_name('timestampContent')
                                    .text in ['Vừa xong', '1 phút', '2 phút', '3 phút', '4 phút', '5 phút',
                                              'Just now', '1 min', '2 mins', '3 mins', '4 mins', '5 mins']):

                                    try:
                                        p.find_element_by_class_name('see_more_link_inner').click()
                                    except:
                                        pass

                                    content = p.find_element_by_class_name('userContent').text
                                    if content == '':
                                        break
                                    else:

                                        if len(re.findall(kwRegex, content, re.IGNORECASE)) != 0 and not re.findall(blacklistKwRegex, content, re.IGNORECASE):
                                            try:
                                                profile = p.find_element_by_class_name('profileLink').get_attribute('href')
                                            except:
                                                profile = p.find_element_by_link_text(p.find_element_by_class_name('_7tae').text).get_attribute('href')
                                            if profile.find('profile.php') == -1:
                                                profile = profile.split('?')[0]
                                            else:
                                                profile = profile.split('&')[0]

                                            try:
                                                phone = re.search(r'([^0-9]+(0|84|\+84)[-.\s]?\d{1,3}[-.\s]?\d{2,4}[-.\s]?\d{2,4}[-.\s]?\d{2,4})', content).group()
                                                phone = re.sub(r'\D+', '', phone)
                                                phone = re.sub(r'^0', '84', phone)
                                            except:
                                                phone = None
                                            if phone in oldUsers:
                                                break
                                            else:
                                                try:
                                                    post = p.find_element_by_class_name('_5pcq').get_attribute('href')
                                                except:
                                                    break

                                                post_time = (pd.to_datetime(p.find_element_by_class_name('_5ptz').get_attribute('data-utime'), unit='s')
                                                            + datetime.timedelta(hours=7))

                                                dataframe = dataframe.append({'imported_time': post_time, 'type':'groups', 'profile':profile, 'post':post,
                                                                            'phone':phone, 'content':content, 'group':groupId, 'user_email':self.userEmail}, ignore_index=True)
                                        else:
                                            continue
                            dataframe = dataframe.drop_duplicates(subset='post', keep='first')
                            dataframe = dataframe.drop_duplicates(subset='content', keep='first')
                            oldPosts = self.dbconn.get_all_posts(self.userEmail, "groups", 'CURRENT_DATE')
                            if oldPosts == '[]':
                                oldPosts = pd.DataFrame(columns=['imported_time', 'type', 'profile', 'post', 'phone', 'content', 'group', 'user_email'])
                            else:
                                oldPosts = pd.json_normalize(json.loads(oldPosts))
                            dataframe = dataframe[(~dataframe.post.isin(oldPosts.post.tolist())) & (~dataframe.content.isin(oldPosts.content.tolist()))]
                            if len(dataframe) > 0:
                                push_tele(teleId, 'groups', df=dataframe)
                                self.dbconn.insert_fb_posts(dataframe)
                            dataframe = pd.DataFrame(columns=['imported_time', 'type', 'profile', 'post', 'phone', 'content', 'group', 'user_email'])
                            self.standby()
                    except Exception as err:
                        if type(err).__name__ in ['InvalidSessionIdException', 'NoSuchWindowException', 'ProtocolError']:
                            try:
                                self.driverAds.close()
                            except:
                                break
                            finally:
                                err_text = f"An {type(err).__name__} error occured.\nYour session have stopped."
                                data = {
                                    'chat_id': teleId,
                                    'text': err_text,
                                    'parse_mode': 'HTML'
                                }
                                requests.post(f"https://api.telegram.org/bot{telegramBotToken}/sendMessage", data=data)
                        else:
                            self.standby()
                            continue


    def scrape_ads(self, email, password, teleId, keywords, blacklistKeywords):
        self.runAds = True

        self.dbconn.insert_app_event((self.session, self.userEmail, datetime.datetime.now(), 'start_scrape_ads', email, None), transform=False)

        kwRegex = get_regex(keywords)
        blacklistKwRegex = get_regex(blacklistKeywords, blacklist=True)

        self.driverAds = webdriver.Chrome(options=self.options)
        login = self.log_in_facebook(self.driverAds, email, password)

        if login:
            try:
                WebDriverWait(self.driverAds, 20).until(EC.presence_of_all_elements_located((By.CLASS_NAME, '_4-u2')))
            except Exception as e:
                if type(e).__name__ == TimeoutException:
                    self.dbconn.insert_app_event((self.session, self.userEmail, datetime.datetime.now(), 'cannot_log_in', email, None), transform=False)
                else:
                    pass
                self.runAds = False
                try:
                    self.driverAds.close()
                except:
                    pass

            oldUsers = pd.read_pickle(self.hubspot_contact_path).id.tolist()
            
            while self.runAds:
                if datetime.datetime.now() - self.ping > datetime.timedelta(seconds=30):
                    self.stop('both', email, None)
                else:
                    try:
                        while len(self.driverAds.window_handles) > 1:
                            self.driverAds.switch_to.window(self.driverAds.window_handles[-1])
                            self.driverAds.close()
                        self.driverAds.get('https://www.facebook.com')
                        for _ in range(30):
                            time.sleep(1)
                            elems = WebDriverWait(self.driverAds, 20).until(EC.presence_of_all_elements_located((By.CLASS_NAME, '_4-u2')))
                            self.driverAds.switch_to.active_element.send_keys(Keys.PAGE_DOWN)
                            for e in elems[7:]:
                                if (e.text != '') and (e.text.find('Sponsored') != -1 or e.text.find('Được tài trợ') != -1):
                                    if not re.findall(blacklistKwRegex, e.text, re.IGNORECASE) and re.findall(kwRegex, e.text, re.IGNORECASE):
                                        try:
                                            page = WebDriverWait(e, 20).until(EC.presence_of_element_located((By.CLASS_NAME, '_5pb8'))).get_attribute('href').split('/')[3]
                                        except:
                                            try:
                                                page = e.find_element_by_link_text(e.find_element_by_class_name('_7tae').text).get_attribute('href').split('/')[3]
                                            except:
                                                if e.find_element_by_class_name('_7tae').text.find('like') != -1:
                                                    page = e.find_element_by_link_text(re.sub(r'\.$', '', e.find_element_by_class_name('_7tae').text.split(' like ')[1])).get_attribute('href').split('/')[3]
                                                elif e.find_element_by_class_name('_7tae').text.find('thích') != -1:
                                                    page = e.find_element_by_link_text(re.sub(r'\.$', '', e.find_element_by_class_name('_7tae').text.split(' thích ')[1])).get_attribute('href').split('/')[3]

                                        facebook = 'https://www.facebook.com/' + page
                                    
                                        oldData = self.dbconn.get_all_posts(self.userEmail, 'ads')
                                        if oldData != '[]':
                                            oldData = pd.json_normalize(json.loads(oldData))
                                        else:
                                            oldData = pd.DataFrame(columns=['profile'])

                                        isNew = facebook not in oldData.profile.tolist()

                                        if isNew:
                                            # Get page info
                                            self.driverAds.execute_script(f"window.open('{'https://www.facebook.com/' + page + '/about?ref=page_internal'}');")
                                            self.driverAds.switch_to.window(self.driverAds.window_handles[-1])
                                            name = WebDriverWait(self.driverAds, 20).until(EC.presence_of_element_located((By.CLASS_NAME, '_64-f'))).text
                                            checkPhone = 0
                                            try:
                                                pageInfo = WebDriverWait(self.driverAds, 20).until(EC.presence_of_element_located((By.ID, 'content')))
                                                phoneList = re.findall(r'\b(((0|84|\+84)[-.\s]?\d{1,3}[-.\s]?\d{2,4}[-.\s]?\d{2,4}[-.\s]?\d{2,4})|((1800|1900)[-.\s]?\d+[-.\s]?\d+))\b', pageInfo.text)
                                            except:
                                                phoneList = []
                                            if phoneList != []:
                                                phones = []
                                                for i in range(0, len(phoneList)):
                                                    phone = phoneList[i][0]
                                                    phone = re.sub(r'\D+', '', phone)
                                                    phone = re.sub(r'^0', '84', phone)
                                                    if phone not in phones:
                                                        phones.append(phone)
                                                
                                                for p in phones:
                                                    if p in oldUsers:
                                                        checkPhone += 1
                                                        break
                                            else:
                                                phones = None

                                            if checkPhone == 0:
                                                phones = ','.join(phones)
                                                self.dbconn.insert_fb_posts([(datetime.datetime.now(), 'ads', facebook, None, phones,
                                                                        None, None, self.userEmail)], transform=False)
                                                push_tele(teleId, 'ads', name, facebook, phones)

                                            self.driverAds.close()
                                            self.driverAds.switch_to.window(self.driverAds.window_handles[0])
                    except Exception as err:
                        if type(err).__name__ in ['InvalidSessionIdException', 'NoSuchWindowException', 'ProtocolError']:
                            try:
                                self.driverAds.close()
                            except:
                                break
                            finally:
                                err_text = f"An {type(err).__name__} error occured.\nYour session have stopped."
                                data = {
                                    'chat_id': teleId,
                                    'text': err_text,
                                    'parse_mode': 'HTML'
                                }
                                requests.post(f"https://api.telegram.org/bot{telegramBotToken}/sendMessage", data=data)
                        else:
                            self.standby()
                            continue
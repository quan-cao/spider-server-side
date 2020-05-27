import requests, json
import configparser as cfg
from exception import MissingValue

import utils.readConfig as cfg


class TelegramBot:
    def __init__(self, config, route):
        self.config = config
        self.route = route
        self.telegramBotToken = cfg.get_one(self.config, 'telegram', 'token')
        self.telegramEndpoint = f'https://api.telegram.org/bot{self.telegramBotToken}'
        self.hapikey = cfg.get_one(self.config, 'hubspot', 'hapikey')
        self.hubspotEndpoint = f'https://api.hubapi.com/contacts/v1/contact/?hapikey={self.hapikey}'


    def send_message(self, message, chatId, parse_mode='HTML', disable_preview=True):
        data = {
            'chat_id': chatId,
            'text': message,
            'parse_mode': parse_mode,
            'disable_web_page_preview': disable_preview
        }
        response = requests.post(self.telegramEndpoint + '/sendMessage', data=data)
        return response


    def create_hubspot_contact(self, **kwargs):
        if "email" in kwargs:
            properties = []
            for k, v in kwargs.items():
                contactProperty = {
                    "property": str(k),
                    "value": str(v)
                }
                properties.append(contactProperty)
            data = {
                "properties": properties
            }
            response = requests.post(self.hubspotEndpoint, data=json.dumps(data), headers={"Content-Type":"application/json"})
            return response
        raise MissingValue('Need property `email`')


    def set_webhook(self):
        """
        Set webhook for Telegram bot. New messages will be sent to webhook url via HTTP POST requests.

        When using webhook, getUpdates method will be disabled.
        """
        response = requests.get(f"{self.telegramEndpoint}/setWebhook?url={self.get_webhook_url}")
        return response


    def delete_webhook(self):
        """
        Remove webhook integration with Telegram Bot.
        """

        response = requests.post(self.telegramEndpoint + '/deleteWebhook')
        return response


    def get_webhook_url(self) -> str:
        """
        Returns target url got from Google Sheet
        """

        gsheetData = cfg.get_section(self.config, 'spreadsheet')
        spreadsheetId = gsheetData['spreadsheetId']
        _range = gsheetData['range']
        apiKey = gsheetData['apiKey']
        sheet = f'https://sheets.googleapis.com/v4/spreadsheets/{spreadsheetId}/values/{_range}?key={apiKey}'

        response = requests.get(sheet).json()
        domain = 'https://{}.localhost.run'.format(response['values'][0][0])
        return domain + self.route


    def push_tele(self, teleId, _type, name=None, facebook=None, phone=None, df=None):
        """
        Send notifications about new data
        """
        if _type == 'ads':
            text = """
<b>SPONSORED MERCHANT</b>
<b>Name:</b> {name}
<b>Facebook:</b> {facebook}
<b>Phone:</b> {phone}""".format(name=name, facebook=facebook, phone=phone)
            data = {
                'chat_id': teleId,
                'text': text,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True
            }
            response = requests.post(self.telegramEndpoint + '/sendMessage', data=data)
            return response


        if _type == 'groups':
            for i in range(len(df)):
                profile = df.iloc[i]['profile']
                content = df.iloc[i]['content']
                phone = df.iloc[i]['phone']
                post = df.iloc[i]['post']
                post_time = df.iloc[i]['imported_time']

                text = """
<b>NEW POST</b>
<b>Nội dung:</b> {content}
<b>Facebook:</b> {profile}
<b>Phone:</b> {phone}
<b>Link:</b> {post}
<b>Post time:</b> {post_time}""".format(content=content, profile=profile, phone=phone, post=post, post_time=post_time)
                data = {
                    'chat_id': teleId,
                    'text': text,
                    'parse_mode': 'HTML',
                    'disable_web_page_preview': True
                }
                response = requests.post(self.telegramEndpoint + '/sendMessage', data=data)
                return response

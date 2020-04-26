import requests

def push_tele(teleId, _type, name=None, facebook=None, phone=None, df=None):
    """
    Push Telegram message
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

        requests.post(f"https://api.telegram.org/bot914886775:AAF3eQTqG2ZL17yJDBtmDXs4ON1XD4Kc-Y0/sendMessage", data=data)

    if _type == 'groups':
        for i in range(len(df)):
            profile = df.iloc[i]['profile']
            content = df.iloc[i]['content']
            phone = df.iloc[i]['phone']
            post = df.iloc[i]['post']
            group = df.iloc[i]['group']
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
            requests.post(f"https://api.telegram.org/bot914886775:AAF3eQTqG2ZL17yJDBtmDXs4ON1XD4Kc-Y0/sendMessage", data=data)
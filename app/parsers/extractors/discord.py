import re
from selectolax.parser import HTMLParser
import json

def extract_discord_json(tree: HTMLParser):
    msgs = []

    for script in tree.css('script'):
        script_text = script.text(strip=False)
        match = re.search(r"let\s+messages\s*=\s*(\[.*?\])\s*;", script_text, re.DOTALL)

        if match:
            json_string = match.group(1)
            try:
                data = json.loads(json_string)

                for msg in data:
                    author_info = msg.get('author', {})
                    sender = author_info.get('username')
                    timestamp = msg.get('timestamp')
                    text = msg.get('content')

                    if not text:
                        if 'sticker_items' in msg and msg['sticker_items']:
                            sticker_name = msg['sticker_items'][0].get('name')
                            if sticker_name:
                                text = f"Sticker: '{sticker_name}'"
                        elif 'embeds' in msg and msg['embeds']:
                            embed_desc = msg['embeds'][0].get('description')
                            if embed_desc:
                                text = embed_desc

                    if sender and text:
                        msgs.append({
                            'source': 'Discord',
                            'timestamp': timestamp,
                            'sender': sender,
                            'message': text.strip()
                        })

                break
            except json.JSONDecodeError:
                print("Warning: Found a script with 'let messages' but failed to decode JSON.")
                continue

    return msgs


def extract_discord_html(tree):
    msgs = []

    for div in tree.css('div.chat-msg'):
        prof = div.css_first('div.chat-msg-profile')
        if prof:
            date_div = prof.css_first('div.chat-msg-date')
            if date_div:
                span = date_div.css_first('span')
                if span:
                    sender = span.text(strip=True)
                    timestamp = date_div.text(strip=True).replace(sender, '').strip()
                else:
                    sender, timestamp = 'Unknown', None
            else:
                sender, timestamp = 'Unknown', None
        else:
            sender, timestamp = 'Unknown', None

        text_div = div.css_first('div.chat-msg-text')
        text = text_div.text(strip=True) if text_div else None

        if text:
            msgs.append({
                'source': 'Discord',
                'timestamp': timestamp,
                'sender': sender,
                'message': text
            })

    return msgs

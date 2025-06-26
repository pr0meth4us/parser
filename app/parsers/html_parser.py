import re
import json
from .json_parser import parse_generic_json


def extract_json_from_html(tree):
    found = []
    for script in tree.css('script'):
        text = script.text() or ''
        match = re.search(r"messages\s*=\s*(\[.*?\])", text, flags=re.DOTALL)
        if match:
            try:
                arr = json.loads(match.group(1))
                found.extend(parse_generic_json(arr))
            except json.JSONDecodeError:
                continue
    return found


def extract_telegram(tree):
    msgs = []
    last_sender = None
    for div in tree.css('div.message'):
        ts = div.css_first('div.pull_right.date.details')
        timestamp = ts.attributes.get('title') if ts else None

        name = div.css_first('div.from_name')
        sender = name.text(strip=True) if name else last_sender
        last_sender = sender or last_sender

        txt = div.css_first('div.text')
        text = txt.text(strip=True) if txt else None

        if text and sender:
            msgs.append({'source': 'Telegram', 'timestamp': timestamp, 'sender': sender, 'message': text})
    return msgs


def extract_facebook(tree):
    msgs = []
    # Facebook structure can be tricky, targeting common patterns for message and timestamp
    message_divs = tree.css('div._3-95._a6-g')
    timestamp_divs = tree.css('div._3-94._a6-o')

    for i, msg_div in enumerate(message_divs):
        sender_div = msg_div.css_first('div._2ph_._a6-h._a6-i')
        sender = sender_div.text(strip=True) if sender_div else None

        if not sender:
            sender_div_fallback = msg_div.css_first('div[data-tooltip-content]')
            if sender_div_fallback:
                sender = sender_div_fallback.text(strip=True)

        if not sender:
            continue

        content_div = msg_div.css_first('div._2ph_._a6-p')
        text = None
        if content_div:
            # Get direct text content
            text = content_div.text(strip=True)
            if not text:
                # Try nested tags
                for tag in content_div.css('p, span, div'):
                    content = tag.text(strip=True)
                    if content:
                        text = content
                        break

        timestamp = None
        if i < len(timestamp_divs):
            ts_div = timestamp_divs[i].css_first('div._a72d') or timestamp_divs[i]
            if ts_div:
                timestamp = ts_div.text(strip=True)

        if sender and text and len(text.strip()) > 0:
            msgs.append({'source': 'Facebook', 'timestamp': timestamp, 'sender': sender, 'message': text.strip()})
    return msgs


def extract_instagram(tree):
    msgs = []
    # Try multiple selectors for Instagram message containers
    message_containers = tree.css('div.pam._3-95._2ph-._a6-g.uiBoxWhite.noborder, div._4tsk')
    if not message_containers:
        # Fallback to broader search
        message_containers = tree.css('div[class*="message"], div[class*="msg"]')

    for container in message_containers:
        # Try multiple selectors for sender
        sender_div = (container.css_first('div._3-95._2pim._a6-h._a6-i') or
                      container.css_first('div._2pim._a6-h._a6-i') or
                      container.css_first('div[class*="sender"]') or
                      container.css_first('div[class*="name"]'))
        sender = sender_div.text(strip=True) if sender_div else None
        if not sender:
            continue

        # Try multiple selectors for content
        content_div = (container.css_first('div._3-95._a6-p') or
                       container.css_first('div._a6-p') or
                       container.css_first('div[class*="content"]'))
        text = None
        if content_div:
            text = content_div.text(strip=True)
            if not text:
                # Try nested tags
                for tag in content_div.css('p, span, div'):
                    content = tag.text(strip=True)
                    if content:
                        text = content
                        break

        # Try multiple selectors for timestamp
        timestamp = None
        ts_div = (container.css_first('div._3-94._a6-o') or
                  container.css_first('div[class*="timestamp"]'))
        if ts_div:
            timestamp = ts_div.text(strip=True)

        if sender and text and len(text.strip()) > 0:
            msgs.append({'source': 'Instagram', 'timestamp': timestamp, 'sender': sender, 'message': text.strip()})
    return msgs


def extract_imessage(tree):
    msgs = []
    for div in tree.css('div.message'):
        rec = div.css_first('div.received')
        sent = div.css_first('div.sent')

        sender = None
        timestamp = None
        text = None
        source = 'iMessage'

        if rec:
            meta = rec.css_first('p')
            if meta:
                sender_span = meta.css_first('span.sender')
                sender = sender_span.text(strip=True) if sender_span else 'Unknown'
                timestamp_span = meta.css_first('span.timestamp')
                timestamp = timestamp_span.text(strip=True) if timestamp_span else None
            bubble = rec.css_first('span.bubble')
            text = bubble.text(strip=True) if bubble else None

        elif sent:
            meta = sent.css_first('p')
            if meta:
                sender_span = meta.css_first('span.sender')
                sender = sender_span.text(strip=True) if sender_span and sender_span.text(strip=True) != 'You' else 'Me'
                timestamp_span = meta.css_first('span.timestamp')
                timestamp = timestamp_span.text(strip=True) if timestamp_span else None
            bubble = sent.css_first('span.bubble')
            text = bubble.text(strip=True) if bubble else None
        else:
            continue

        if text:
            msgs.append({'source': source, 'timestamp': timestamp, 'sender': sender, 'message': text})
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
            msgs.append({'source': 'Discord', 'timestamp': timestamp, 'sender': sender, 'message': text})
    return msgs
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
            msgs.append({
                'source': 'Telegram',
                'timestamp': timestamp,
                'sender': sender,
                'message': text
            })

    return msgs
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
            msgs.append({
                'source': source,
                'timestamp': timestamp,
                'sender': sender,
                'message': text
            })

    return msgs
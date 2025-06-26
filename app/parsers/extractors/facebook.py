def extract_facebook(tree):
    msgs = []
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
            msgs.append({
                'source': 'Facebook',
                'timestamp': timestamp,
                'sender': sender,
                'message': text.strip()
            })

    return msgs
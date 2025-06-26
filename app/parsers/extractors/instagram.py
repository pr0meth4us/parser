def extract_instagram(tree):
    msgs = []

    message_containers = tree.css('div.pam._3-95._2ph-._a6-g.uiBoxWhite.noborder, div._4tsk')
    if not message_containers:
        message_containers = tree.css('div[class*="message"], div[class*="msg"]')

    for container in message_containers:
        sender_div = (container.css_first('div._3-95._2pim._a6-h._a6-i') or
                      container.css_first('div._2pim._a6-h._a6-i') or
                      container.css_first('div[class*="sender"]') or
                      container.css_first('div[class*="name"]'))
        sender = sender_div.text(strip=True) if sender_div else None

        if not sender:
            continue

        content_div = (container.css_first('div._3-95._a6-p') or
                       container.css_first('div._a6-p') or
                       container.css_first('div[class*="content"]'))
        text = None
        if content_div:
            text = content_div.text(strip=True)
            if not text:
                for tag in content_div.css('p, span, div'):
                    content = tag.text(strip=True)
                    if content:
                        text = content
                        break

        timestamp = None
        ts_div = (container.css_first('div._3-94._a6-o') or
                  container.css_first('div[class*="timestamp"]'))
        if ts_div:
            timestamp = ts_div.text(strip=True)

        if sender and text and len(text.strip()) > 0:
            msgs.append({
                'source': 'Instagram',
                'timestamp': timestamp,
                'sender': sender,
                'message': text.strip()
            })

    return msgs
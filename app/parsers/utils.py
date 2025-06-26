import hashlib

def generate_message_hash(message: dict) -> str:
    ts = str(message.get('timestamp', ''))
    sender = str(message.get('sender', ''))
    message_content = str(message.get('message', ''))
    unique_string = f"{ts}{sender}{message_content}"
    return hashlib.sha256(unique_string.encode('utf-8')).hexdigest()
import hashlib
import json
from datetime import datetime
from bs4 import BeautifulSoup
from .config import Config
from .date_parser import parse_datetime_comprehensive
from .html_parser import (
    extract_json_from_html, extract_telegram, extract_facebook,
    extract_instagram, extract_imessage, extract_discord_html
)
from .json_parser import parse_generic_json


def process_single_file(file_obj, seen_hashes: set):
    """
    Reads, parses, and deduplicates a single file object on-the-fly.

    Args:
        file_obj: The file-like object to process.
        seen_hashes: A set of hashes of messages that have already been processed.
                     This function will add new hashes to this set.

    Returns:
        A list of message dictionaries that are new and unique.
    """
    unique_msgs_from_this_file = []
    filename = getattr(file_obj, 'filename', 'unknown_file')


    if filename.lower().endswith('.json'):
        content = file_obj.read().decode('utf-8', errors='ignore')
        extracted_messages = parse_generic_json(json.loads(content))
    else: # Handle .html and .htm
        soup = BeautifulSoup(file_obj, 'lxml')
        extracted_messages = []
        extracted_messages.extend(extract_json_from_html(soup))
        extracted_messages.extend(extract_telegram(soup))
        extracted_messages.extend(extract_facebook(soup))
        extracted_messages.extend(extract_instagram(soup))
        extracted_messages.extend(extract_imessage(soup))
        extracted_messages.extend(extract_discord_html(soup))

    # --- Perform on-the-fly deduplication ---
    for msg in extracted_messages:
        # Ensure all components of the hash are strings to avoid errors
        ts = str(msg.get('timestamp', ''))
        sender = str(msg.get('sender', ''))
        message_content = str(msg.get('message', ''))

        # Create a unique hash for the message
        content_hash = hashlib.md5(f"{ts}{sender}{message_content}".encode('utf-8')).hexdigest()

        if content_hash not in seen_hashes:
            seen_hashes.add(content_hash)
            unique_msgs_from_this_file.append(msg)
    return unique_msgs_from_this_file


def deduplicate_and_sort_messages(unique_messages_list: list):
    standardized = []
    for msg in unique_messages_list:
        dt = parse_datetime_comprehensive(msg.get('timestamp', ''))
        if dt:
            msg['timestamp'] = dt.strftime(Config.TARGET_FORMAT)
            standardized.append(msg)

    def sort_key(msg):
        try:
            # This is the key for sorting based on the standardized timestamp string
            return datetime.strptime(msg.get('timestamp'), Config.TARGET_FORMAT)
        except (ValueError, TypeError):
            # Fallback for any message that might have failed standardization
            return datetime.min

    standardized.sort(key=sort_key)

    return standardized

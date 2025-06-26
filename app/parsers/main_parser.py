import json
from datetime import datetime

from selectolax.parser import HTMLParser

from .config import Config
from .date_parser import parse_datetime_comprehensive
from .detector import PlatformDetector
from .extractors import EXTRACTOR_MAP
from .json_parser import parse_generic_json
from .utils import generate_message_hash


def process_single_file(file_obj, seen_hashes: set):
    unique_msgs_from_this_file = []
    filename = getattr(file_obj, 'filename', getattr(file_obj, 'name', 'unknown_file'))
    extracted_messages = []

    try:
        if filename.lower().endswith('.json'):
            content = file_obj.read().decode('utf-8', errors='ignore')
            extracted_messages = parse_generic_json(json.loads(content))
        else:
            content = file_obj.read().decode('utf-8', errors='ignore')
            tree = HTMLParser(content)
            platform = PlatformDetector.detect_platform(tree)
            extractor_func = EXTRACTOR_MAP.get(platform)

            if extractor_func:
                platform_name = platform.replace('_', ' ').title()
                extracted_messages = extractor_func(tree)
                print(f"Detected {platform_name} in {filename}, extracted {len(extracted_messages)} messages.")
            else:
                print(f"Warning: Unknown or unsupported format in {filename}. Skipping.")

    except Exception as e:
        print(f"Error processing file {filename}: {e}")
        return []

    for msg in extracted_messages:
        if not msg.get('message'):
             continue
        content_hash = generate_message_hash(msg)
        if content_hash not in seen_hashes:
            seen_hashes.add(content_hash)
            unique_msgs_from_this_file.append(msg)

    return unique_msgs_from_this_file

def deduplicate_and_sort_messages(unique_messages_list: list):
    if not unique_messages_list:
        return []

    standardized = []
    for msg in unique_messages_list:
        dt = parse_datetime_comprehensive(msg.get('timestamp', ''))
        if dt:
            msg['timestamp'] = dt.strftime(Config.TARGET_FORMAT)
            standardized.append(msg)

    def sort_key(msg):
        try:
            return datetime.strptime(msg.get('timestamp'), Config.TARGET_FORMAT)
        except (ValueError, TypeError):
            return datetime.min

    standardized.sort(key=sort_key)

    return standardized
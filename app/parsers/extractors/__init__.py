# chat_parser/extractors/__init__.py

from .telegram import extract_telegram
from .facebook import extract_facebook
from .instagram import extract_instagram
from .imessage import extract_imessage
from .discord import extract_discord_html, extract_discord_json

EXTRACTOR_MAP = {
    'telegram': extract_telegram,
    'facebook': extract_facebook,
    'instagram': extract_instagram,
    'imessage': extract_imessage,
    'discord': extract_discord_html,
    'discord_json_embed': extract_discord_json
}
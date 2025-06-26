import re
from selectolax.parser import HTMLParser

class PlatformDetector:
    @staticmethod
    def detect_platform(tree: HTMLParser) -> str:

        for script in tree.css('script'):
            if re.search(r"let\s+messages\s*=\s*\[", script.text(strip=False)):
                return 'discord_json_embed'

        if tree.css_first('div.iMessage'):
            return 'imessage'

        if tree.css_first('div.history') and tree.css_first('div.message'):
            return 'telegram'

        if tree.css_first('div.pre--content') or tree.css_first('div.chat-msg'):
            return 'discord'

        if tree.css_first('div._2pim'):
            return 'instagram'

        if tree.css_first('div._2ph_'):
            return 'facebook'

        return 'unknown'


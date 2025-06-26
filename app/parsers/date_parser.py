import re
from datetime import datetime

def clean_timestamp(ts):
    if not ts:
        return ts
    ts = re.sub(r'\s*\([^)]*\)\s*$', '', ts)
    ts = re.sub(r'^\s*at\s+', '', ts, flags=re.IGNORECASE)
    ts = re.sub(r'\s*UTC.*$', '', ts)
    ts = re.sub(r'\s*GMT.*$', '', ts)
    ts = re.sub(r'\s*\+\d{4}.*$', '', ts)
    ts = ' '.join(ts.split())
    return ts

def parse_khmer_date(ts):
    if not ts:
        return None

    khmer_months = {
        'មករា': 1, 'កុម្ភៈ': 2, 'មីនា': 3, 'មេសា': 4,
        'ឧសភា': 5, 'មិថុនា': 6, 'កក្កដា': 7, 'សីហា': 8,
        'កញ្ញា': 9, 'តុលា': 10, 'វិច្ឆិកា': 11, 'ធ្នូ': 12
    }
    try:
        has_khmer_month = any(month in ts for month in khmer_months.keys())
        if not has_khmer_month:
            return None
        month_num = None
        for kh_month, num in khmer_months.items():
            if kh_month in ts:
                month_num = num
                break
        if not month_num:
            return None
        numbers = re.findall(r'\d+', ts)
        if len(numbers) >= 5:
            day, year, hour, minute, second = map(int, numbers[:5])
        elif len(numbers) >= 4:
            day, year, hour, minute = map(int, numbers[:4])
            second = 0
        else:
            return None
        if 'ល្ងាច' in ts and hour < 12:
            hour += 12
        elif 'ព្រឹក' in ts and hour == 12:
            hour = 0
        return datetime(year, month_num, day, hour, minute, second)
    except Exception as e:
        return None

def parse_datetime_comprehensive(ts):
    if not ts:
        return None

    ts = clean_timestamp(ts)
    khmer_date = parse_khmer_date(ts)
    if khmer_date:
        return khmer_date

    formats = [
        '%Y-%m-%dT%H:%M:%S.%f%z', '%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%dT%H:%M:%S.%fZ',
        '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M',
        '%d.%m.%Y %H:%M:%S', '%d.%m.%Y %H:%M', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y %I:%M:%S %p',
        '%m/%d/%Y %H:%M', '%m/%d/%Y %I:%M %p', '%m/%d/%y %H:%M:%S', '%m/%d/%y %I:%M:%S %p',
        '%d/%m/%Y %H:%M:%S', '%d/%m/%Y %I:%M:%S %p', '%d/%m/%Y %H:%M',
        '%b %d, %Y, %I:%M %p', '%b %d, %Y %I:%M:%S %p', '%b %d, %Y  %I:%M:%S %p',
        '%B %d, %Y, %I:%M %p', '%B %d, %Y %I:%M:%S %p', '%b %d, %Y, %H:%M:%S',
        '%B %d, %Y, %H:%M:%S', '%b %d, %Y %H:%M:%S', '%B %d, %Y %H:%M:%S',
        '%b %d, %Y', '%B %d, %Y', '%d-%m-%Y', '%m-%d-%Y', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y'
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(ts, fmt)
            # Basic validation to avoid default 1900 year for incomplete formats
            if dt.year == 1900 and not any(c.isdigit() for c in ts):
                continue
            return dt
        except ValueError:
            continue

    # Attempt to parse with a flexible regex for common patterns if direct parsing fails
    try:
        # Example: "DD Mon YYYY HH:MM:SS" or "Mon DD, YYYY HH:MM:SS AM/PM"
        match = re.search(r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{4})\s+(\d{1,2}:\d{2}:\d{2}(?:\s+[AP]M)?)', ts, re.IGNORECASE)
        if match:
            day, month_abbr, year, time_str = match.groups()
            # Convert month abbreviation to number
            month_num = datetime.strptime(month_abbr[:3], '%b').month
            if 'PM' in time_str.upper() and int(time_str.split(':')[0]) < 12:
                hour = int(time_str.split(':')[0]) + 12
            elif 'AM' in time_str.upper() and int(time_str.split(':')[0]) == 12:
                hour = 0
            else:
                hour = int(time_str.split(':')[0])

            minute = int(time_str.split(':')[1])
            second = int(time_str.split(':')[2].split(' ')[0]) # Handle potential AM/PM after seconds

            return datetime(int(year), month_num, int(day), hour, minute, second)
    except Exception:
        pass # Fall through to more general attempts or return None

    try:
        year_match = re.search(r'\b(19|20)\d{2}\b', ts)
        if year_match:
            year = int(year_match.group())
            numbers = [int(x) for x in re.findall(r'\d+', ts)]
            if len(numbers) >= 3:
                for i, num in enumerate(numbers):
                    if num == year:
                        remaining = numbers[:i] + numbers[i+1:]
                        if len(remaining) >= 2:
                            month = remaining[0] if remaining[0] <= 12 else remaining[1] if remaining[1] <= 12 else 1
                            day = remaining[1] if remaining[0] <= 12 else remaining[0] if remaining[0] <= 31 else 1
                            if month > 12: month, day = day, month # Swap if month seems to be day
                            if day > 31: day = 1 # Cap day
                            if month > 12: month = 1 # Cap month
                            hour = remaining[2] if len(remaining) > 2 and remaining[2] <= 23 else 0
                            minute = remaining[3] if len(remaining) > 3 and remaining[3] <= 59 else 0
                            second = remaining[4] if len(remaining) > 4 and remaining[4] <= 59 else 0
                            if 'PM' in ts.upper() and hour < 12: hour += 12
                            elif 'AM' in ts.upper() and hour == 12: hour = 0 # 12 AM is 0 hour
                            return datetime(year, month, day, hour, minute, second)
    except Exception:
        pass
    return None
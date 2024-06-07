def replace_str(value: str):
    return value.replace("default", "maxresdefault")

def format_date(value: str):
    return value.replace("T", " ").replace("Z", "")

def convert(value: str):
    return value.replace('"', '')
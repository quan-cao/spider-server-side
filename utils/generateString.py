import string, random

def generate_string(length=24):
    """
    Generate a <length> character string.
    """
    import string, random
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
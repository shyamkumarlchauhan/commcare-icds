import random
import string


# Reference: https://django-simple-captcha.readthedocs.io/en/latest/advanced.html#roll-your-own
def random_alphanumeric_challenge():
    ret = u''
    ret += ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return ret, ret

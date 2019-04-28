from urllib import request


def get_url(url, charset='utf-8'):
    req = request.urlopen(url)
    return req.read().decode(charset)

from __future__ import unicode_literals

def make_sure_unicode(msg):
    if isinstance(msg, bytes):
        return msg.decode()
    return u"%s" % __builtins__['str'](msg)

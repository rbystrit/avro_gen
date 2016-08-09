def make_fullname(ns, name):
    return ((ns + '.') if ns else '') + name


def split_fullname(fullname):
    idx = fullname.rfind('.')
    if idx < 0:
        return '', fullname

    return fullname[:idx], fullname[idx + 1:]


def get_shortname(fullname):
    return split_fullname(fullname)[1]

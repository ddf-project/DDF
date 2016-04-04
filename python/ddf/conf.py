from __future__ import unicode_literals


def find_ddf():
    import os
    if 'DDF_HOME' in os.environ:
        return os.path.abspath(os.environ['DDF_HOME'])

    path = os.path.abspath(os.path.split(os.path.abspath(__file__))[0] + '/../../')
    if all([os.path.exists(os.path.join(path, x)) for x in ['core', 'spark']]):
        return path
    raise ImportError('Unable to find DDF_HOME. Please define this variable in your environment')


DDF_HOME = find_ddf()

# TODO: find a better way to set this
SCALA_VERSION = '2.10'

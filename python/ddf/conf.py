def find_ddf():
    import os
    if 'DDF_HOME' not in os.environ:
        raise ImportError('Unable to find DDF_HOME. Please define this variable in your environment')
    return os.path.abspath(os.environ['DDF_HOME'])

DDF_HOME = find_ddf()

# TODO: find a better way to set this
SCALA_VERSION = '2.10'

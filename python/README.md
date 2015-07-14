Distributed DataFrame
=====================

1. Installation
---------------

Assume that the DDF package is already successfully built. We need to install the requirements for `pyddf`:

    $ cd <DDF_DIRECTORY>/python
    $ pip install -r requirements.txt
    
Then we will need to set the `$DDF_HOME` environment variable:

    $ cd <DDF_DIRECTORY>
    $ export DDF_HOME=`pwd`
    
**Optional**: to make the `DDF_HOME` variable to be available for all working session, we can add the following command into 
the `~/.bash_profile` (on MacOS) or `~/.profile` (on other Unix systems):
    
    export DDF_HOME=<DDF_DIRECTORY>
    
Now open your Python interpreter:

    $ cd <DDF_DIRECTORY>/python
    $ ipython
    
or if you don't set the `DDF_HOME` variable previously:

    $ cd <DDF_DIRECTORY>/python
    $ DDF_HOME=../ ipython

Of course, `python` will work just fine if you don't have `IPython`.

Now inside the Python interpreter, the DDF API is ready for usage:

    >>> import ddf
    >>> from ddf import DDFManager, DDF_HOME

    >>> dm = DDFManager('spark')

    >>> dm.sql('set hive.metastore.warehouse.dir=/tmp/hive/warehouse')
    >>> dm.sql('drop table if exists mtcars')
    >>> dm.sql("CREATE TABLE mtcars (mpg double, cyl int, disp double, hp int, drat double, wt double,"
       " qesc double, vs int, am int, gear int, carb string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '")
    >>> dm.sql("LOAD DATA LOCAL INPATH '" + DDF_HOME + "/resources/test/mtcars' INTO TABLE mtcars")

    >>> ddf = dm.sql2ddf('select * from mtcars')

    >>> print('Columns: ' + ', '.join(ddf.colnames))

    >>> print('Number of columns: {}'.format(ddf.cols))
    >>> print('Number of rows: {}'.format(ddf.rows))

    >>> ddf.summary()

    >>> ddf.head(10)

    >>> ddf.aggregate('sum(mpg), min(hp)', 'vs, am')

    >>> ddf.five_nums()

    >>> ddf.sample(10)

    >>> dm.shutdown()

2. Run tests
------------

    $ cd <YOUR_DDF_DIRECTORY>/python
    $ DDF_HOME=../ python examples/basics.py
    $ DDF_HOME=../ python tests/manager.py

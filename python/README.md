Distributed DataFrame
=====================

1. Installation
---------------

Assume that you already successfully built DDF. Then you will need to set the `$DDF_HOME` environment variable.

    $ cd <YOUR_DDF_DIRECTORY>
    $ export DDF_HOME=`pwd`
    
You might also want to add the following command into your `~/.bash_profile` or `~/.profile` depending on your OS:
    
    export DDF_HOME=<YOUR_DDF_DIRECTORY>
    
Now you can open your Python interpreter (of course you don't need to set `DDF_HOME` if you already export that
variable in your environment):

    $ cd <YOUR_DDF_DIRECTORY>/python
    $ DDF_HOME=../ ipython
    
and start playing with DDF API:

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

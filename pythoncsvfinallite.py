import pandas as pd
from sqlalchemy import create_engine # database connection


from IPython.display import display
 display(pd.read_csv('file1.csv'))


def makeFileIntoSQL(myFile, sqlName, sqlengine):
    chunksize = 20000
    j = 0
    index_start = 1
    for df in pd.read_csv(myFile, chunksize=chunksize, iterator=True, encoding='utf-8'):
        df = df.rename(columns={c: c.replace(' ', '|') for c in df.columns}) 
        df.index += index_start
        df.to_sql(sqlName, sqlengine, if_exists='append')

 ##change to if_exists='replace' if you don't want to replace the database file
        index_start = df.index[-1] + 1

if __name__ == "__main__":
  
    temp = create_engine('sqlite:///sampledb.db')

   
    makeFileIntoSQL('file1.csv', 'augdata', temp)
    makeFileIntoSQL('file2.csv', 'julydata', temp)

  
    df = pd.read_sql_query('SELECT * FROM augdata', temp)
    print "This is data from file1.csv"
    print df
    print ""

    ##TestCase 2
    df_july = pd.read_sql_query('SELECT * FROM julydata', temp)
    print "This is data from file2.csv"
    print df_july
    print ""

    ##TestCase 3
    df = pd.read_sql_query('SELECT NAME FROM augdata', temp)
    print "This is data from file1.csv"
    print df
    print ""

    ##TestCase 4
    df = pd.read_sql_query('SELECT AGE, COUNT(*) as `num_complaints`'
                           'FROM augdata '
                           'GROUP BY NAME', temp)
    print "This is data from file1.csv"
    print df
    print ""

    ##TestCase 5
    df = pd.read_sql_query('SELECT ID, COUNT(*) as `num_complaints`'
                           'FROM augdata '
                           'GROUP BY BEING '
                           'ORDER BY -num_complaints', temp)
    print "This is data from file1.csv"
    print df
    print ""
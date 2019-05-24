import snowflake.connector
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

import pandas as pd
import sys
import datetime

from snow.queries import get_query



def __create_connection(role):
  try:
    ctx = snowflake.connector.connect(
        user='Zan',
        password='Kundalini55!',
        account='livenpay.ap-southeast-2',
        role=role)
    print('Using {}...'.format(role))
  except:
    print('Something went wrong with establishing the connection.')
  return ctx



def __setup_snowflake(cursor,wh,db,schema):
  try:
    cursor.execute("use warehouse {}".format(wh))
    cursor.execute("use {}.{}".format(db,schema))
  except: 
    print('\n--- Error setting up Snowflake ---\n')
    return False
  return True



def __query(cursor,query):
  try:
    cursor.execute(query)
    results = cursor.fetchall()
  except: pass
  return results



def __list_to_df(data,q_kind):
  try:
    if q_kind=='interactions':
      df = pd.DataFrame(data,columns=['email','userId','funnel1','funnel2'
                                ,'funnel3','funnel10','amount','is_inorganic','city'
                                ,'merchantId','merchant','branchId','date','aov'])
      return df

    elif q_kind=='coordinates':
      df = pd.DataFrame(data,columns=['branchId','latitude','longitude'])
      return df

    elif q_kind=='emails':
      df = pd.DataFrame(data,columns=['id','email'])
      return df
  
    else:
      True==False

  except: pass
  finally: return df



def from_snow(schema='postgres'
              ,wh='zans_wh'
              ,db='zans_db'
              ,q_kind='interactions'
              ,query=None
              ,to_df=True
              ,verbose=False
              ,role='zans_role'):
  
  results = None
  y = ''

  if query != None:
    qry = query
  else:
    qry = get_query(kind=q_kind)
    
  try:
    y = 'creating the connection'
    ctx = __create_connection(role)
    cursor = ctx.cursor()
    print('Established connection.')
    y = 'executing {}.{}.{}'.format(wh,db,schema)
    
    success = __setup_snowflake(cursor,wh,db,schema)
    assert success == True

    print('Querying {}.{}.{}'.format(wh,db,schema))
    
    x = q_kind if query==None else 'custom'
    
    if verbose:
      print('\nQuery: ',qry,'\n')
    
    y = 'fetching {} query'.format(x)
    
    data = __query(cursor,qry)
    print('{} data: '.format(x.capitalize()),data[0],'\n')
    
    if to_df:
      results = __list_to_df(data=data,q_kind=q_kind)
    else:
      results = data

  except:
    print('\n--- Something went wrong while {} ---\n'.format(y))

  finally:
    cursor.close()
    ctx.close()
    print('Connection succesfully closed.')
    
  return results



def to_snow(df=None
          ,table=None
          ,schema=None
          ,stamp=False
          ,extend=False):

  date = datetime.datetime.now().strftime("%Y-%m-%d")
  print(date)
  
  if extend:
    pre = from_snow(schema=schema,query='select * from {}'.format(table+'_'+date),to_df=False)
    df = pre.merge(df,on='email',how='outer')
  
  if ~stamp: date = ''
  try:
    df.head()
    print('Accepted dataframe object')
  except ValueError:
      print('Missing dataframe object')

  try: type(table)==str
  except ValueError:
    if table==None: print('Table element not specified.')
    else: print('Table element is not a string.')
  
  try:
    type(schema)==str
  except ValueError:
    if schema==None:
      print('Schema element not specified.')
    else:
      print('Schema element is not a string.')

  engine = create_engine(
      'snowflake://{user}:{password}@{account}/'.format(
          user='Zan',
          password='Kundalini55!',
          account='livenpay.ap-southeast-2',
      )
  )

  try:
      connection = engine.connect()
      connection.execute('use warehouse zans_wh')
      try:
        connection.execute('create schema zans_db.{}'.format(schema))
        print('Schema: {} created.'.format(schema))
      except:
        print('Schema: {} already exists; updating {} table.'.format(schema,table))
        pass
      connection.execute('use zans_db.{}'.format(schema))

      df.drop_duplicates().to_sql(table.lower()+'_'+date
                  , con=connection 
                  , if_exists='replace'
                  , index=False) 
      
      print('{} successfully updated.'.format(table))

  finally:
      connection.close()
      engine.dispose()
      print('Engine successfully closed')

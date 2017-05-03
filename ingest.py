import sqlite3
import os
import csv
import hashlib
import pandas
import email_normalize

def reset_db():
  os.unlink('knowledge.db')
  conn = sqlite3.connect('knowledge.db')
  c = conn.cursor()
  c.executescript(open('docs/schema.sql', 'r').read())

def norm(value, label):
  if label.lower() == 'email':
    value = email_normalize.normalize(value)

  return value

def get_entity(factset, ids, c):
  """ attempts to return a list of entity ids related a given factset """
  for label in ids:
    value = norm(factset[label], label)
    label = label.lower().replace(' ','-').replace('_','-')

    c.execute('select entity_id from factsets where id in (select factset_id from facts where value=?)', (value,))
    entities = c.fetchall()

    print(value,len(entities))

    if len(entities) > 0:
      return entities[0][0]

  c.execute('insert into entities default values')
  return c.lastrowid

def get_df_ids(df):
  ids = []

  for col in df:
    # TODO: check for compound keys (e.g. first + last + birthday)
    uniqueness = float(len(df[col].unique())) / len(df[col])

    if uniqueness == 1.0:
      ids.append(col)

  return ids

def ingest_csv(filename):
  sha256 = hashlib.sha256(open(filename, 'rb').read()).hexdigest()

  conn = sqlite3.connect('knowledge.db')
  c = conn.cursor()

  c.execute('insert into datasources (uri, sha256) values (?,?)',
    (filename, sha256))

  datasource_id = c.lastrowid

  df = pandas.read_csv(open(filename, 'r'))
    
  ids = get_df_ids(df)

  for (i, row) in df.iterrows():
    entity_id = get_entity(row, ids, c)
    
    c.execute('insert into factsets (datasource_id, datasource_field, entity_id) values (?,?,?)',
      (datasource_id, i, entity_id))

    factset_id = c.lastrowid

    for key in row.keys():
      value = norm(row[key], key)
      c.execute('insert into facts (value,label,factset_id) values (?,?,?)',
        (value, key.lower(), factset_id))
            
  conn.commit()

reset_db()
ingest_csv('test/studir.csv')
ingest_csv('test/stusucc.csv')
#ingest_csv('test/studiv.csv')
#ingest_csv('test/retention.csv')
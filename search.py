import sqlite3

conn = sqlite3.connect('knowledge.db')
c = conn.cursor()

for row in c.execute("""
  select distinct entity_id from factsets where id in (
    select distinct factset_id from facts where value like '%smith%'
  )
  """).fetchall():
  
  print("Entity: %s" % row[0])

  for fact in c.execute('select label, value from facts where factset_id in (select id from factsets where entity_id=?)', (int(row[0]),)):
    print(fact)


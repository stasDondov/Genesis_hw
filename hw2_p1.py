import sqlite3
import pandas
import os


db = sqlite3.connect('sandbox.db')
cur = db.cursor()

for folder in os.listdir(path="sandbox.KNU"):
	for table in os.listdir(path=f"sandbox.KNU/{folder}"):
		df = pandas.read_table(f'sandbox.KNU/{folder}/{table}', delim_whitespace=False)
		s = "CREATE TABLE IF NOT EXISTS " + table.replace(".tsv", "") + " (" + ", ".join([f"{k} {v}" for k, v in df.dtypes.items()]) + ")"
		s = s.replace("int64", "INTEGER").replace("object", "TEXT").replace("float64", "REAL")
		#print(s)
		cur.execute(s)
		df.to_sql(table.replace(".tsv", ""), db, if_exists='append', index=False)
		db.commit()

db.close()


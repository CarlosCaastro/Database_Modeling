from objeto import MySQL

schema = 'teste'
istance = MySQL(db_connection='mysql://root:0106@localhost:3306/')
istance.create_schema(schema_name=schema)

with open('ddls.sql','r') as sql_dlls:
    sql_script = sql_dlls.read()

statements = sql_script.split(';')

for ddl in statements:
    istance.run_sql(sql=ddl,schema=schema)
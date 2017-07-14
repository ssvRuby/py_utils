from sys import modules
import cx_Oracle


conn = cx_Oracle.connect('sys/oracle@172.25.100.212/wla', mode=cx_Oracle.SYSDBA)
cursor = conn.cursor()
# cursor.execute(create_table)


M = []
for m_name in modules.items():
    try:
        M.append((m_name))
    except AttributeError:
        pass

print(type(M[0]))
print(M)

# cursor.prepare("INSERT INTO python_modules(module_name, file_path) VALUES (:1, :2)")
cursor.prepare("INSERT INTO python_modules(module_name) VALUES (:1)")
cursor.executemany(None, M)
conn.commit()

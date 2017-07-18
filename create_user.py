import cx_Oracle

oracle_user_name = 'test'
oracle_user_password = 'test'
oracle_ip = '172.25.100.212'
oracle_sid = 'wla'

create_user_clause = 'CREATE USER {} IDENTIFIED BY {} DEFAULT TABLESPACE \
               USERS TEMPORARY TABLESPACE TEMP'.format(oracle_user_name, oracle_user_password)
clause_list = [create_user_clause,
                'GRANT CREATE SESSION TO {}'.format(oracle_user_name),
                'GRANT CREATE SESSION TO {}'.format(oracle_user_name),
                'GRANT CONNECT,RESOURCE TO {}'.format(oracle_user_name),
                'GRANT CREATE PROCEDURE TO {}'.format(oracle_user_name),
                'GRANT CREATE VIEW TO {}'.format(oracle_user_name),

                'GRANT ALTER ANY TABLE  TO {}'.format(oracle_user_name),
                'GRANT ALTER ANY PROCEDURE TO {}'.format(oracle_user_name),
                'GRANT ALTER ANY TRIGGER TO {}'.format(oracle_user_name),
                'GRANT ALTER PROFILE  TO {}'.format(oracle_user_name),

                'GRANT DELETE ANY TABLE TO {}'.format(oracle_user_name),
                'GRANT DROP ANY TABLE TO {}'.format(oracle_user_name),
                'GRANT DROP ANY PROCEDURE TO {}'.format(oracle_user_name),
                'GRANT DROP ANY TRIGGER TO {}'.format(oracle_user_name),
                'GRANT DROP ANY VIEW TO {}'.format(oracle_user_name),
                'GRANT DROP PROFILE TO {}'.format(oracle_user_name),
                'GRANT CREATE SESSION TO {}'.format(oracle_user_name)
               ]
try:

    conn = cx_Oracle.connect('sys/oracle@{}/{}'.format(oracle_ip, oracle_sid), mode=cx_Oracle.SYSDBA)
    cursor = conn.cursor()

    for clause in clause_list:
        print(clause)
        cursor.execute(clause)

    conn.close()

    conn = cx_Oracle.connect('{}/{}@{}/{}'.format(oracle_user_name, oracle_user_password, oracle_ip, oracle_sid))

except cx_Oracle.DatabaseError as ora_ex:
    error, = ora_ex.args
    print("Oracle-Error-Code:", error.code)
    print("Oracle-Error-Message:", error.message)


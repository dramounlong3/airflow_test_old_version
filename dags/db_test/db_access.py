import pymssql

class DB_Access:

    def db_conn(self):
        try:
            conn = pymssql.connect(host = "172.21.176.1", database = "BI_Data_Alert", user='sa', password='19890729')
            cur = conn.cursor()
            return conn, cur
        except Exception as e:
            print(f'{str(e)}')
            raise RuntimeError("db connection failed.")


            
    def db_execute(self, conn, cur, sql_statement, parameters, is_commit):
            try:
                cur.execute(sql_statement, parameters)
                if is_commit:
                    conn.commit()
            except Exception as e:
                conn.rollback()
                print(f'{str(e)}')
                raise RuntimeError("db executed failed.")
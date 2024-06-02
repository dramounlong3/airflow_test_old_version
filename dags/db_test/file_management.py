from db_test.db_access import DB_Access

class File_Management:
    db = DB_Access()

    def read_file(self):
        try:
            conn = None
            cur = None
            conn, cur = self.db.db_conn()
            
            sql_query = f"""
                INSERT INTO BI_Data_Alert.dbo.Project
                VALUES(%s, %s, %s)
            """
            params = ('6', 'ABCD', 'Mark.Lin')
            self.db.db_execute(conn, cur, sql_query, params, is_commit=True)

            sql_query = f"""
                INSERT INTO BI_Data_Alert.dbo.Project
                VALUES(%s, %s, %s)
            """
            params = ('7', 'ABCD', 'Joe.Lin')
            self.db.db_execute(conn, cur, sql_query, params, is_commit=True)
        except Exception as e:
            print(str(e))
            raise RuntimeError("file_management failed.")
        finally:
            print("file_management close")
            if cur is not None: cur.close()
            if conn is not None: conn.close()
            
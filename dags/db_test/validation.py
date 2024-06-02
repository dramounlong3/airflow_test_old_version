from db_test.db_access import DB_Access

class Validation:
    db = DB_Access()

    def write_project_info(self):
        try:
            conn = None
            cur = None
            conn, cur = self.db.db_conn()
            
            sql_query = f"""
                DELETE FROM BI_Data_Alert.dbo.Project
                WHERE Project_ID >= %s
            """
            params = ('3')
            self.db.db_execute(conn, cur, sql_query, params, is_commit=False)

            sql_query = f"""
                INSERT INTO BI_Data_Alert.dbo.Project
                VALUES(%s, %s, %s)
            """
            params = ('3', 'ABCD', 'Marry.Lin')
            self.db.db_execute(conn, cur, sql_query, params, is_commit=False)

            sql_query = f"""
                INSERT INTO BI_Data_Alert.dbo.Project
                VALUES(%s, %s, %s)
            """
            params = ('4', 'ABCD', 'Jemery.Lin')
            self.db.db_execute(conn, cur, sql_query, params, is_commit=False)
            
            sql_query = f"""
                SELECT Project_ID, Project_Name, PM
                FROM BI_Data_Alert.dbo.Project
                WHERE Project_ID = %s
            """
            params = (6)
            self.db.db_execute(conn, cur, sql_query, params, is_commit=False)
            data = cur.fetchall() # ==> 此時查詢, 除了DB原有的資料之外, 前面做完還沒commit的狀況也通通都會納入此查詢的考量
            
            # if data is None or len(data) == 0:
            #     raise RuntimeError("no data")
            
            # data = [row[1] for row in data] #取row裡面的其中一個元素時可以這樣變list
            data = [element for row in data for element in row] #取row裡面的所有元素 要展開為一維陣列時可以這樣變list

            print("Project content:\n", data)

            sql_query = f"""
                INSERT INTO BI_Data_Alert.dbo.project
                VALUES(%s, %s, %s)
            """
            params = ('5', 'ABCD', 'Steve.Lin')
            self.db.db_execute(conn, cur, sql_query, params, is_commit=True)
        except Exception as e:
            print(str(e))
            raise RuntimeError("validation failed.")
        finally:
            print("validation close")
            if cur is not None: cur.close()
            if conn is not None: conn.close()
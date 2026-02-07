import os
import sys
import mysql.connector
from dotenv import load_dotenv


def check_db_connection():
    load_dotenv()
    host = os.getenv("DB_HOST")
    port = int(os.getenv("DB_PORT") or 3306)
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    database = os.getenv("DB_NAME")
    ssl_ca = os.getenv("DB_SSL_CA")

    if not host:
        return False, "DB_HOST chưa được cấu hình"

    try:
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            ssl_ca=ssl_ca,
            ssl_disabled=not bool(ssl_ca),
            connection_timeout=10,
        )
        conn.close()
        return True, "Kết nối DB thành công"
    except mysql.connector.Error as e:
        return False, f"Lỗi kết nối MySQL: {e}"
    except Exception as e:
        return False, f"Lỗi khác: {e}"


if __name__ == "__main__":
    ok, msg = check_db_connection()
    print(msg)
    sys.exit(0 if ok else 1)

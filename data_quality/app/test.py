from app import get_connection
from werkzeug.security import generate_password_hash

conn = get_connection()
cursor = conn.cursor()

password = generate_password_hash("admin123")

cursor.execute("""
INSERT INTO portal_users (Username, Email, password_hash, Role, must_reset_password)
VALUES (?, ?, ?, ?, ?)
""", ("admin", "sejal28862@gmail.com", password, "admin", 0))

conn.commit()

# 🔍 VERIFY IMMEDIATELY
cursor.execute("SELECT * FROM portal_users WHERE Username = 'admin'")
print(cursor.fetchall())

conn.close()

print("Admin created!")
from flask import Flask, render_template, request, redirect, flash, url_for, jsonify, send_file, session
from datetime import datetime, timedelta
import re
import os
import pyodbc
import pandas as pd
import tempfile
from groq import Groq
from dotenv import load_dotenv
import json
import random
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
from flask_mail import Mail, Message
import string 

load_dotenv()

app = Flask(__name__)
# app.secret_key = os.getenv("FLASK_SECRET_KEY")
client = Groq(api_key=os.getenv("GROQ_API_KEY"))
app.config['MAIL_SERVER'] = os.getenv('MAIL_SERVER')
app.config['MAIL_PORT'] = int(os.getenv('MAIL_PORT'))
app.config['MAIL_USE_TLS'] = os.getenv('MAIL_USE_TLS') == 'True'
app.config['MAIL_USERNAME'] = os.getenv('MAIL_USERNAME')
app.config['MAIL_PASSWORD'] = os.getenv('MAIL_PASSWORD')
if not app.config.get('MAIL_PASSWORD'):
    raise Exception("MAIL_PASSWORD not set in environment")
app.secret_key = os.getenv('SECRET_KEY')
FAQ_DF = pd.read_csv("portal_faq.csv", encoding="utf-8")

otp_store = {}
otp_store_signup = {}
mail = Mail(app)
mail.init_app(app)
ALLOWED_TABLES = {
    "Customer",
    "CustomerTexts",
    "CustomerType",
    "CustomerTypeTexts",
    "FinancialTransactions",
    "GLAccounts",
    "GLAccountsHierarchy",
    "GLAccountsTexts",
    "GLAccountType",
    "GLAccountTypeTexts",
    "Product",
    "ProductCategories",
    "ProductCategoryTexts",
    "ProductHierarchy",
    "ProductTexts",
    "ProfitCenter",
    "ProfitCenterHierarchy",
    "ProfitCenterTexts",
}
ALLOWED_TABLES_MAP = {t.lower(): t for t in ALLOWED_TABLES}
def get_connection():
    return pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=localhost;'
        'DATABASE=dcc;' #sales;'
        'Trusted_Connection=yes;'
    )

def table_exists(table_name):
    temp_conn = get_connection()
    cursor = temp_conn.cursor()
    cursor.execute("SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?", (table_name,))
    result = cursor.fetchone()
    temp_conn.close()
    return result is not None

def column_exists(table_name, column_name):
    temp_conn = get_connection()
    cursor = temp_conn.cursor()
    cursor.execute("SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME = ?", (table_name, column_name))
    result = cursor.fetchone()
    temp_conn.close()
    return result is not None

def create_user_from_request(request_id,role):
    role=role
    temp_conn = get_connection()
    cursor = temp_conn.cursor()

    cursor.execute("SELECT * FROM user_requests WHERE id=?", (request_id,))
    user = cursor.fetchone()
    
    temp_password = generate_temp_password()
    hashed_password = generate_password_hash(temp_password)
    
    cursor.execute("""
        INSERT INTO portal_users (username, email, password, role, must_reset_password, password_hash)
        VALUES (?, ?, ?,?, ?, ?)
    """, (user.name, user.email, temp_password, role,1, hashed_password))

    cursor.execute("UPDATE user_requests SET status='approved' WHERE id=?", (request_id,))

    temp_conn.commit()
    temp_conn.close()

    send_email(user.email, temp_password, user.name)

def send_email(to_email, password, user_name):
    

    msg = Message(
        subject="Welcome to Genesis DCC 🚀",
        sender=app.config['MAIL_USERNAME'],
        recipients=[to_email]
    )

    msg.html = f"""
    <html>
     <body style="font-family:Arial;background:#f4f6f9;padding:20px;">
        <div style="max-width:600px;margin:auto;background:white;border-radius:10px;overflow:hidden;">

            <div style="background:#2a5298;padding:20px;text-align:center;">
                <img src="http://127.0.0.1:5000/static/images/dcc_logo.png" height="50">
            </div>

            <div style="padding:30px;">
                <h2 style="color:#2a5298;">Welcome {user_name} 🎉</h2>

                <p>Your account has been approved.</p>

                <div style="background:#f1f1f1;padding:15px;border-radius:8px;">
                    <p><b>Username:</b> {user_name}</p>
                    <p><b>Password:</b> {password}</p>
                </div>

                <p style="color:red;"><b>⚠ Reset password on first login</b></p>

                <a href="http://localhost:5000/login"
                   style="display:inline-block;background:#2a5298;color:white;padding:10px 20px;border-radius:5px;text-decoration:none;">
                   Login Now
                </a>

                <hr>
                <p style="font-size:12px;color:#888;">Genesis Data Control Centre</p>
            </div>

        </div>
    </body>
    </html>
    """
    
    mail.send(msg)

def get_table_names():
    temp_conn = get_connection()
    cursor = temp_conn.cursor()
    cursor.execute("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT IN ('dq_query_log','counter_file_id','Master_Rules','Suggestions','ChatbotLogs')
    """)
    return [row[0] for row in cursor.fetchall()]

def get_db_schema():
    temp_conn = get_connection()
    cursor = temp_conn.cursor()
    cursor.execute("""
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        ORDER BY TABLE_NAME
    """)
    rows = cursor.fetchall()
    temp_conn.close()

    schema = {}
    for table, column, dtype in rows:
        schema.setdefault(table, []).append({
            "column": column,
            "type": dtype
        })
    return schema

def rank_tables_by_rule(rule, schema):
    rule_lower = rule.lower()
    ranked = []

    for table, columns in schema.items():
        score = 0

        # Boost if table name appears in rule
        if table.lower() in rule_lower:
            score += 5

        for col in columns:
            if col["column"].lower() in rule_lower:
                score += 2

        if score > 0:
            ranked.append((table, score))

    ranked.sort(key=lambda x: x[1], reverse=True)

    # Keep top 3 most relevant tables only
    top_tables = [t[0] for t in ranked[:3]]

    # If nothing matched, fallback to all
    if not top_tables:
        return schema

    return {t: schema[t] for t in top_tables}

def extract_entity(rule):
    rule_lower = rule.lower()
    tables = get_table_names()

    for table in tables:
        if table.lower() in rule_lower:
            return table

        # also try singular form match
        if table.lower().rstrip("s") in rule_lower:
            return table

    return None

def extract_target_table(rule, schema):
    """
    Detect table based on business context like:
    'in finance details'
    'from customer table'
    'within transactions'
    """

    rule_lower = rule.lower()

    # Look for context after keywords
    match = re.search(r"\b(in|from|within|inside|under)\s+([a-zA-Z\s]+)", rule_lower)
    if not match:
        return None

    context_phrase = match.group(2).strip()

    # Normalize phrase
    context_phrase = context_phrase.replace(" ", "")

    for table in schema.keys():
        table_clean = table.lower()

        # direct match
        if table_clean in context_phrase:
            return table

        # semantic priority rules (important for your schema)
        if "finance" in context_phrase or "transaction" in context_phrase:
            if table_clean == "financialtransactions":
                return table

        if "customer" in context_phrase and table_clean == "customer":
            return table

        if "product" in context_phrase and table_clean == "product":
            return table

        if "profitcenter" in context_phrase and table_clean == "profitcenter":
            return table

        if "glaccount" in context_phrase and table_clean == "glaccounts":
            return table

    return None


def extract_json_from_llm(text: str):
    """
    Safely extracts JSON object from LLM responses
    that may contain markdown or extra text.
    """
    if not text:
        return None
    cleaned = text.strip()
    cleaned = cleaned.replace("```json", "").replace("```", "").strip()

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            return None

    return None

def detect_table_column_from_rule(rule, schema):
    schema_json = json.dumps(schema, indent=2)
    prompt = f"""
    SYSTEM INSTRUCTION:
    You are a database schema expert.

    IMPORTANT RULES:
    1. If multiple tables contain the same column name,
    choose the table that best matches the business meaning of the rule.
    2. DO NOT select the first occurrence blindly.
    3. Prefer the table whose name semantically matches the rule context.
    4. Only choose from the provided schema.

    Return ONLY valid JSON:

    {{"table":"<table_name>","column":"<column_name>"}}

    If not possible:
    NOT_POSSIBLE

    DATABASE SCHEMA:
    {schema_json}

    BUSINESS RULE:
    {rule}
    """

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    return response.choices[0].message.content.strip()

def build_sql_condition(table_name, column_name, functionality, expected=None):
    if not column_exists(table_name, column_name):
        raise ValueError("Invalid column")
    col = f"[{column_name}]"
    functionality=functionality.strip().lower()
    if functionality == "null":
        return f"{col} IS NULL"
    elif functionality == "like":
        if expected is None:
            raise ValueError("Expected value required for LIKE")
        return f"CAST({col} AS VARCHAR) LIKE '%{expected}%'"
    elif functionality == "not_like":
        if expected is None:
            raise ValueError("Expected value required for NOT LIKE")
        return f"{col} NOT LIKE '%{expected}%'"
    elif functionality == "specific_value":
        if expected is None:
            raise ValueError("Expected value required")
        if str(expected).isdigit():
            return f"{col} = {expected}"
        return f"{col} = '{expected}'"
    elif functionality == "start_with":
        return f"{col} LIKE '{expected}%'"
    elif functionality == "is_numeric":
        return f"TRY_CAST({col} AS FLOAT) IS NOT NULL"
    elif functionality == "is_not_numeric":
        return f"TRY_CAST({col} AS FLOAT) IS NULL"
    elif functionality == "not_contain_number":
        return f"{col} NOT LIKE '%[0-9]%'"
    elif functionality == "special_character":
        return f"{col} LIKE '%[^a-zA-Z0-9 ]%'"
    elif functionality == "is_lower":
        return f"{col} = LOWER({col})"
    elif functionality == "fixed_length":
        if expected is None:
            raise ValueError("Expected length required")
        return f"LEN(CONCAT({col}, '')) = {expected}"
    elif functionality == "ranged_length":
        if expected is None or "," not in expected:
            raise ValueError("Expected must be in 'min,max' format")
        min_len, max_len = expected.split(",")
        return f"LEN({col}) BETWEEN {min_len} AND {max_len}"
    elif functionality == "date_format":
        return f"TRY_CONVERT(date, {col}, 112) IS NOT NULL"
    elif functionality == "length":
        limit = expected if expected else "10"
        return f"LEN(CONCAT({col}, '')) < {limit}"
    elif functionality.strip().lower() == "duplicity":
        return f"""
                {col} IN (
                    SELECT {col}
                    FROM {table_name}
                    GROUP BY {col}
                    HAVING COUNT(*) > 1
                )
                """
    else:
        raise ValueError(f"Unsupported functionality: {functionality}")

def build_final_query(table_name, column_name, functionality, expected=None):
       
    valid_tables = [t.lower() for t in get_table_names()]
    if table_name.lower() not in valid_tables:
        raise ValueError("Invalid table")
    condition = build_sql_condition(table_name,column_name, functionality, expected)
    return f"""
    SELECT *
    FROM [{table_name}]
    WHERE {condition}
    """

def sanitize_llm_sql(sql: str) -> str:
    sql = sql.strip()

    sql = sql.replace("```sql", "").replace("```", "").strip()

    upper_sql = sql.upper()
    if "SELECT" in upper_sql:
        sql = sql[upper_sql.index("SELECT"):]

    sql = sql.rstrip(";")

    return sql.strip()

def is_safe_select(sql: str) -> bool:
    forbidden = ["DELETE", "UPDATE", "INSERT", "DROP", "ALTER", "TRUNCATE", "CREATE", "MERGE", "EXEC", "EXECUTE", ";"]
    sql_upper = sql.upper()

    if not sql_upper.startswith("SELECT"):
        return False

    return not any(word in sql_upper for word in forbidden)

def log_dq_query(
    table_name,
    column_name,
    sql_query,
    status,
    row_count=None
):
    try:
        temp_conn = get_connection()
        cursor = temp_conn.cursor()
        cursor.execute("""
            INSERT INTO dq_query_log
            (table_name, column_name, sql_query, executed_at, status, row_count)
            VALUES (?, ?, ?, GETDATE(), ?, ?)
        """, (
            table_name,
            column_name,
            sql_query,
            status,
            row_count
        ))
        temp_conn.commit()
        temp_conn.close()
    except Exception as e:
        print("⚠ DQ log insert failed:", e)

def log_analytics_query(
    table_name,
    column_name,
    prompt,
    sql_query,
    status,
    row_count=None
):
    try:
        temp_conn = get_connection()
        cursor = temp_conn.cursor()
        cursor.execute("""
            INSERT INTO dq_analytics_log
            (table_name, column_name,prompt, sql_query, executed_at, status, row_count)
            VALUES (?, ?, ?, ?, GETDATE(), ?, ?)
        """, (
            table_name,
            column_name,
            prompt,
            sql_query,
            status,
            row_count
        ))
        temp_conn.commit()
    except Exception as e:
        app.logger.error(f"Analytics log insert failed: {e}")
    finally:
        temp_conn.close()

def add_new_rule_helper(table_name: str, column_name: str, business_rule: str, dimension: str) -> str:
    temp_conn = get_connection()
    cursor = temp_conn.cursor()
    table_prefix = table_name.upper()[:4]

    cursor.execute("SELECT Rule_Number FROM Master_Rules WHERE Table_Name = ?", (table_name,))
    existing = cursor.fetchall()
    
    if existing:
        # Extract the numeric part after the underscore and increment
        nums = [float(r[0].split('_')[1]) for r in existing if '_' in r[0]]
        next_num = round(max(nums) + 0.1, 1) if nums else 1.1
    else:
        next_num = 1.1

    rule_no = f"{table_prefix}_{next_num:.1f}"
    try:
        prompt = f"""
        Classify the data quality rule into ONE keyword only based on rule type violation:
        null, duplicity, fixed_length, date_format, ranged_length, is_lower, special_character, not_contain_number, is_not_numeric, is_numeric, start_with, specific_value, not_like, like
        Rule:
        {business_rule}
        """
        resp = client.chat.completions.create(
            model="llama3-8b-8192",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        functionality = resp.choices[0].message.content.strip().lower()
        functionality = functionality.replace(" ", "_").replace("-", "_")
    except Exception:
        functionality = "custom_rule"

    try:
        prompt = f"""
        Classify the data quality rule into ONE keyword only based on the rule type violation:
        completeness, conformity, duplicity, inactivity
        Rule:
        {business_rule}
        """
        resp = client.chat.completions.create(
            model="llama3-8b-8192",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        dimension = resp.choices[0].message.content.strip().lower()
        dimension = dimension.replace(" ", "_").replace("-", "_")
    except Exception:
        dimension = "custom"

    # Get datatype
    cursor.execute("""
        SELECT DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME=? AND COLUMN_NAME=?
    """, (table_name, column_name))
    row = cursor.fetchone()
    datatype = row[0] if row else "unknown"

    attribute_group = f"{table_name} details"

    cursor.execute("""
        INSERT INTO Master_Rules
        (Rule_Number, Table_Name, Column_Name, Business_Rule_Definition, Functionality, Data_Type, Attribute_Group, Dimension)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (rule_no, table_name, column_name, business_rule, functionality, datatype, attribute_group, dimension))
    temp_conn.commit()
    temp_conn.close()
    return rule_no

def get_categories():
    return FAQ_DF["category"].str.strip().unique().tolist()

def get_questions_by_category(category):
    df = FAQ_DF[(FAQ_DF["category"] == category) & (FAQ_DF["level"] == 1)]

    grouped = {}
    for _, row in df.iterrows():
        sub = row["sub_category"]
        grouped.setdefault(sub, []).append({
            "question": row["question"],
            "id": row.get("id", "") # Useful if you have a unique ID for children
        })
    return grouped

def get_faq_answer(question):
    if not question or len(question.strip()) < 3:
        return None

    q = question.strip().lower()

    exact = FAQ_DF[FAQ_DF["question"].str.lower().str.strip() == q]
    if not exact.empty:
        return exact.iloc[0]["answer"]

    for index, row in FAQ_DF.iterrows():
        if row["question"].lower() in q or q in row["question"].lower():
            return row["answer"]

    return None


def auto_suggest(text):
    matches = FAQ_DF[FAQ_DF["question"].str.contains(text, case=False)]
    return matches["question"].head(5).tolist()

def llm_fallback(question):
    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are Gia, the Genesis Data Quality Portal Assistant. "
                        "Answer ONLY about portal usage, Data Quality Control Centre, "
                        "Power BI reports, and DQ rules. Keep it professional and concise. "
                        "If asked about anything else, say: 'I specialize in Genesis Portal queries. Please contact the administrator for other matters.'"
                    )
                },
                {"role": "user", "content": question}
            ],
            temperature=0.2
        )

        return completion.choices[0].message.content

    except Exception as e:
        print("❌ GROQ ERROR:", e)
        return "I'm having trouble thinking right now. Please try choosing a predefined question or contact support."


def log_chat(question, source, response):
    try:
        # It is better to get a fresh connection for logging to avoid 'Link Failure'
        temp_conn = get_connection()
        cursor = temp_conn.cursor()

        cursor.execute("""
            INSERT INTO ChatbotLogs ([Timestamp], Question, AnswerSource, Answer)
            VALUES (?, ?, ?, ?)
        """, (
            datetime.now(),
            str(question),
            str(source),
            str(response)
        ))

        temp_conn.commit()
        temp_conn.close()
        print("Chat log inserted successfully.")
    except Exception as e:
        app.logger.error(f"Chat log failed: {e}")
def generate_temp_password():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=10))

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            flash("Please login first")
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def get_user_acronym(username):
    parts = username.split()
    if len(parts) == 1:
        return username[:2].upper()
    return (parts[0][0] + parts[1][0]).upper()

def safe_cursor():
    conn = get_connection()
    return conn, conn.cursor()

def cleanup_otp_store():
    now = datetime.now()
    expired = [k for k, v in otp_store.items() if v.get("expires") < now]
    for k in expired:
        del otp_store[k]

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if session.get('role') != 'admin':
            flash("Admin access required")
            return redirect(url_for('welcome'))
        return f(*args, **kwargs)
    return decorated_function

@app.context_processor
def inject_user():
    username = session.get('user')
    if username:
        return {
            "current_user": username,
            "user_acronym": get_user_acronym(username),
            "role": session.get("role"),
            "user_email":session.get("email")
        }
    return {}

@app.route('/')
def landing():
    return render_template('landing.html')
@app.route('/welcome')
@login_required
def welcome():
    return render_template('welcome.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        temp_conn = get_connection()
        cursor = temp_conn.cursor()

        cursor.execute(
        "SELECT Username, email, password_hash, must_reset_password, role FROM portal_users WHERE Username = ?",
        (username,)
        )

        user = cursor.fetchone()

        if user is None:
            flash("User does not exist")
            return render_template('login.html')

        print("USER DATA:", user)

        # if not check_password_hash(user.password, password):
        if not check_password_hash(user.password_hash, password):
            flash("Incorrect password")
            return render_template('login.html')

        if user.must_reset_password:
            session['user'] = user.Username
            session['email'] = user.email
            return redirect('/reset-password')

        session['user'] = user.Username
        session['role'] = user.role
        session['email'] = user.email

        return redirect(url_for('welcome'))
    return render_template('login.html')
@app.route('/reset-password', methods=['GET', 'POST'])
def reset_password():
    if 'user' not in session:
        return redirect(url_for('login'))

    if request.method == 'POST':
        new_password = request.form['password']

        hashed = generate_password_hash(new_password)

        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE portal_users
            SET password_hash = ?, must_reset_password = 0
            WHERE Username = ?
        """, (hashed, session['user']))

        conn.commit()
        conn.close()

        flash("Password updated successfully")
        return redirect(url_for('welcome'))

    return render_template("reset_password.html")

@app.route('/request_access', methods=['POST'])
def request_access():
    try:
        name = request.form['name']
        email = request.form['email']
        phone = request.form['phone']
        org = request.form['organization']
        dept = request.form['department']

        temp_conn = get_connection()
        cursor = temp_conn.cursor()

        cursor.execute("""
            INSERT INTO user_requests (name, email, phone, organization, department)
            VALUES (?, ?, ?, ?, ?)
        """, (name, email, phone, org, dept))

        temp_conn.commit()
        temp_conn.close()
        print("✅ INSERT SUCCESS")

        return jsonify({"status": "success"})
    except Exception as e:
        print("❌ DB ERROR:", e)
        return jsonify({"status": "error", "message": str(e)})

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/admin/requests')
@admin_required
def admin_requests():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM user_requests WHERE status='pending'")
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]

    data = [dict(zip(columns, r)) for r in rows]

    return render_template("admin_requests.html", requests=data)

@app.route('/admin')
@admin_required
def admin_dashboard():
    return render_template('admin_dashboard.html')

@app.route('/approve_user', methods=['POST'])
@admin_required
def approve_user():
    data = request.get_json()
    request_id = data.get("id")
    role = data.get("role")  # default role

    create_user_from_request(request_id, role)

    return jsonify({"status": "success"})

# @app.route('/admin/approve_request/<int:req_id>', methods=['POST'])
# def approve_request(req_id):
#     conn = get_connection()
#     cursor = conn.cursor()

#     cursor.execute("SELECT * FROM user_requests WHERE id=?", (req_id,))
#     user = cursor.fetchone()

#     if not user:
#         return jsonify({"status": "error", "message": "Request not found"})

#     temp_password = generate_temp_password()
#     hashed_password = generate_password_hash(temp_password)

#     role = request.json.get("role", "user")

#     cursor.execute("""
#         INSERT INTO Users (Username, Email, password_hash, Role, must_reset_password)
#         VALUES (?, ?, ?, ?, ?)
#     """, (user.name, user.email, hashed_password, role, 1))

#     cursor.execute("UPDATE user_requests SET status='approved' WHERE id=?", (req_id,))

#     conn.commit()
#     conn.close()

#     send_email(user.email, temp_password)

#     return jsonify({"status": "success"})

# @app.route('/admin/users')
# @admin_required
# def admin_users():
#     conn = get_connection()
#     cursor = conn.cursor()

#     cursor.execute("SELECT Username, Email, Role FROM portal_users")
#     users = cursor.fetchall()

#     return render_template("admin_users.html", users=users)

@app.route('/admin/logs')
@admin_required
def admin_logs():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT TOP 100 * FROM dq_query_log ORDER BY executed_at DESC")
    dq_logs = cursor.fetchall()

    return render_template("admin_logs.html", logs=dq_logs)

@app.route('/signup', methods=['GET','POST'])
def signup():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        confirm = request.form['confirm_password']
        email = request.form['email']

        if password != confirm:
            flash("Passwords do not match")
            return redirect(url_for('signup_page'))

        temp_conn = get_connection()
        cursor = temp_conn.cursor()
        

        # Check user exists
        cursor.execute("SELECT * FROM portal_users WHERE Username=? OR Email=?", (username, email))
        if cursor.fetchone():
            flash("User or Email already exists")
            return redirect(url_for('signup_page'))

        # Generate OTP
        otp = str(random.randint(100000, 999999))

        otp_store_signup[username] = {
            "otp": otp,
            "email": email,
            "password": password
        }

        # Send Mail
        msg = Message(
            "Signup OTP - Genesis DCC",
            sender=app.config['MAIL_USERNAME'],
            recipients=[email]
        )
        msg.body = f"Congratulations! \n You have successfullly created your account on Genesis Data Control Centre. Your signup OTP is: {otp} \n Enjoy exploring the portal. \n Thanks for choosing us."

        mail.send(msg)

        return jsonify({"status": "otp_sent"})
    
    return render_template('signup.html')

@app.route('/send_otp', methods=['POST'])
def send_otp():
    username = request.json.get('username')

    temp_conn = get_connection()
    cursor = temp_conn.cursor()
    
    cursor.execute("SELECT Email FROM portal_users WHERE Username = ?", (username,))
    user = cursor.fetchone()

    if not user:
        return jsonify({"status": "error", "message": "User not found"})

    email = user.Email

    otp = str(random.randint(100000, 999999))
    otp_store[username] = {
    "otp": otp,
    "expires": datetime.now() + timedelta(minutes=5)}

    msg = Message(
        sender=app.config['MAIL_USERNAME'],
        recipients=[email],
        subject=f"Your OTP for Genesis DCC Portal Password Reset"
    )

    msg.body = f"""Hello {username},

Your One-Time Password (OTP) for verification is:

🔢 {otp}

This code is valid for the next 5 minutes.

If you did not request this, please ignore this email — your account is safe.

For security reasons:
• Do not share this code with anyone
• Our team will never ask for your OTP

 Genesis Data Control Centre (DCC)"""

    try:
        mail.send(msg)
    except Exception as e:
        print("MAIL ERROR:", e)
        return jsonify({"status": "error", "message": "Failed to send OTP"})
    return jsonify({"status": "success", "message": "OTP sent to your email"})
    # return jsonify({"status": "success"})

@app.route('/verify_otp', methods=['POST'])
def verify_otp():
    username = request.json.get('username')
    otp = request.json.get('otp')
    new_password = request.json.get('password')

    temp_conn = get_connection()
    cursor = temp_conn.cursor()

    if username not in otp_store:
        return jsonify({"status": "error", "message": "OTP not found"})

    stored_otp = otp_store[username]

# Check expiry
    if datetime.now() > stored_otp["expires"]:
        del otp_store[username]
        return jsonify({"status": "error", "message": "OTP expired"})

    # Check match
    if stored_otp["otp"] != otp:
        return jsonify({"status": "error", "message": "Invalid OTP"})
    hashed_password = generate_password_hash(new_password)
    
    cursor.execute("SELECT * FROM portal_users WHERE Username = ?", (username,))
    user = cursor.fetchone()


    if not user:
        return jsonify({"status": "error", "message": "User not found"})
    
    temp_conn = get_connection()
    cursor = temp_conn.cursor()
    cursor.execute(
        """UPDATE portal_users 
        SET password_hash = ?, password = ?, must_reset_password = 0
        WHERE Username = ?""",
        (hashed_password, new_password, username)
    )
    temp_conn.commit()

    del otp_store[username]

    return jsonify({"status": "success"})

@app.route('/verify_signup_otp', methods=['POST'])
def verify_signup_otp():
    data = request.json
    username = data.get("username")
    otp = data.get("otp")

    if username not in otp_store_signup:
        return jsonify({"status": "error", "message": "Session expired"})

    if otp_store_signup[username]["otp"] != otp:
        return jsonify({"status": "error", "message": "Invalid OTP"})

    # Create user
    email = otp_store_signup[username]["email"]
    password = otp_store_signup[username]["password"]

    hashed_password = generate_password_hash(password)

    temp_conn = get_connection()
    cursor = temp_conn.cursor()

    cursor.execute("""
        INSERT INTO portal_users 
        (Username, Email, password_hash, role, must_reset_password)
        VALUES (?, ?, ?, 'user', 1)""" )
    temp_conn.commit()

    del otp_store_signup[username]

    return jsonify({"status": "success"})

@app.route('/account_details')
@login_required
def account_details():
    temp_conn = get_connection()
    cursor = temp_conn.cursor()

    cursor.execute("""
        SELECT Username, Email, Role
        FROM portal_users
        WHERE Username = ?
    """, (session['user'],))

    user = cursor.fetchone()

    return render_template('account_details.html', user=user)

@app.route('/help-center')
@login_required
def help_center():
    return render_template('help_center.html')

@app.route('/admin/reject_request/<int:req_id>', methods=['POST'])
def reject_request(req_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT email, name FROM user_requests WHERE id=?", (req_id,))
    user = cursor.fetchone()

    cursor.execute("UPDATE user_requests SET status='rejected' WHERE id=?", (req_id,))
    conn.commit()
    conn.close()

    # send rejection mail
    msg = Message(
        subject="Request Rejected - Genesis DCC",
        sender=app.config['MAIL_USERNAME'],
        recipients=[user.email]
    )

    msg.html = f"""
        <div style="max-width:600px;margin:auto;background:white;padding:30px;">
            <h2 style="color:#e74c3c;">Request Update</h2>

            <p>Hello {user.name},</p>

            <p>We regret to inform you that your request was not approved.</p>

            <p>If you believe this is incorrect, contact support.</p>

            <hr>
            <p style="font-size:12px;">Genesis DnAI Team</p>
        </div>
        """

    mail.send(msg)

    return jsonify({"status": "rejected"})

@app.route('/dataquality')
def dataquality():
    return render_template('dataquality.html', categories=get_categories())

@app.route('/raw-data', methods=['GET', 'POST'])
def raw_data():
    tables = get_table_names()
    data = None
    if request.method == 'POST':
        selected_table = request.form.get('table_name')
        # Safety check
        if selected_table not in tables:
            flash('Invalid table selected!', 'danger')
            return redirect(url_for('raw_data'))
        query = "SELECT * FROM [{}]".format(selected_table)
        temp_conn = get_connection()
        cursor = temp_conn.cursor()
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        data = {
            'columns': columns,
            'rows': rows
        }
    return render_template(
        'raw_data.html',
        tables=tables,
        data=data
    )

@app.route('/run_query', methods=['POST'])
def run_query():
    try:
        data = request.get_json()
        table_name = data.get("table_name")
        column_name = data.get("column_name")
        functionality = data.get("functionality")
        expected = data.get("expected")
        if not table_name or not column_name or not functionality:
            return jsonify({
                            "status": "error",
                            "message" : "Missing required inputs"
                            }), 400
        sql_query = build_final_query(
            table_name,
            column_name,
            functionality,
            expected
        )
        temp_conn = get_connection()
        cursor = temp_conn.cursor()
        print(sql_query)
        cursor.execute(sql_query)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        rows = [list(row) for row in rows]
        log_dq_query(table_name=table_name,column_name=column_name,sql_query=sql_query,status="SUCCESS",row_count=len(rows))
        return jsonify({
            "source": "master",
            "sql": sql_query,
            "columns": columns,
            "rows": rows
        })
    except Exception as e:
        log_dq_query(table_name=data.get("table_name"), 
                     column_name=data.get("column_name"), 
                     sql_query=None, 
                     status="FAILED", 
                     row_count=0)
        return jsonify({"status":"error", "message": str(e)}), 500

@app.route('/query-data', methods=['GET'])
def query_data():
    return render_template('query_data.html')

@app.route("/submit_suggestion", methods=["POST"])
def submit_suggestion():
    data = request.get_json(silent=True)

    if not data:
        return jsonify({
            "status": "error",
            "message": "Invalid or missing JSON payload"
        }), 400


    table = data.get('table') or None
    column = data.get('column') or None
    rule = data.get('rule')
    timestamp = datetime.now()

    if not rule:
        return jsonify({
            "status": "error",
            "message": "Business rule is required."
        }), 400
    
    auto_detect = False

    if not table or not column:
        auto_detect = True
        schema = get_db_schema()
        filtered_schema = {}

        for table, cols in schema.items():
            table_lower = table.lower()
            if table_lower in ALLOWED_TABLES_MAP:
                canonical_name = ALLOWED_TABLES_MAP[table_lower]
                filtered_schema[canonical_name] = cols

        schema = filtered_schema

        context_table = extract_target_table(rule, schema)

        if context_table:
            print(f"🎯 Context-selected table: {context_table}")
            schema = {context_table: schema[context_table]}
        else:
            # 2️⃣ Fallback: direct entity name detection
            entity_table = extract_entity(rule)

            if entity_table and entity_table in schema:
                print(f"📌 Entity matched table directly: {entity_table}")
                schema = {entity_table: schema[entity_table]}
            else:
                # 3️⃣ Final fallback: ranking
                schema = rank_tables_by_rule(rule, schema)

        detection = detect_table_column_from_rule(rule, schema)
        print("Detection value:", detection)

        if detection == "NOT_POSSIBLE":
            log_dq_query(
                table_name=None,
                column_name=None,
                sql_query="NOT_POSSIBLE",
                status="NOT_POSSIBLE",
                row_count=0
            )
            return jsonify({
                "status": "not_possible",
                "message": "Unable to identify table and column for this rule."
            }), 400

        try:
            detected = extract_json_from_llm(detection)

            if detected is None:
                log_dq_query(
                    table_name=None,
                    column_name=None,
                    sql_query="NOT_POSSIBLE",
                    status="NOT_POSSIBLE",
                    row_count=0
                )
                return jsonify({
                    "status": "not_possible",
                    "message": "Unable to identify table and column."
                }), 400

            table = detected.get("table")
            column = detected.get("column")
            print("🔍 LLM TABLE/COLUMN DETECTION:")
            print(detected)
            print("-----------------------------")


        except Exception as e:
            print("RAW LLM RESPONSE:")
            print(detection)

            return jsonify({
                "status": "error",
                "message": "Failed to parse LLM response for table/column.",
                "raw_response": detection
            }), 500

    if not table_exists(table):
        return jsonify({
            "status": "error",
            "message": "Table does not exist in the database."
        }), 400

    if not column_exists(table, column):
        return jsonify({
            "status": "error",
            "message": "Column does not exist in the table."
        }), 400

    temp_conn = get_connection()
    cursor = temp_conn.cursor()
    cursor.execute("""
        INSERT INTO Suggestions (TableName, ColumnName, BusinessRule, Timestamp)
        VALUES (?, ?, ?, ?)""", (table, column, rule, timestamp))
    temp_conn.commit()
    prompt = f"""
        You are a senior SQL Server expert.

        Table: {table}
        Column: {column}

        Business rule:
        {rule}

        Generate a SQL Server query that finds rows violating this rule.

        Rules:
        - Output ONLY SQL
        - Use SELECT * FROM [{table}]
        - Use ONLY this table
        - NEVER use JOIN
        - utilise {schema} for table and column selection
        - NEVER use DELETE, UPDATE, DROP
        - If not possible, output exactly: NOT_POSSIBLE
        """

    try:
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You generate safe, read-only SQL Server queries only."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2
        )

        generated_sql = sanitize_llm_sql(
            response.choices[0].message.content.strip()
        )

        print("----- GROQ GENERATED SQL -----")
        print(generated_sql)
        print("------------------------------")

        if not is_safe_select(generated_sql):
            log_dq_query(
                table_name=table,
                column_name=column,
                sql_query=generated_sql,
                status="BLOCKED",
                row_count=0
            )
            return jsonify({
                "status": "blocked",
                "message": "Unsafe SQL generated and blocked.",
                "sql": generated_sql
            }), 400

        if generated_sql == "NOT_POSSIBLE":
            log_dq_query(
                table_name=table,
                column_name=column,
                sql_query="NOT_POSSIBLE",
                status="NOT_POSSIBLE",
                row_count=0
            )
            return jsonify({
                "status": "not_possible",
                "message": "Entered rule cannot be implemented as a query."
            })

        temp_conn = get_connection()
        cursor = temp_conn.cursor()
        cursor.execute(generated_sql)

        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        row_count = len(rows)

        log_dq_query(
            table_name=table,
            column_name=column,
            sql_query=generated_sql,
            status="SUCCESS",
            row_count=row_count
        )
        
        if data.get("save_rule") is True:
            rule_no = add_new_rule_helper(
                table_name=table,
                column_name=column,
                business_rule=rule,
                dimension=data.get("dimension", "completeness")
            )

        else:
            rule_no = None

        interpretation = f"There are {row_count} rows violating the rule."
        if row_count == 0:
            interpretation += " No violations found."

        return jsonify({
            "status": "implemented",
            "source": "llm",
            "message": interpretation,
            "rule_number":rule_no,
            "rule": rule,             
            "table": table,
            "column": column,
            "sql": generated_sql,
            "columns": columns,
            "rows": [list(r) for r in rows]
        })


    except pyodbc.Error as db_err:
        log_dq_query(
            table_name=table,
            column_name=column,
            sql_query=generated_sql if 'generated_sql' in locals() else None,
            status="FAILED",
            row_count=0
        )
        return jsonify({
            "status": "error",
            "message": "Database execution failed.",
            "details": str(db_err)
        }), 500

    except Exception as e:
        log_dq_query(
            table_name=table,
            column_name=column,
            sql_query=generated_sql if 'generated_sql' in locals() else None,
            status="FAILED",
            row_count=0
        )
        return jsonify({
            "status": "error",
            "message": "Unexpected error occurred.",
            "details": str(e)
        }), 500


@app.route("/dq-report")
def dq_report():
    return render_template("dq_report.html")

@app.route("/view-rules")
def view_rules():
    temp_conn = get_connection()
    cursor = temp_conn.cursor()
    cursor.execute("SELECT * FROM Master_Rules")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return render_template(
        "view_rules.html",
        columns=columns,
        rows=[list(r) for r in rows]
    )

@app.route("/get-master-rules")
def get_master_rules():
    temp_conn = get_connection()
    cursor = temp_conn.cursor()
    cursor.execute("SELECT * FROM Master_Rules")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    data = [dict(zip(columns, r)) for r in rows]
    return jsonify(data)

@app.route("/pbi-report")
def pbi_report():
    return render_template("pbi_report.html")


@app.route("/chatbot/ask", methods=["POST"])
def chatbot_ask():
    data = request.json
    question = data.get("question")
    # New: The frontend should send a flag if 'Others' mode is active
    is_others = data.get("isOthers", False) 

    print(f"DEBUG: Received Question: {question} | Others Mode: {is_others}")

    # 1. If 'Others' mode is on, skip FAQ and go straight to LLM for a smart response
    if is_others:
        llm_reply = llm_fallback(question)
        log_chat(question, "LLM_DIRECT", llm_reply)
        return jsonify({"reply": llm_reply})

    # 2. Otherwise, try a strict FAQ match first
    answer = get_faq_answer(question)
    if answer:
        print("Attempting to log now...") # Add this
        log_chat(question, "FAQ", answer)
        print("Log function finished.")
        return jsonify({"reply": answer})

    # 3. Fallback to LLM if FAQ fails
    llm_reply = llm_fallback(question)
    log_chat(question, "LLM_FALLBACK", llm_reply)
    return jsonify({"reply": llm_reply})

@app.route("/chatbot/questions", methods=["POST"])
def chatbot_questions():
    data = request.json
    category = data.get("category")
    parent_id = data.get("parent_id")

    try:
        # Check if user clicked a specific question/category (has parent_id)
        if parent_id:
            # Look for sub-questions (Level 2) belonging to this parent
            children = FAQ_DF[(FAQ_DF["category"] == category) & 
                              (FAQ_DF["parent_question"].astype(str) == str(parent_id)) & 
                              (FAQ_DF["level"] == 2)]
            
            if not children.empty:
                questions_list = [{"text": row["question"], "id": row["question_id"]} for _, row in children.iterrows()]
                # REMOVED: log_chat(row["question"], "FAQ", answer) - 'row' and 'answer' are undefined here
                return jsonify({"type": "questions", "data": questions_list})
            
            # If no children, it's a leaf node. Provide the actual answer.
            ans_row = FAQ_DF[FAQ_DF["question_id"].astype(str) == str(parent_id)]
            if not ans_row.empty:
                answer = ans_row["answer"].iloc[0]
                question_text = ans_row["question"].iloc[0]
                log_chat(question_text, "FAQ", answer) # Log the final answer provided
                return jsonify({"type": "answer", "data": answer})

        # Base case: Get top-level (Level 1) questions for a category
        level1 = FAQ_DF[(FAQ_DF["category"] == category) & (FAQ_DF["level"] == 1)]
        questions_list = [{"text": row["question"], "id": row["question_id"]} for _, row in level1.iterrows()]
        
        # REMOVED: log_chat here because we are just showing a list of options, not answering yet
        return jsonify({"type": "questions", "data": questions_list})
        
    except Exception as e:
        print(f"CHATBOT ERROR: {e}")
        return jsonify({"type": "answer", "data": "Error processing your request."}), 500

@app.route("/chatbot/suggest", methods=["POST"])
def chatbot_suggest():
    text = request.json.get("text")
    return jsonify(auto_suggest(text))

@app.route("/analytics", methods=["GET"]) 
def analytics(): 
    return render_template('analytics.html')
@app.route("/analytics_query", methods=["POST"])
def analytics_query():
    data = request.get_json(silent=True)

    if not data or not data.get('rule'):
        return jsonify({"status": "error", "message": "Analytics Rule is required."}), 400

    rule = data.get('rule')
    input_table = data.get('table')
    input_column = data.get('column')

    generated_sql = None
    detected_table = None
    detected_column = None

    try:
        # -------------------------------
        # 1. Get and Format Schema
        # -------------------------------
        full_schema = get_db_schema()

        formatted_lines = []
        for tbl, cols in full_schema.items():
            col_definitions = []
            for c in cols:
                if isinstance(c, dict):
                    name = c.get("column", "unknown")
                    dtype = c.get("type", "TEXT")
                    col_definitions.append(f"{name} {dtype}")
                else:
                    col_definitions.append(f"{str(c)} TEXT")

            formatted_lines.append(f"{tbl} ({', '.join(col_definitions)})")

        formatted_schema = "\n".join(formatted_lines)

        # -------------------------------
        # 2. Prompt
        # -------------------------------
        prompt = f"""
You are a SQL Server expert. Generate a SAFE T-SQL query.

DATABASE SCHEMA:
{formatted_schema}

USER RULE:
{rule}

INSTRUCTIONS:
1. Use ONLY tables and columns from schema.
2. Respect DATA TYPES.
3. ONLY generate a SELECT query.
4. LIMIT results using TOP 100.
5. DO NOT use INSERT, UPDATE, DELETE, DROP, ALTER, EXEC.
6. If unsure, return null values.
7. Return STRICT JSON format:
8. Please ensure that you use the exact table and column names as provided in the schema, including correct casing.
9. do not add any extra character like underscore or space in the table and column names. use them exactly as they are in the schema.
10. pack each column names in square brackets in the generated SQL query, for example [ColumnName].

{{
  "identified_table": "...",
  "identified_column": "...",
  "sql": "..."
}}
"""

        # -------------------------------
        # 3. LLM Call
        # -------------------------------
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "Return ONLY valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1
        )

        raw_content = response.choices[0].message.content.strip()

        # -------------------------------
        # 4. Robust JSON Extraction
        # -------------------------------
        import re
        match = re.search(r'\{.*\}', raw_content, re.DOTALL)
        if not match:
            raise ValueError("Invalid JSON response from LLM")

        parsed = json.loads(match.group(0))

        generated_sql = parsed.get("sql")
        detected_table = parsed.get("identified_table")
        detected_column = parsed.get("identified_column")

        if not generated_sql:
            raise ValueError("LLM did not return SQL")

        print("Generated SQL:", generated_sql)

        # -------------------------------
        # 5. SQL Safety Check
        # -------------------------------
        def is_safe_select(query):
            forbidden = ["insert", "update", "delete", "drop", "alter", "exec", ";"]
            q = query.lower()
            return q.strip().startswith("select") and not any(f in q for f in forbidden)

        if not is_safe_select(generated_sql):
            raise ValueError("Unsafe SQL detected")

        # -------------------------------
        # 6. Strict Table Validation
        # -------------------------------
        valid_tables = [t.lower() for t in full_schema.keys()]

        if not any(f" {t} " in generated_sql.lower() or f" {t}\n" in generated_sql.lower()
                   for t in valid_tables):
            raise ValueError("Query references invalid table")

        # -------------------------------
        # 7. Execute Query (Safe)
        # -------------------------------
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(generated_sql)

            rows = cursor.fetchmany(100)  # hard cap
            columns = [col[0] for col in cursor.description]

        # -------------------------------
        # 8. Logging
        # -------------------------------
        log_analytics_query(
            table_name=input_table or detected_table,
            column_name=input_column or detected_column,
            prompt=rule,
            sql_query=generated_sql,
            status="SUCCESS",
            row_count=len(rows)
        )

        # -------------------------------
        # 9. Response
        # -------------------------------
        return jsonify({
            "status": "success",
            "table": input_table or detected_table,
            "column": input_column or detected_column,
            "sql": generated_sql,
            "columns": columns,
            "rows": [list(r) for r in rows]
        })

    except Exception as e:
        # -------------------------------
        # Error Logging (Safe)
        # -------------------------------
        log_analytics_query(
            table_name=input_table or detected_table,
            column_name=input_column or detected_column,
            prompt=rule,
            sql_query=generated_sql,
            status="FAILED",
            row_count=0
        )

        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route('/admin/users', methods=['GET', 'POST'])
@admin_required
def admin_users():
    tables = 'portal_users', 'user_requests', 'Suggestions', 'dq_query_log', 'Master_Rules'
    data = None
    if request.method == 'POST':
        selected_table = request.form.get('table_name')
        # Safety check
        if selected_table not in tables:
            flash('Invalid table selected!', 'danger')
            return redirect(url_for('admin_users'))
        query = f"SELECT * FROM {selected_table}"
        temp_conn = get_connection()
        cursor = temp_conn.cursor()
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        temp_conn.close()
        data = {
            'columns': columns,
            'rows': rows
        }
    return render_template(
        'admin_users.html',
        tables=tables,
        data=data
    )


if __name__ == '__main__':
    app.run(debug=True, port=5000)
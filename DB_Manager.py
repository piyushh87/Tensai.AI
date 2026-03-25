import os
import mysql.connector
from mysql.connector import Error
import hashlib
from datetime import datetime

# --- MYSQL CONFIGURATION ---
def _get_env(name, default=""):
    return str(os.getenv(name, default)).strip()


def _build_db_config():
    config = {
        "host": _get_env("DB_HOST", "localhost"),
        "user": _get_env("DB_USER", "root"),
        "password": _get_env("DB_PASSWORD", "Piyush$8799"),
        "database": _get_env("DB_NAME", "talent_os"),
    }
    raw_port = _get_env("DB_PORT")
    if raw_port:
        try:
            config["port"] = int(raw_port)
        except ValueError:
            print(f"Invalid DB_PORT value: {raw_port!r}")
    return config

def get_connection():
    """Establishes and returns a connection to the MySQL database."""
    try:
        conn = mysql.connector.connect(**_build_db_config())
        if conn.is_connected():
            return conn
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
    return None

def _extract_column_name(row):
    if isinstance(row, dict):
        return row.get("Field")
    return row[0] if row else None

def _get_table_columns(cursor, table_name):
    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
    return {name for name in (_extract_column_name(row) for row in cursor.fetchall()) if name}

def _merge_auth_provider(existing_provider, incoming_provider):
    providers = []
    for value in (existing_provider or "", incoming_provider or ""):
        for item in str(value).split(','):
            item = item.strip()
            if item and item not in providers:
                providers.append(item)
    return ",".join(providers) if providers else "email"

def _hash_otp_code(raw_code):
    return hashlib.sha256(str(raw_code or "").encode("utf-8")).hexdigest()

def ensure_platform_user_table(cursor):
    """Creates the authenticated Talent OS user table when needed and upgrades missing columns."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS platform_users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            google_sub VARCHAR(191) UNIQUE,
            email VARCHAR(100) NOT NULL UNIQUE,
            full_name VARCHAR(120),
            given_name VARCHAR(80),
            family_name VARCHAR(80),
            picture_url VARCHAR(255),
            contact_email VARCHAR(100),
            current_role VARCHAR(120),
            target_locations VARCHAR(255),
            primary_stack VARCHAR(255),
            locale VARCHAR(20),
            email_verified BOOLEAN DEFAULT FALSE,
            password_hash VARCHAR(255),
            otp_code VARCHAR(255),
            otp_expires_at DATETIME NULL,
            otp_requested_at DATETIME NULL,
            auth_provider VARCHAR(40) DEFAULT 'google',
            is_active BOOLEAN DEFAULT TRUE,
            last_login_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            login_count INT DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
    """)
    existing_columns = _get_table_columns(cursor, "platform_users")
    required_columns = {
        "password_hash": "ALTER TABLE platform_users ADD COLUMN password_hash VARCHAR(255) NULL AFTER email_verified",
        "otp_code": "ALTER TABLE platform_users ADD COLUMN otp_code VARCHAR(255) NULL AFTER password_hash",
        "otp_expires_at": "ALTER TABLE platform_users ADD COLUMN otp_expires_at DATETIME NULL AFTER otp_code",
        "otp_requested_at": "ALTER TABLE platform_users ADD COLUMN otp_requested_at DATETIME NULL AFTER otp_expires_at",
        "auth_provider": "ALTER TABLE platform_users ADD COLUMN auth_provider VARCHAR(40) DEFAULT 'google' AFTER otp_requested_at",
        "is_active": "ALTER TABLE platform_users ADD COLUMN is_active BOOLEAN DEFAULT TRUE AFTER auth_provider",
        "contact_email": "ALTER TABLE platform_users ADD COLUMN contact_email VARCHAR(100) NULL AFTER picture_url",
        "current_role": "ALTER TABLE platform_users ADD COLUMN current_role VARCHAR(120) NULL AFTER contact_email",
        "target_locations": "ALTER TABLE platform_users ADD COLUMN target_locations VARCHAR(255) NULL AFTER current_role",
        "primary_stack": "ALTER TABLE platform_users ADD COLUMN primary_stack VARCHAR(255) NULL AFTER target_locations",
    }
    for column_name, alter_sql in required_columns.items():
        if column_name not in existing_columns:
            cursor.execute(alter_sql)

def _split_name(full_name):
    safe_name = str(full_name or "").strip()
    if not safe_name:
        return "Talent OS User", None, None
    parts = safe_name.split()
    if len(parts) == 1:
        return safe_name[:120], parts[0][:80], None
    return safe_name[:120], parts[0][:80], " ".join(parts[1:])[:80]

def _normalize_platform_user_row(row):
    if not row:
        return None
    return {
        "id": row.get("id"),
        "google_sub": row.get("google_sub"),
        "email": str(row.get("email") or "").strip(),
        "full_name": str(row.get("full_name") or row.get("email") or "Talent OS User").strip(),
        "given_name": str(row.get("given_name") or "").strip(),
        "family_name": str(row.get("family_name") or "").strip(),
        "picture_url": str(row.get("picture_url") or "").strip(),
        "contact_email": str(row.get("contact_email") or "").strip(),
        "current_role": str(row.get("current_role") or "").strip(),
        "target_locations": str(row.get("target_locations") or "").strip(),
        "primary_stack": str(row.get("primary_stack") or "").strip(),
        "locale": str(row.get("locale") or "").strip(),
        "email_verified": bool(row.get("email_verified")),
        "auth_provider": str(row.get("auth_provider") or "email").strip(),
        "is_active": bool(row.get("is_active", True)),
        "login_count": int(row.get("login_count") or 0),
    }

def upsert_google_user(user_info):
    """Creates or updates a Google-authenticated Talent OS user."""
    conn = get_connection()
    if not conn:
        return {"success": False, "error": "Database connection failed"}

    cursor = None
    try:
        cursor = conn.cursor(dictionary=True)
        ensure_platform_user_table(cursor)

        safe_email = str(user_info.get('email', '')).strip()[:100]
        if not safe_email:
            return {"success": False, "error": "Google account email is required"}

        safe_google_sub = str(user_info.get('sub', '')).strip()[:191] or None
        safe_name = str(user_info.get('name') or user_info.get('given_name') or 'Talent OS User').strip()[:120]
        safe_given = str(user_info.get('given_name', '')).strip()[:80] or None
        safe_family = str(user_info.get('family_name', '')).strip()[:80] or None
        safe_picture = str(user_info.get('picture', '')).strip()[:255] or None
        safe_locale = str(user_info.get('locale', '')).strip()[:20] or None
        verified_email = 1 if user_info.get('email_verified') else 0

        cursor.execute(
            "SELECT id, google_sub, auth_provider FROM platform_users WHERE google_sub = %s OR email = %s LIMIT 1",
            (safe_google_sub, safe_email)
        )
        existing_user = cursor.fetchone()

        if existing_user:
            resolved_google_sub = safe_google_sub or existing_user.get('google_sub')
            merged_provider = _merge_auth_provider(existing_user.get('auth_provider'), 'google')
            cursor.execute("""
                UPDATE platform_users
                SET google_sub = %s,
                    email = %s,
                    full_name = %s,
                    given_name = %s,
                    family_name = %s,
                    picture_url = %s,
                    locale = %s,
                    email_verified = %s,
                    auth_provider = %s,
                    is_active = 1,
                    last_login_at = NOW(),
                    login_count = login_count + 1
                WHERE id = %s
            """, (
                resolved_google_sub,
                safe_email,
                safe_name,
                safe_given,
                safe_family,
                safe_picture,
                safe_locale,
                verified_email,
                merged_provider,
                existing_user['id']
            ))
            user_id = existing_user['id']
            is_new_user = False
        else:
            cursor.execute("""
                INSERT INTO platform_users (
                    google_sub,
                    email,
                    full_name,
                    given_name,
                    family_name,
                    picture_url,
                    locale,
                    email_verified,
                    auth_provider,
                    is_active,
                    last_login_at,
                    login_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'google', 1, NOW(), 1)
            """, (
                safe_google_sub,
                safe_email,
                safe_name,
                safe_given,
                safe_family,
                safe_picture,
                safe_locale,
                verified_email
            ))
            user_id = cursor.lastrowid
            is_new_user = True

        conn.commit()
        cursor.execute("SELECT * FROM platform_users WHERE id = %s LIMIT 1", (user_id,))
        refreshed_user = cursor.fetchone()
        return {
            "success": True,
            "user_id": user_id,
            "is_new_user": is_new_user,
            "user": _normalize_platform_user_row(refreshed_user),
        }

    except Error as e:
        conn.rollback()
        print(f"Failed to upsert Google user: {e}")
        return {"success": False, "error": str(e)}
    finally:
        if cursor:
            cursor.close()
        if conn.is_connected():
            conn.close()

def begin_email_signup(full_name, email, password_hash, otp_code, otp_expires_at):
    """Creates or refreshes an email-signup record and stores a fresh OTP hash."""
    conn = get_connection()
    if not conn:
        return {"success": False, "error": "Database connection failed"}

    cursor = None
    try:
        cursor = conn.cursor(dictionary=True)
        ensure_platform_user_table(cursor)

        safe_email = str(email or "").strip().lower()[:100]
        if not safe_email:
            return {"success": False, "error": "Email is required"}

        safe_name, safe_given, safe_family = _split_name(full_name or safe_email.split("@")[0])
        otp_hash = _hash_otp_code(otp_code)

        cursor.execute("SELECT * FROM platform_users WHERE email = %s LIMIT 1", (safe_email,))
        existing_user = cursor.fetchone()

        if existing_user:
            existing_provider = str(existing_user.get("auth_provider") or "")
            if existing_user.get("email_verified") and existing_user.get("is_active") and existing_user.get("password_hash"):
                return {"success": False, "error": "An account with this email already exists. Please log in."}
            if "google" in existing_provider.split(",") and not existing_user.get("password_hash"):
                return {"success": False, "error": "This email is already linked to Google sign-in. Use Google login for this account."}

            merged_provider = _merge_auth_provider(existing_provider, "email")
            cursor.execute("""
                UPDATE platform_users
                SET full_name = %s,
                    given_name = %s,
                    family_name = %s,
                    password_hash = %s,
                    otp_code = %s,
                    otp_expires_at = %s,
                    otp_requested_at = NOW(),
                    email_verified = 0,
                    auth_provider = %s,
                    is_active = 0
                WHERE id = %s
            """, (
                safe_name,
                safe_given,
                safe_family,
                password_hash,
                otp_hash,
                otp_expires_at,
                merged_provider,
                existing_user["id"]
            ))
            user_id = existing_user["id"]
            is_new_user = False
        else:
            cursor.execute("""
                INSERT INTO platform_users (
                    google_sub,
                    email,
                    full_name,
                    given_name,
                    family_name,
                    picture_url,
                    locale,
                    email_verified,
                    password_hash,
                    otp_code,
                    otp_expires_at,
                    otp_requested_at,
                    auth_provider,
                    is_active,
                    last_login_at,
                    login_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, 0, %s, %s, %s, NOW(), 'email', 0, NOW(), 0)
            """, (
                None,
                safe_email,
                safe_name,
                safe_given,
                safe_family,
                None,
                None,
                password_hash,
                otp_hash,
                otp_expires_at
            ))
            user_id = cursor.lastrowid
            is_new_user = True

        conn.commit()
        return {"success": True, "user_id": user_id, "is_new_user": is_new_user}

    except Error as e:
        conn.rollback()
        print(f"Failed to begin email signup: {e}")
        return {"success": False, "error": str(e)}
    finally:
        if cursor:
            cursor.close()
        if conn.is_connected():
            conn.close()

def verify_email_signup_otp(email, otp_code):
    """Verifies the email OTP and activates the pending email account."""
    conn = get_connection()
    if not conn:
        return {"success": False, "error": "Database connection failed"}

    cursor = None
    try:
        cursor = conn.cursor(dictionary=True)
        ensure_platform_user_table(cursor)

        safe_email = str(email or "").strip().lower()[:100]
        cursor.execute("SELECT * FROM platform_users WHERE email = %s LIMIT 1", (safe_email,))
        user = cursor.fetchone()
        if not user:
            return {"success": False, "error": "No pending signup found for this email."}

        if user.get("email_verified") and user.get("is_active"):
            return {"success": False, "error": "This account is already verified. Please log in."}

        if not user.get("otp_code") or not user.get("otp_expires_at"):
            return {"success": False, "error": "No valid OTP request was found. Please request a new code."}

        if _hash_otp_code(otp_code) != str(user.get("otp_code")):
            return {"success": False, "error": "Invalid OTP. Please check the code and try again."}

        if user.get("otp_expires_at") and datetime.utcnow() > user.get("otp_expires_at"):
            return {"success": False, "error": "OTP expired. Please request a new code."}

        cursor.execute("""
            UPDATE platform_users
            SET email_verified = 1,
                is_active = 1,
                otp_code = NULL,
                otp_expires_at = NULL,
                otp_requested_at = NULL,
                last_login_at = NOW(),
                login_count = login_count + 1
            WHERE id = %s
        """, (user["id"],))
        conn.commit()

        cursor.execute("SELECT * FROM platform_users WHERE id = %s LIMIT 1", (user["id"],))
        verified_user = cursor.fetchone()
        return {"success": True, "user": _normalize_platform_user_row(verified_user)}

    except Error as e:
        conn.rollback()
        print(f"Failed to verify email OTP: {e}")
        return {"success": False, "error": str(e)}
    finally:
        if cursor:
            cursor.close()
        if conn.is_connected():
            conn.close()

def authenticate_email_user(email, password_checker):
    """Authenticates an email/password user and returns the normalized user record."""
    conn = get_connection()
    if not conn:
        return {"success": False, "error": "Database connection failed"}

    cursor = None
    try:
        cursor = conn.cursor(dictionary=True)
        ensure_platform_user_table(cursor)

        safe_email = str(email or "").strip().lower()[:100]
        cursor.execute("SELECT * FROM platform_users WHERE email = %s LIMIT 1", (safe_email,))
        user = cursor.fetchone()

        if not user or not user.get("password_hash"):
            return {"success": False, "error": "No email account found for this address."}
        if not user.get("email_verified") or not user.get("is_active"):
            return {"success": False, "error": "Please verify your email before logging in."}
        if not password_checker(user.get("password_hash")):
            return {"success": False, "error": "Incorrect email or password."}

        cursor.execute("""
            UPDATE platform_users
            SET last_login_at = NOW(),
                login_count = login_count + 1
            WHERE id = %s
        """, (user["id"],))
        conn.commit()

        cursor.execute("SELECT * FROM platform_users WHERE id = %s LIMIT 1", (user["id"],))
        refreshed_user = cursor.fetchone()
        return {"success": True, "user": _normalize_platform_user_row(refreshed_user)}

    except Error as e:
        conn.rollback()
        print(f"Failed to authenticate email user: {e}")
        return {"success": False, "error": str(e)}
    finally:
        if cursor:
            cursor.close()
        if conn.is_connected():
            conn.close()

def update_platform_user_profile(
    email,
    full_name,
    picture_url=None,
    contact_email=None,
    current_role=None,
    target_locations=None,
    primary_stack=None,
):
    """Updates the editable profile fields for an authenticated Talent OS user."""
    conn = get_connection()
    if not conn:
        return {"success": False, "error": "Database connection failed"}

    cursor = None
    try:
        cursor = conn.cursor(dictionary=True)
        ensure_platform_user_table(cursor)

        safe_email = str(email or "").strip().lower()[:100]
        if not safe_email:
            return {"success": False, "error": "Email is required"}

        cursor.execute("SELECT * FROM platform_users WHERE email = %s LIMIT 1", (safe_email,))
        user = cursor.fetchone()
        if not user:
            return {"success": False, "error": "No account found for this profile."}

        safe_name, safe_given, safe_family = _split_name(full_name or user.get("full_name") or safe_email.split("@")[0])
        safe_picture = str(picture_url or "").strip()[:255] or None
        safe_contact_email = str(contact_email or "").strip().lower()[:100] or None
        safe_current_role = str(current_role or "").strip()[:120] or None
        safe_target_locations = str(target_locations or "").strip()[:255] or None
        safe_primary_stack = str(primary_stack or "").strip()[:255] or None

        cursor.execute("""
            UPDATE platform_users
            SET full_name = %s,
                given_name = %s,
                family_name = %s,
                picture_url = %s,
                contact_email = %s,
                current_role = %s,
                target_locations = %s,
                primary_stack = %s
            WHERE id = %s
        """, (
            safe_name,
            safe_given,
            safe_family,
            safe_picture,
            safe_contact_email,
            safe_current_role,
            safe_target_locations,
            safe_primary_stack,
            user["id"]
        ))
        conn.commit()

        cursor.execute("SELECT * FROM platform_users WHERE id = %s LIMIT 1", (user["id"],))
        refreshed_user = cursor.fetchone()
        return {"success": True, "user": _normalize_platform_user_row(refreshed_user)}

    except Error as e:
        conn.rollback()
        print(f"Failed to update platform user profile: {e}")
        return {"success": False, "error": str(e)}
    finally:
        if cursor:
            cursor.close()
        if conn.is_connected():
            conn.close()

def save_candidate_data(job_title, job_description, candidate_data, analysis_data):
    """Saves job context, candidate profile, and match results to the MySQL tables."""
    conn = get_connection()
    if not conn:
        return {"success": False, "error": "Database connection failed"}
        
    try:
        cursor = conn.cursor()
        
        # 1. Insert Job Role context
        cursor.execute(
            "INSERT INTO jobs (job_title, job_description) VALUES (%s, %s)", 
            (job_title, job_description)
        )
        job_id = cursor.lastrowid
        
        # 2. Insert Candidate Information
        skills_str = candidate_data.get('skills', '')
        if isinstance(skills_str, list):
            skills_str = ", ".join(skills_str)
            
        # --- FIX: Truncate values to fit MySQL column limits safely ---
        safe_name = str(candidate_data.get('name', 'Unknown'))[:100]
        safe_email = str(candidate_data.get('email', 'N/A'))[:100]
        safe_exp = str(candidate_data.get('total_experience_years', 'N/A'))[:50]
        safe_edu = str(candidate_data.get('education', 'N/A'))[:100]
            
        cursor.execute("""
            INSERT INTO candidates (name, email, skills, experience, education, resume_score) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            safe_name, 
            safe_email, 
            skills_str, 
            safe_exp, 
            safe_edu, 
            analysis_data.get('overall_score', 0)
        ))
        candidate_id = cursor.lastrowid
        
        # 3. Insert Relational Match Results
        cursor.execute("""
            INSERT INTO match_results (candidate_id, job_id, match_score, matched_skills, missing_skills)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            candidate_id, 
            job_id, 
            analysis_data.get('overall_score', 0), 
            ", ".join(analysis_data.get('matched_skills', [])), 
            ", ".join(analysis_data.get('missing_skills', []))
        ))
        
        conn.commit()
        return {"success": True, "candidate_id": candidate_id}
        
    except Error as e:
        conn.rollback()
        print(f"Failed to insert candidate data: {e}")
        return {"success": False, "error": str(e)}
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def save_chat_message(user_query, ai_response):
    """Saves user query and AI response to the chat_history table."""
    conn = get_connection()
    if not conn:
        return False
        
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO chat_history (user_query, ai_response) VALUES (%s, %s)", 
            (user_query, ai_response)
        )
        conn.commit()
        return True
    except Error as e:
        print(f"Failed to save chat history: {e}")
        return False
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def get_top_candidates(limit=5):
    """Retrieves the top candidates ranked by their AI match score for the leaderboard."""
    conn = get_connection()
    if not conn:
        return []
        
    try:
        cursor = conn.cursor()
        # Fetching candidate details along with the job they matched for
        cursor.execute("""
            SELECT c.name, c.email, c.skills, c.resume_score, j.job_title, m.match_score
            FROM candidates c
            LEFT JOIN match_results m ON c.id = m.candidate_id
            LEFT JOIN jobs j ON m.job_id = j.id
            ORDER BY c.resume_score DESC
            LIMIT %s
        """, (limit,))
        
        # Convert rows to a list of dictionaries so it's easy for Flask/HTML to use
        columns = [col[0] for col in cursor.description]
        candidates = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        return candidates
        
    except Error as e:
        print(f"Failed to fetch leaderboard data: {e}")
        return []
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# --- NEW FUNCTIONS FOR DATA PURGING & DELETION ---
def delete_candidate_by_name(name):
    """Deletes a candidate completely from the database."""
    conn = get_connection()
    if not conn: return False
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM candidates WHERE name = %s", (name,))
        conn.commit()
        return True
    except Error as e:
        print(f"Delete Error: {e}")
        return False
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def purge_database():
    """Wipes all candidates and match results from the database."""
    conn = get_connection()
    if not conn: return False
    try:
        cursor = conn.cursor()
        # Because of ON DELETE CASCADE, deleting candidates and jobs will wipe match_results too!
        cursor.execute("DELETE FROM candidates")
        cursor.execute("DELETE FROM jobs")
        conn.commit()
        return True
    except Error as e:
        print(f"Purge Error: {e}")
        return False
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
# ------------------------------------------------

# --- CONNECTION TEST SCRIPT ---
if __name__ == "__main__":
    print("Testing connection to MySQL Database 'talent_os'...")
    connection = get_connection()
    if connection:
        print("✅ SUCCESS! Connected to the database.")
        
        # Quick test to see if tables exist
        try:
            cur = connection.cursor()
            cur.execute("SHOW TABLES;")
            tables = cur.fetchall()
            print(f"✅ Found {len(tables)} tables in the database:")
            for table in tables:
                print(f"   - {table[0]}")
            cur.close()
            
            # Test our new function
            print("\nFetching Top Candidates from the database:")
            top_talents = get_top_candidates(limit=3)
            if top_talents:
                for idx, talent in enumerate(top_talents, 1):
                    print(f" #{idx} - {talent['name']} | Score: {talent['resume_score']} | Role: {talent['job_title']}")
            else:
                print(" No candidates found in the database yet.")
                
        except Exception as e:
            print(f"⚠️ Connected, but encountered an error: {e}")
            
        connection.close()
    else:
        print("❌ FAILED. Please check your MySQL server, username, password, and ensure 'talent_os' exists.")

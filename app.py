import io
import re
import sys
import json
import os
import smtplib
import ssl
from datetime import datetime, timedelta
from email.message import EmailMessage
from random import SystemRandom
from urllib.parse import urlparse
from uuid import uuid4


def load_local_env(filename=".env"):
    env_path = os.path.join(os.path.dirname(__file__), filename)
    if not os.path.exists(env_path):
        return

    with open(env_path, encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


load_local_env()

# Import the newly created chat engine
try:
    from chat_engine import (
        get_financial_answer,
        build_osint_bundle,
        run_openrouter_chat,
        stream_financial_answer,
        get_candidate_scorecard,
    )
except ImportError:
    get_financial_answer = None
    build_osint_bundle = None
    run_openrouter_chat = None
    stream_financial_answer = None
    get_candidate_scorecard = None

try:
    from resume_intelligence import analyze_resume_document
except ImportError:
    analyze_resume_document = None

# Import Database Manager
try:
    from DB_Manager import (
        save_candidate_data,
        save_chat_message,
        get_top_candidates,
        delete_candidate_by_name,
        purge_database,
        upsert_google_user,
        begin_email_signup,
        verify_email_signup_otp,
        authenticate_email_user,
        update_platform_user_profile,
    )
    DB_AVAILABLE = True
except ImportError as e:
    print(f"Database Module Error: {e}")
    DB_AVAILABLE = False

try:
    # Added redirect, url_for, and session for Google Login
    from flask import Flask, request, jsonify, render_template, redirect, url_for, session, Response, stream_with_context
    FLASK_AVAILABLE = True
except Exception:
    FLASK_AVAILABLE = False

# --- NEW: SECURITY & RATE LIMITING MODULES ---
try:
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address
    LIMITER_AVAILABLE = True
except ImportError:
    LIMITER_AVAILABLE = False

try:
    import pdfplumber
    PDFPLUMBER_AVAILABLE = True
except Exception:
    PDFPLUMBER_AVAILABLE = False

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    SKLEARN_AVAILABLE = True
except Exception:
    SKLEARN_AVAILABLE = False

# Import Authlib for Google OAuth
try:
    from authlib.integrations.flask_client import OAuth
    AUTHLIB_AVAILABLE = True
except ImportError:
    AUTHLIB_AVAILABLE = False

try:
    from werkzeug.security import generate_password_hash, check_password_hash
    WERKZEUG_SECURITY_AVAILABLE = True
except ImportError:
    WERKZEUG_SECURITY_AVAILABLE = False

try:
    from werkzeug.utils import secure_filename
    WERKZEUG_UTILS_AVAILABLE = True
except ImportError:
    WERKZEUG_UTILS_AVAILABLE = False

try:
    from PIL import Image, ImageOps
    PILLOW_AVAILABLE = True
except ImportError:
    PILLOW_AVAILABLE = False

EMAIL_REGEX = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$", re.IGNORECASE)
OTP_EXPIRY_MINUTES = 10
OTP_RANDOM = SystemRandom()
DEFAULT_PROFILE_NAME = "Netrunner 99"
DEFAULT_PROFILE_AVATAR = "/static/default-avatar.svg"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROFILE_AVATAR_FOLDER = os.path.join("static", "uploads", "avatars")
PROFILE_AVATAR_URL_PREFIX = "/static/uploads/avatars/"
MAX_PROFILE_AVATAR_BYTES = 5 * 1024 * 1024
PROFILE_AVATAR_OUTPUT_SIZE = 512


def clean_text(text):
    text = re.sub(r'[,/|]', ' ', text)
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    return text.lower().strip()


def serialize_google_user(user_info):
    if not user_info:
        return {}
    return {
        "sub": str(user_info.get("sub", "")).strip(),
        "name": str(user_info.get("name") or user_info.get("given_name") or "Tensai.AI User").strip(),
        "email": str(user_info.get("email", "")).strip(),
        "picture": str(user_info.get("picture", "")).strip(),
        "given_name": str(user_info.get("given_name", "")).strip(),
        "family_name": str(user_info.get("family_name", "")).strip(),
        "locale": str(user_info.get("locale", "")).strip(),
        "email_verified": bool(user_info.get("email_verified")),
        "auth_provider": "google",
        "contact_email": "",
        "current_role": "",
        "target_locations": "",
        "primary_stack": "",
    }

def serialize_platform_user(user_record):
    if not user_record:
        return {}
    full_name = str(user_record.get("full_name") or user_record.get("email") or "Tensai.AI User").strip()
    given_name = str(user_record.get("given_name") or "").strip()
    family_name = str(user_record.get("family_name") or "").strip()
    if not given_name and full_name:
        given_name = full_name.split()[0]
    if not family_name and full_name and len(full_name.split()) > 1:
        family_name = " ".join(full_name.split()[1:])
    return {
        "sub": str(user_record.get("google_sub") or "").strip(),
        "name": full_name,
        "email": str(user_record.get("email") or "").strip(),
        "picture": str(user_record.get("picture_url") or "").strip(),
        "given_name": given_name,
        "family_name": family_name,
        "locale": str(user_record.get("locale") or "").strip(),
        "email_verified": bool(user_record.get("email_verified")),
        "auth_provider": str(user_record.get("auth_provider") or "email").strip(),
        "contact_email": str(user_record.get("contact_email") or "").strip(),
        "current_role": str(user_record.get("current_role") or "").strip(),
        "target_locations": str(user_record.get("target_locations") or "").strip(),
        "primary_stack": str(user_record.get("primary_stack") or "").strip(),
    }

def is_valid_email_address(value):
    return bool(EMAIL_REGEX.match(str(value or "").strip()))

def normalize_profile_text(value, limit, lowercase=False):
    safe_value = re.sub(r"\s+", " ", str(value or "")).strip()
    if lowercase:
        safe_value = safe_value.lower()
    return safe_value[:limit]

def generate_otp_code():
    return f"{OTP_RANDOM.randint(0, 999999):06d}"

def split_display_name(full_name):
    safe_name = str(full_name or "").strip()
    if not safe_name:
        return DEFAULT_PROFILE_NAME, "Netrunner", "99"
    parts = safe_name.split()
    given_name = parts[0][:80] if parts else ""
    family_name = " ".join(parts[1:])[:80] if len(parts) > 1 else ""
    return safe_name[:120], given_name, family_name

def is_valid_profile_image_url(value):
    raw_value = str(value or "").strip()
    if not raw_value:
        return True
    parsed = urlparse(raw_value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)

def build_session_profile_user(current_user, profile_payload):
    base_user = dict(current_user or {})
    resolved_name = profile_payload["name"] if "name" in profile_payload else base_user.get("name")
    resolved_picture = profile_payload["picture"] if "picture" in profile_payload else base_user.get("picture")
    resolved_contact_email = profile_payload["contact_email"] if "contact_email" in profile_payload else base_user.get("contact_email")
    resolved_current_role = profile_payload["current_role"] if "current_role" in profile_payload else base_user.get("current_role")
    resolved_target_locations = profile_payload["target_locations"] if "target_locations" in profile_payload else base_user.get("target_locations")
    resolved_primary_stack = profile_payload["primary_stack"] if "primary_stack" in profile_payload else base_user.get("primary_stack")
    safe_name, given_name, family_name = split_display_name(
        resolved_name
    )
    picture_url = normalize_profile_text(
        resolved_picture or DEFAULT_PROFILE_AVATAR,
        255,
    )
    return {
        "sub": str(base_user.get("sub") or "").strip(),
        "name": safe_name,
        "email": str(base_user.get("email") or "").strip(),
        "picture": picture_url or DEFAULT_PROFILE_AVATAR,
        "given_name": given_name,
        "family_name": family_name,
        "locale": str(base_user.get("locale") or "").strip(),
        "email_verified": bool(base_user.get("email_verified")),
        "auth_provider": str(base_user.get("auth_provider") or ("guest" if not base_user.get("email") else "email")).strip(),
        "contact_email": normalize_profile_text(
            resolved_contact_email,
            100,
            lowercase=True,
        ),
        "current_role": normalize_profile_text(
            resolved_current_role,
            120,
        ),
        "target_locations": normalize_profile_text(
            resolved_target_locations,
            255,
        ),
        "primary_stack": normalize_profile_text(
            resolved_primary_stack,
            255,
        ),
    }

def ensure_profile_avatar_dir():
    avatar_dir = os.path.join(BASE_DIR, PROFILE_AVATAR_FOLDER)
    os.makedirs(avatar_dir, exist_ok=True)
    return avatar_dir

def remove_managed_profile_avatar(picture_url):
    raw_url = str(picture_url or "").strip()
    if not raw_url:
        return

    parsed_path = urlparse(raw_url).path or raw_url
    normalized_path = parsed_path.replace("\\", "/")
    if not (
        normalized_path.startswith(PROFILE_AVATAR_URL_PREFIX)
        or normalized_path.startswith(PROFILE_AVATAR_FOLDER.replace("\\", "/"))
    ):
        return

    relative_path = normalized_path.lstrip("/")
    candidate_path = os.path.abspath(os.path.join(BASE_DIR, relative_path))
    avatar_root = os.path.abspath(os.path.join(BASE_DIR, PROFILE_AVATAR_FOLDER))
    if os.path.commonpath([candidate_path, avatar_root]) == avatar_root and os.path.isfile(candidate_path):
        try:
            os.remove(candidate_path)
        except OSError as exc:
            print(f"Avatar cleanup warning: {exc}")

def save_profile_avatar_image(file_storage, previous_picture_url=""):
    if not PILLOW_AVAILABLE:
        raise APIError("Avatar processing requires Pillow on the server.", 500)

    incoming_name = str(getattr(file_storage, "filename", "") or "").strip()
    safe_name = secure_filename(incoming_name) if WERKZEUG_UTILS_AVAILABLE else incoming_name
    if not safe_name:
        raise APIError("Avatar file name is invalid.", 400)

    mimetype = str(getattr(file_storage, "mimetype", "") or "").lower()
    if mimetype and not mimetype.startswith("image/"):
        raise APIError("Avatar upload must be an image file.", 400)

    raw_bytes = file_storage.read()
    if not raw_bytes:
        raise APIError("Avatar upload was empty.", 400)
    if len(raw_bytes) > MAX_PROFILE_AVATAR_BYTES:
        raise APIError("Avatar files must be 5 MB or smaller.", 400)

    try:
        image = Image.open(io.BytesIO(raw_bytes))
        image = ImageOps.exif_transpose(image)
        image.load()
    except Exception:
        raise APIError("Avatar upload could not be processed. Use PNG, JPG, or WEBP.", 400)

    if image.mode != "RGBA":
        image = image.convert("RGBA")
    matte = Image.new("RGBA", image.size, (17, 20, 26, 255))
    matte.alpha_composite(image)
    image = matte.convert("RGB")

    resampling = Image.Resampling.LANCZOS if hasattr(Image, "Resampling") else Image.LANCZOS
    image = ImageOps.fit(
        image,
        (PROFILE_AVATAR_OUTPUT_SIZE, PROFILE_AVATAR_OUTPUT_SIZE),
        method=resampling,
    )

    output = io.BytesIO()
    image.save(output, format="JPEG", quality=82, optimize=True)

    avatar_dir = ensure_profile_avatar_dir()
    avatar_filename = f"profile_{uuid4().hex}.jpg"
    avatar_path = os.path.join(avatar_dir, avatar_filename)
    with open(avatar_path, "wb") as avatar_handle:
        avatar_handle.write(output.getvalue())

    remove_managed_profile_avatar(previous_picture_url)
    return f"{PROFILE_AVATAR_URL_PREFIX}{avatar_filename}"

def user_uses_google_auth(user_info):
    profile = user_info or {}
    provider = str(profile.get("auth_provider") or "").strip().lower()
    return "google" in provider or bool(str(profile.get("sub") or "").strip())

def send_signup_otp_email(recipient_email, otp_code):
    smtp_host = os.getenv("TALENT_OS_SMTP_HOST", "").strip()
    smtp_user = os.getenv("TALENT_OS_SMTP_USER", "").strip()
    smtp_password = os.getenv("TALENT_OS_SMTP_PASSWORD", "").strip()
    smtp_from = os.getenv("TALENT_OS_SMTP_FROM", "").strip() or smtp_user
    smtp_port = int(os.getenv("TALENT_OS_SMTP_PORT", "587"))
    use_ssl = str(os.getenv("TALENT_OS_SMTP_USE_SSL", "")).strip().lower() in {"1", "true", "yes"}
    use_tls = str(os.getenv("TALENT_OS_SMTP_USE_TLS", "true")).strip().lower() in {"1", "true", "yes"}

    if not smtp_host or not smtp_from:
        print(f"[DEV OTP] Signup code for {recipient_email}: {otp_code}")
        return {
            "success": True,
            "delivery": "console",
            "message": "SMTP is not configured, so the OTP was written to the backend console. Set TALENT_OS_SMTP_HOST, TALENT_OS_SMTP_USER, TALENT_OS_SMTP_PASSWORD, TALENT_OS_SMTP_FROM, and TALENT_OS_SMTP_PORT to send real emails."
        }

    message = EmailMessage()
    message["Subject"] = "Tensai.AI verification code"
    message["From"] = smtp_from
    message["To"] = recipient_email
    message.set_content(
        f"Your Tensai.AI verification code is {otp_code}. "
        f"It expires in {OTP_EXPIRY_MINUTES} minutes.\n\n"
        "If you did not request this code, you can ignore this email."
    )

    try:
        if use_ssl or smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port, context=ssl.create_default_context(), timeout=15) as server:
                if smtp_user and smtp_password:
                    server.login(smtp_user, smtp_password)
                server.send_message(message)
        else:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as server:
                if use_tls:
                    server.starttls(context=ssl.create_default_context())
                if smtp_user and smtp_password:
                    server.login(smtp_user, smtp_password)
                server.send_message(message)
        return {"success": True, "delivery": "smtp", "message": "OTP sent to your email address."}
    except Exception as exc:
        print(f"SMTP OTP delivery failed for {recipient_email}: {exc}")
        print(f"[DEV OTP FALLBACK] Signup code for {recipient_email}: {otp_code}")
        return {
            "success": True,
            "delivery": "console",
            "message": "SMTP delivery failed, so the OTP was written to the backend console. Check your SMTP host, port, username, password, and sender address."
        }


def compute_match(jd_raw, resume_text):
    jd_clean = clean_text(jd_raw or "")
    res_clean = clean_text(resume_text or "")

    if SKLEARN_AVAILABLE:
        docs = [jd_clean, res_clean]
        vectorizer = TfidfVectorizer(stop_words='english', ngram_range=(1, 2))
        tfidf_matrix = vectorizer.fit_transform(docs)
        score = float(cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0] * 100)
        features = vectorizer.get_feature_names_out()
        jd_vec = tfidf_matrix.toarray()[0]
        res_vec = tfidf_matrix.toarray()[1]
        matched_skills = [features[i] for i in range(len(features)) if jd_vec[i] > 0 and res_vec[i] > 0]
        missing_gaps = [features[i] for i in range(len(features)) if jd_vec[i] > 0.1 and res_vec[i] == 0]
    else:
        jd_tokens = set(jd_clean.split())
        res_tokens = set(res_clean.split())
        if not jd_tokens:
            score = 0.0
        else:
            score = (len(jd_tokens & res_tokens) / len(jd_tokens)) * 100
        matched_skills = list(jd_tokens & res_tokens)[:10]
        missing_gaps = list(jd_tokens - res_tokens)[:8]

    return {
        "score": round(score, 1),
        "matched_skills": matched_skills,
        "missing_skills": missing_gaps,
        "verdict": "Strong Candidate" if score > 60 else "Moderate Match" if score > 30 else "Low Fit"
    }


if FLASK_AVAILABLE:
    app = Flask(__name__)
    
    # Secret key is required to use Flask sessions securely
    app.secret_key = "talent_os_super_secret_key_change_in_production"

    # --- NEW: RATE LIMITER (Security) ---
    if LIMITER_AVAILABLE:
        limiter = Limiter(
            get_remote_address,
            app=app,
            default_limits=["200 per day", "50 per hour"],
            storage_uri="memory://"
        )
    else:
        # Dummy limiter if library is missing
        class DummyLimiter:
            def limit(self, *args, **kwargs):
                def decorator(f): return f
                return decorator
        limiter = DummyLimiter()

    # --- NEW: CUSTOM ERROR CLASS (Architecture) ---
    class APIError(Exception):
        """Custom Exception Class for precise API Error Handling"""
        def __init__(self, message, status_code=400):
            super().__init__()
            self.message = message
            self.status_code = status_code

    # --- NEW: GLOBAL ERROR HANDLERS ---
    @app.errorhandler(APIError)
    def handle_api_error(error):
        return jsonify({"success": False, "error": error.message}), error.status_code

    @app.errorhandler(404)
    def not_found_error(error):
        return jsonify({"success": False, "error": "Endpoint not found"}), 404

    @app.errorhandler(500)
    def internal_error(error):
        return jsonify({"success": False, "error": "Internal Server Error"}), 500


    def build_sse_event(event_name, payload):
        return f"event: {event_name}\ndata: {json.dumps(payload)}\n\n"


    # Initialize Google OAuth
    if AUTHLIB_AVAILABLE:
        # THIS LINE IS CRITICAL FOR LOCAL TESTING - ALLOWS HTTP INSTEAD OF HTTPS
        os.environ['AUTHLIB_INSECURE_TRANSPORT'] = '1'
        
        oauth = OAuth(app)
        google = oauth.register(
            name='google',
            client_id=os.environ.get("GOOGLE_CLIENT_ID"),
            client_secret=os.environ.get("GOOGLE_CLIENT_SECRET"),
            server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
            client_kwargs={'scope': 'openid email profile'}
        )

    @app.route('/')
    def home():
        user = session.get('user')
        if user:
            if DB_AVAILABLE and user_uses_google_auth(user):
                sync_result = upsert_google_user(user)
                if not sync_result.get('success'):
                    print(f"Existing session sync warning: {sync_result.get('error')}")
                elif sync_result.get('user'):
                    session['user'] = serialize_platform_user(sync_result.get('user'))
            session['entry_animation'] = 'returning-user'
            return redirect(url_for('scanner_app'))
        return render_template('home.html')

    # --- GOOGLE LOGIN ROUTE ---
    @app.route('/login')
    def login():
        if session.get('user'):
            if DB_AVAILABLE and user_uses_google_auth(session.get('user')):
                sync_result = upsert_google_user(session['user'])
                if not sync_result.get('success'):
                    print(f"Session sync warning before redirect: {sync_result.get('error')}")
                elif sync_result.get('user'):
                    session['user'] = serialize_platform_user(sync_result.get('user'))
            session['entry_animation'] = 'returning-user'
            return redirect(url_for('scanner_app'))

        if AUTHLIB_AVAILABLE:
            # Tell Google to send the user back to /authorize after logging in
            redirect_uri = url_for('authorize', _external=True)
            return google.authorize_redirect(redirect_uri)
        else:
            print("Authlib missing! Install it via 'pip install Authlib requests' to enable Google login.")
            return redirect(url_for('scanner_app')) # Fallback if library is missing

    # --- GOOGLE CALLBACK ROUTE ---
    @app.route('/authorize')
    def authorize():
        if AUTHLIB_AVAILABLE:
            try:
                # Fetch the authentication token from Google
                token = google.authorize_access_token()
                # Parse the user's profile info (email, name, picture)
                user_info = token.get('userinfo')
                if not user_info:
                    user_info = google.get('userinfo').json()
                if user_info:
                    # Save user details in the Flask session
                    session_user = serialize_google_user(user_info)
                    session['user'] = session_user

                    sync_result = {"success": False, "is_new_user": False}
                    if DB_AVAILABLE:
                        sync_result = upsert_google_user(session_user)
                        if not sync_result.get('success'):
                            print(f"Google user sync warning: {sync_result.get('error')}")
                        elif sync_result.get('user'):
                            session_user = serialize_platform_user(sync_result.get('user'))
                            session['user'] = session_user

                    session['entry_animation'] = 'new-user' if sync_result.get('is_new_user') else 'returning-user'
                    print(f"Logged in successfully as: {session_user.get('email')}")
            except Exception as e:
                print(f"Google OAuth Error: {e}")
                
        # Redirect into the main application once authentication is done
        return redirect(url_for('scanner_app'))

    @app.route('/logout')
    def logout():
        session.pop('user', None)
        session.pop('entry_animation', None)
        return redirect(url_for('home'))

    @app.route('/auth/email/request-otp', methods=['POST'])
    @limiter.limit("5 per minute")
    def request_email_signup_otp():
        if not DB_AVAILABLE:
            raise APIError("Database is not available for email signup.", 500)
        if not WERKZEUG_SECURITY_AVAILABLE:
            raise APIError("Password hashing support is unavailable on this server.", 500)

        data = request.get_json(silent=True) or {}
        full_name = str(data.get('full_name', '')).strip()
        email = str(data.get('email', '')).strip().lower()
        password = str(data.get('password', ''))

        if len(full_name) < 2:
            raise APIError("Full name must be at least 2 characters long.", 400)
        if not is_valid_email_address(email):
            raise APIError("Please enter a valid email address.", 400)
        if len(password) < 8:
            raise APIError("Password must be at least 8 characters long.", 400)

        otp_code = generate_otp_code()
        otp_expires_at = datetime.utcnow() + timedelta(minutes=OTP_EXPIRY_MINUTES)
        password_hash = generate_password_hash(password)

        signup_result = begin_email_signup(full_name, email, password_hash, otp_code, otp_expires_at)
        if not signup_result.get('success'):
            raise APIError(signup_result.get('error') or "Could not start email signup.", 400)

        delivery_result = send_signup_otp_email(email, otp_code)
        if not delivery_result.get('success'):
            raise APIError(delivery_result.get('error') or "Failed to send OTP email.", 500)

        return jsonify({
            "success": True,
            "email": email,
            "message": delivery_result.get('message') or "OTP sent to your email address.",
            "delivery": delivery_result.get('delivery', 'smtp'),
            "expires_in_minutes": OTP_EXPIRY_MINUTES
        })

    @app.route('/auth/email/verify-otp', methods=['POST'])
    @limiter.limit("10 per minute")
    def verify_email_signup():
        if not DB_AVAILABLE:
            raise APIError("Database is not available for email signup.", 500)

        data = request.get_json(silent=True) or {}
        email = str(data.get('email', '')).strip().lower()
        otp = re.sub(r'\D', '', str(data.get('otp', '')))

        if not is_valid_email_address(email):
            raise APIError("Please enter a valid email address.", 400)
        if len(otp) != 6:
            raise APIError("OTP must be a 6-digit code.", 400)

        verification_result = verify_email_signup_otp(email, otp)
        if not verification_result.get('success'):
            raise APIError(verification_result.get('error') or "OTP verification failed.", 400)

        session['user'] = serialize_platform_user(verification_result.get('user'))
        session['entry_animation'] = 'new-user'

        return jsonify({
            "success": True,
            "message": "Email verified. Your Tensai.AI workspace is ready.",
            "redirect": url_for('scanner_app')
        })

    @app.route('/auth/email/login', methods=['POST'])
    @limiter.limit("10 per minute")
    def email_login():
        if not DB_AVAILABLE:
            raise APIError("Database is not available for email login.", 500)
        if not WERKZEUG_SECURITY_AVAILABLE:
            raise APIError("Password hashing support is unavailable on this server.", 500)

        data = request.get_json(silent=True) or {}
        email = str(data.get('email', '')).strip().lower()
        password = str(data.get('password', ''))

        if not is_valid_email_address(email):
            raise APIError("Please enter a valid email address.", 400)
        if not password:
            raise APIError("Password is required.", 400)

        login_result = authenticate_email_user(email, lambda stored_hash: check_password_hash(stored_hash, password))
        if not login_result.get('success'):
            raise APIError(login_result.get('error') or "Login failed.", 401)

        session['user'] = serialize_platform_user(login_result.get('user'))
        session['entry_animation'] = 'returning-user'

        return jsonify({
            "success": True,
            "message": "Login successful.",
            "redirect": url_for('scanner_app')
        })

    @app.route('/profile/update', methods=['POST'])
    @limiter.limit("20 per hour")
    def update_profile():
        data = request.form.to_dict() if request.form else (request.get_json(silent=True) or {})
        display_name = normalize_profile_text(data.get('name'), 120)
        contact_email = normalize_profile_text(data.get('contact_email'), 100, lowercase=True)
        current_role = normalize_profile_text(data.get('current_role'), 120)
        target_locations = normalize_profile_text(data.get('target_locations'), 255)
        primary_stack = normalize_profile_text(data.get('primary_stack'), 255)
        avatar_file = request.files.get('avatar')

        if len(display_name) < 2:
            raise APIError("Display name must be at least 2 characters long.", 400)
        if contact_email and not is_valid_email_address(contact_email):
            raise APIError("Professional contact email must be valid.", 400)
        if current_role and len(current_role) < 2:
            raise APIError("Current role must be at least 2 characters long.", 400)

        current_user = session.get('user', {}) or {}
        picture_url = normalize_profile_text(
            current_user.get('picture') or DEFAULT_PROFILE_AVATAR,
            255,
        )

        legacy_picture_url = normalize_profile_text(data.get('picture'), 255)
        if avatar_file and getattr(avatar_file, 'filename', ''):
            picture_url = save_profile_avatar_image(
                avatar_file,
                previous_picture_url=current_user.get('picture'),
            )
        elif legacy_picture_url:
            if not is_valid_profile_image_url(legacy_picture_url):
                raise APIError("Avatar URL must start with http:// or https://.", 400)
            picture_url = legacy_picture_url

        updated_user = build_session_profile_user(current_user, {
            "name": display_name,
            "picture": picture_url,
            "contact_email": contact_email,
            "current_role": current_role,
            "target_locations": target_locations,
            "primary_stack": primary_stack,
        })

        persisted = False
        storage = 'session'
        message = "Profile updated."

        if DB_AVAILABLE and updated_user.get('email'):
            update_result = update_platform_user_profile(
                updated_user.get('email'),
                updated_user.get('name'),
                picture_url=updated_user.get('picture'),
                contact_email=updated_user.get('contact_email'),
                current_role=updated_user.get('current_role'),
                target_locations=updated_user.get('target_locations'),
                primary_stack=updated_user.get('primary_stack'),
            )
            if update_result.get('success'):
                updated_user = serialize_platform_user(update_result.get('user'))
                persisted = True
                storage = 'database'
            else:
                print(f"Profile update sync warning: {update_result.get('error')}")
                message = "Profile updated for this session."

        session['user'] = updated_user

        return jsonify({
            "success": True,
            "message": message,
            "persisted": persisted,
            "storage": storage,
            "user": updated_user,
        })

    # --- MAIN APPLICATION ---
    @app.route('/app')
    def scanner_app():
        # Extract user from session and pass it to the index template
        user = session.get('user', {})
        entry_animation = session.pop('entry_animation', '')
        return render_template('index.html', user=user, entry_animation=entry_animation)

    @app.route('/analyze', methods=['POST'])
    @limiter.limit("10 per minute") # Prevent API abuse
    def analyze():
        # --- NEW: INPUT VALIDATION ---
        jd_raw = request.form.get('jd')
        if not jd_raw or len(jd_raw.strip()) < 2:
            raise APIError("Enter at least a short role title or job description.", 400)

        file = request.files.get('resume')
        resume_text = str(request.form.get('resume_text', '') or '')
        file_name = str(request.form.get('file_name', '') or '').strip()

        if not file and len(resume_text.strip()) < 40:
            raise APIError("A resume file or extracted resume text is required.", 400)

        if file:
            allowed_extensions = {'pdf', 'txt'}
            if '.' not in file.filename or file.filename.rsplit('.', 1)[1].lower() not in allowed_extensions:
                raise APIError("Invalid file type. Only PDF and TXT are allowed.", 400)
            if not file_name:
                file_name = file.filename

        if len(resume_text.strip()) < 40 and file:
            if PDFPLUMBER_AVAILABLE and file.filename.lower().endswith('.pdf'):
                try:
                    with pdfplumber.open(file) as pdf:
                        resume_text = " ".join([p.extract_text() for p in pdf.pages if p.extract_text()])
                except Exception:
                    raise APIError("Could not read PDF text from the uploaded resume.", 500)
            else:
                try:
                    file.stream.seek(0)
                    raw = file.stream.read()
                    resume_text = raw.decode('utf-8', errors='ignore')
                except Exception:
                    resume_text = ""

        if len(resume_text.strip()) < 40:
            raise APIError("The resume text was too short to analyze reliably.", 400)

        if analyze_resume_document:
            result = analyze_resume_document(jd_raw, resume_text, file_name=file_name)
        else:
            result = compute_match(jd_raw, resume_text)
        return jsonify(result)

    @app.route('/api/ai/chat', methods=['POST'])
    @limiter.limit("40 per hour")
    def ai_chat_proxy():
        if not run_openrouter_chat:
            raise APIError("AI proxy is unavailable on this server.", 500)

        data = request.get_json(silent=True) or {}
        messages = data.get('messages')
        if not isinstance(messages, list) or not messages:
            raise APIError("A non-empty messages array is required.", 400)

        normalized_messages = []
        for raw_message in messages[-10:]:
            if not isinstance(raw_message, dict):
                continue
            role = str(raw_message.get('role', 'user')).strip().lower()
            if role not in {'system', 'user', 'assistant'}:
                continue
            content = raw_message.get('content', '')
            if isinstance(content, list):
                content = " ".join(
                    str(item.get('text', '')) if isinstance(item, dict) else str(item)
                    for item in content
                )
            content = str(content).strip()
            if not content:
                continue
            normalized_messages.append({
                "role": role,
                "content": content[:15000],
            })

        if not normalized_messages:
            raise APIError("No valid messages were provided.", 400)

        try:
            temperature = float(data.get('temperature', 0.2))
        except Exception:
            temperature = 0.2
        temperature = max(0.0, min(temperature, 1.0))

        max_tokens = data.get('max_tokens')
        try:
            max_tokens = int(max_tokens) if max_tokens is not None else None
        except Exception:
            max_tokens = None

        try:
            content = run_openrouter_chat(normalized_messages, temperature=temperature, max_tokens=max_tokens)
        except Exception as exc:
            raise APIError(str(exc), 502)

        return jsonify({"success": True, "content": content})
        
    @app.route('/save_candidate', methods=['POST'])
    def save_candidate():
        if not DB_AVAILABLE:
            return jsonify({"error": "Database not configured/available."}), 500
            
        data = request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400
            
        jd_text = data.get('jd_text', 'No JD Provided')
        candidate_data = data.get('candidate_data', {})
        analysis_data = data.get('analysis_data', {})
        
        job_title = "Unknown Mission"
        lines = [line.strip() for line in jd_text.split('\n') if line.strip()]
        if lines:
            job_title = lines[0][:90] 
            
        result = save_candidate_data(job_title, jd_text, candidate_data, analysis_data)
        return jsonify(result)

    @app.route('/api/leaderboard', methods=['GET'])
    def get_leaderboard():
        if not DB_AVAILABLE:
            return jsonify({"error": "Database not configured/available."}), 500

        try:
            limit = int(request.args.get('limit', 50))
        except Exception:
            limit = 50
        limit = max(1, min(limit, 100))

        top_candidates = get_top_candidates(limit=limit)
        return jsonify(top_candidates)

    @app.route('/api/delete_candidate', methods=['POST'])
    def delete_candidate():
        if not DB_AVAILABLE:
            return jsonify({"error": "Database not configured/available."}), 500
            
        data = request.json
        name = data.get('name')
        if not name:
            return jsonify({"success": False, "error": "No name provided"}), 400
            
        success = delete_candidate_by_name(name)
        return jsonify({"success": success})

    @app.route('/api/purge', methods=['POST'])
    def purge():
        if not DB_AVAILABLE:
            return jsonify({"error": "Database not configured/available."}), 500
            
        success = purge_database()
        return jsonify({"success": success})

    @app.route('/chat', methods=['POST'])
    @limiter.limit("40 per hour")
    def chat():
        if not get_financial_answer:
            return jsonify({"answer": "> **ERROR**: chat_engine.py not found on backend."}), 500

        data = request.get_json(silent=True) or {}
        query = str(data.get('query', '') or '').strip()
        context_text = str(data.get('context', '') or '')
        api_key = data.get('api_key', '')
        history = data.get('history', []) if isinstance(data.get('history', []), list) else []
        analysis_mode = str(data.get('analysis_mode', 'financial') or 'financial').strip().lower()
        persona_mode = str(data.get('persona_mode', 'analyst') or 'analyst').strip().lower()
        response_mode = str(data.get('response_mode', 'markdown') or 'markdown').strip().lower()
        interview_mode = persona_mode == 'interview' and analysis_mode == 'resume'

        if not query:
            return jsonify({"error": "Query is required."}), 400

        if response_mode == 'scorecard':
            if analysis_mode != 'resume':
                return jsonify({"error": "Structured scorecards are only available in Resume Analysis."}), 400
            if not get_candidate_scorecard:
                return jsonify({"error": "Structured scorecards are unavailable on this server."}), 500
            scorecard = get_candidate_scorecard(
                query,
                context_text,
                history=history,
                interview_mode=interview_mode,
                analysis_mode=analysis_mode,
            )
            answer = scorecard.get('markdown_summary') or scorecard.get('summary') or ''
            payload = {"answer": answer, "mode": "scorecard", "scorecard": scorecard}
        else:
            answer = get_financial_answer(
                query,
                context_text,
                api_key,
                history,
                interview_mode=interview_mode,
                analysis_mode=analysis_mode,
            )
            payload = {"answer": answer, "mode": "markdown"}

        if DB_AVAILABLE and answer:
            save_chat_message(query, answer)

        return jsonify(payload)

    @app.route('/chat/stream', methods=['POST'])
    @limiter.limit("40 per hour")
    def chat_stream():
        if not stream_financial_answer:
            return Response(
                build_sse_event("error", {"message": "Streaming chat is unavailable on this server."}),
                status=500,
                mimetype='text/event-stream',
            )

        data = request.get_json(silent=True) or {}
        query = str(data.get('query', '') or '').strip()
        context_text = str(data.get('context', '') or '')
        history = data.get('history', []) if isinstance(data.get('history', []), list) else []
        analysis_mode = str(data.get('analysis_mode', 'financial') or 'financial').strip().lower()
        persona_mode = str(data.get('persona_mode', 'analyst') or 'analyst').strip().lower()
        response_mode = str(data.get('response_mode', 'markdown') or 'markdown').strip().lower()
        interview_mode = persona_mode == 'interview' and analysis_mode == 'resume'

        if not query:
            return Response(
                build_sse_event("error", {"message": "Query is required."}),
                status=400,
                mimetype='text/event-stream',
            )

        def generate():
            answer_parts = []
            try:
                yield build_sse_event("start", {"mode": response_mode, "persona_mode": persona_mode, "analysis_mode": analysis_mode})
                if response_mode == 'scorecard':
                    if analysis_mode != 'resume':
                        raise RuntimeError("Structured scorecards are only available in Resume Analysis.")
                    if not get_candidate_scorecard:
                        raise RuntimeError("Structured scorecards are unavailable on this server.")
                    yield build_sse_event("status", {"message": "Building scorecard..."})
                    scorecard = get_candidate_scorecard(
                        query,
                        context_text,
                        history=history,
                        interview_mode=interview_mode,
                        analysis_mode=analysis_mode,
                    )
                    summary = scorecard.get('markdown_summary') or scorecard.get('summary') or ''
                    if summary:
                        answer_parts.append(summary)
                    yield build_sse_event("widget", {"mode": "scorecard", "scorecard": scorecard})
                else:
                    for event in stream_financial_answer(
                        query,
                        context_text,
                        history=history,
                        interview_mode=interview_mode,
                        analysis_mode=analysis_mode,
                    ):
                        event_type = str(event.get('type', '') or '')
                        if event_type == 'status':
                            yield build_sse_event("status", {"message": event.get('message', '')})
                            continue
                        if event_type == 'delta':
                            chunk = str(event.get('content', '') or '')
                            if chunk:
                                answer_parts.append(chunk)
                                yield build_sse_event("token", {"content": chunk})

                final_answer = "".join(answer_parts).strip()
                if DB_AVAILABLE and final_answer:
                    save_chat_message(query, final_answer)
                yield build_sse_event("done", {"answer": final_answer, "mode": response_mode})
            except Exception as exc:
                yield build_sse_event("error", {"message": str(exc)})

        return Response(
            stream_with_context(generate()),
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'X-Accel-Buffering': 'no',
            },
        )

    @app.route('/api/osint_trace', methods=['POST'])
    @limiter.limit("20 per minute")
    def osint_trace():
        if not build_osint_bundle:
            return jsonify({"success": False, "error": "OSINT engine unavailable."}), 500

        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No request data"}), 400

        target_name = data.get('name', '').strip()
        context_text = data.get('context', '')
        if not target_name:
            return jsonify({"success": False, "error": "Candidate name is required."}), 400

        bundle = build_osint_bundle(target_name, context_text)
        return jsonify({"success": True, "data": bundle})

    if __name__ == '__main__':
        # NOTE: OAuth often strictly requires HTTPS, but works on 127.0.0.1 in development
        app.run(debug=True, use_reloader=False)
else:
    def _read_text_from_path(path):
        try:
            with open(path, 'rb') as f:
                data = f.read()
            return data.decode('utf-8', errors='ignore')
        except Exception:
            return ''

    def _usage_and_exit():
        print('Usage: python app.py <job_description.txt> <resume.txt_or_pdf_optional>')
        sys.exit(1)

    if __name__ == '__main__':
        if len(sys.argv) < 2:
            jd_text = "Looking for a Python developer with Flask and REST API experience."
            resume_text = "Experienced Python developer; worked with APIs, Flask, and testing."
        else:
            jd_path = sys.argv[1]
            resume_path = sys.argv[2] if len(sys.argv) > 2 else None
            jd_text = _read_text_from_path(jd_path)
            resume_text = _read_text_from_path(resume_path) if resume_path else ''

        output = compute_match(jd_text, resume_text)
        print(json.dumps(output, indent=2))

import math
import re
from collections import Counter
from datetime import datetime


STOP_WORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has", "have",
    "in", "is", "it", "of", "on", "or", "that", "the", "to", "with", "will", "this",
    "their", "them", "they", "you", "your", "our", "we", "us", "using", "use", "used",
    "experience", "work", "working", "role", "roles", "team", "candidate", "resume",
    "job", "description", "required", "preferred", "responsibilities", "skills",
    "skill", "years", "year", "strong", "ability", "must", "plus", "including",
    "within", "across", "into", "over", "under", "through", "while", "such", "than",
    "than", "can", "may", "also", "about", "after", "before", "built", "build",
}

SKILL_PATTERNS = {
    "Python": (r"\bpython\b",),
    "Java": (r"\bjava\b",),
    "JavaScript": (r"\bjavascript\b",),
    "TypeScript": (r"\btypescript\b",),
    "SQL": (r"\bsql\b",),
    "MySQL": (r"\bmysql\b",),
    "PostgreSQL": (r"\bpostgresql\b", r"\bpostgres\b"),
    "MongoDB": (r"\bmongodb\b",),
    "Redis": (r"\bredis\b",),
    "Flask": (r"\bflask\b",),
    "Django": (r"\bdjango\b",),
    "FastAPI": (r"\bfastapi\b",),
    "React": (r"\breact\b",),
    "Node.js": (r"\bnode\.?js\b",),
    "HTML": (r"\bhtml\b",),
    "CSS": (r"\bcss\b",),
    "REST APIs": (r"\brest(?:ful)? api(?:s)?\b", r"\bapi design\b"),
    "Microservices": (r"\bmicroservices?\b",),
    "Docker": (r"\bdocker\b",),
    "Kubernetes": (r"\bkubernetes\b", r"\bk8s\b"),
    "AWS": (r"\baws\b", r"\bamazon web services\b"),
    "Azure": (r"\bazure\b",),
    "GCP": (r"\bgcp\b", r"\bgoogle cloud\b"),
    "CI/CD": (r"\bci/?cd\b", r"\bcontinuous integration\b", r"\bcontinuous delivery\b"),
    "Git": (r"\bgit\b", r"\bgithub\b", r"\bgitlab\b"),
    "Linux": (r"\blinux\b",),
    "Testing": (r"\bunit testing\b", r"\bintegration testing\b", r"\bpytest\b", r"\btesting\b"),
    "Automation": (r"\bautomation\b", r"\bautomated\b"),
    "Machine Learning": (r"\bmachine learning\b", r"\bml\b"),
    "Deep Learning": (r"\bdeep learning\b",),
    "NLP": (r"\bnlp\b", r"\bnatural language processing\b"),
    "Pandas": (r"\bpandas\b",),
    "NumPy": (r"\bnumpy\b",),
    "Scikit-learn": (r"\bscikit[- ]learn\b", r"\bsklearn\b"),
    "TensorFlow": (r"\btensorflow\b",),
    "PyTorch": (r"\bpytorch\b",),
    "Power BI": (r"\bpower bi\b",),
    "Tableau": (r"\btableau\b",),
    "Excel": (r"\bexcel\b",),
    "Data Analysis": (r"\bdata analysis\b", r"\banalytical\b"),
    "Data Visualization": (r"\bdata visualization\b", r"\bdashboard(?:s)?\b"),
    "ETL": (r"\betl\b",),
    "Spark": (r"\bspark\b",),
    "Hadoop": (r"\bhadoop\b",),
    "Financial Modeling": (r"\bfinancial modeling\b",),
    "Forecasting": (r"\bforecasting\b", r"\bforecast\b"),
    "Project Management": (r"\bproject management\b",),
    "Agile": (r"\bagile\b",),
    "Scrum": (r"\bscrum\b",),
    "Product Strategy": (r"\bproduct strategy\b",),
    "Figma": (r"\bfigma\b",),
    "Salesforce": (r"\bsalesforce\b",),
    "Jira": (r"\bjira\b",),
    "Cybersecurity": (r"\bcybersecurity\b", r"\binformation security\b", r"\bsecurity operations\b"),
    "Penetration Testing": (r"\bpenetration testing\b", r"\bpen testing\b"),
    "Recruiting": (r"\brecruiting\b", r"\brecruitment\b"),
    "Talent Acquisition": (r"\btalent acquisition\b",),
    "Sourcing": (r"\bsourcing\b", r"\bcandidate sourcing\b"),
    "Stakeholder Management": (r"\bstakeholder management\b", r"\bstakeholder engagement\b"),
    "Leadership": (r"\bleadership\b", r"\bled\b", r"\bmanaged\b", r"\bmentor(?:ed|ing)?\b"),
    "Communication": (r"\bcommunication\b", r"\bpresented\b", r"\bpresentation\b"),
    "Problem Solving": (r"\bproblem solving\b", r"\bresolved\b", r"\bsolution(?:s)?\b"),
    "Nursing": (r"\bnursing\b", r"\bnurse\b"),
    "Patient Care": (r"\bpatient care\b", r"\bpatient advocacy\b"),
    "Patient Assessment": (r"\bpatient assessment\b", r"\bpatient assessments\b"),
    "Medication Administration": (r"\bmedication administration\b",),
    "IV Therapy": (r"\biv therapy\b", r"\bintravenous therapy\b"),
    "Wound Care": (r"\bwound care\b",),
    "Vital Signs Monitoring": (r"\bvital signs\b", r"\bvital signs monitoring\b"),
    "Electronic Health Records": (r"\belectronic health records\b", r"\behr\b"),
    "Clinical Documentation": (r"\bclinical documentation\b", r"\bdocument patient information\b"),
    "Discharge Planning": (r"\bdischarge planning\b",),
    "Pediatrics": (r"\bpediatrics\b",),
    "Intensive Care": (r"\bintensive care\b", r"\bicu\b"),
    "Medical-Surgical": (r"\bmedical-surgical\b", r"\bmedical surgical\b"),
    "Pharmacology": (r"\bpharmacology\b",),
    "Pathophysiology": (r"\bpathophysiology\b",),
    "Health Assessment": (r"\bhealth assessment\b",),
    "Microbiology": (r"\bmicrobiology\b",),
}

SECTION_ALIASES = {
    "summary": {"summary", "professional summary", "profile", "about", "objective"},
    "experience": {"experience", "work experience", "professional experience", "employment", "career history"},
    "projects": {"projects", "project experience", "key projects"},
    "skills": {"skills", "technical skills", "core skills", "competencies"},
    "education": {"education", "academic background", "academics"},
    "certifications": {"certifications", "licenses", "certificates"},
}

TITLE_PATTERN = re.compile(
    r"\b(?:senior|lead|principal|staff|junior|associate|assistant|head|chief)?\s*"
    r"(?:software|data|business|financial|product|project|operations|marketing|sales|"
    r"machine learning|ml|ai|devops|backend|front[- ]end|full[- ]stack|cloud|security|"
    r"cybersecurity|talent|recruiting|hr|human resources)\s+"
    r"(?:engineer|developer|scientist|analyst|manager|specialist|consultant|architect|"
    r"recruiter|partner|director|lead)\b",
    re.IGNORECASE,
)

GENERIC_TITLE_PATTERN = re.compile(
    r"\b(?:senior|lead|principal|staff|junior|associate|assistant|head|chief)?\s*"
    r"(?:[a-zA-Z+/&.\-]+\s+){0,3}"
    r"(?:engineer|developer|scientist|analyst|manager|specialist|consultant|architect|"
    r"recruiter|partner|director|lead|student|intern|nurse|coordinator|technician)\b",
    re.IGNORECASE,
)

DEGREE_PATTERN = re.compile(
    r"\b(?:integrated\s+)?(?:b\.?\s*tech|m\.?\s*tech|b\.?\s*sc|m\.?\s*sc|b\.?\s*ca|m\.?\s*ca|"
    r"b\.?\s*com|m\.?\s*com|b\.e\.|m\.e\.|bachelor(?:'s)?|master(?:'s)?|mba|ph\.?\s*d|"
    r"doctorate|diploma)\b[^,\n]{0,100}",
    re.IGNORECASE,
)

EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE_PATTERN = re.compile(r"(?:\+?\d{1,3}[\s\-]?)?(?:\(?\d{3,4}\)?[\s\-]?)?\d{3}[\s\-]?\d{4}")
YEAR_RANGE_PATTERN = re.compile(
    r"\b((?:19|20)\d{2})\s*(?:-|–|—|to)\s*(present|current|now|(?:19|20)\d{2})\b",
    re.IGNORECASE,
)
EXPLICIT_YEARS_PATTERN = re.compile(r"\b(\d{1,2})\+?\s+years?\b", re.IGNORECASE)
EDUCATION_INSTITUTION_PATTERN = re.compile(
    r"\b(?:university|college|institute|school|academy|department|faculty|campus)\b",
    re.IGNORECASE,
)
EDUCATION_CONTEXT_PATTERN = re.compile(
    r"\b(?:education|academic|specialization|major|minor|coursework|cgpa|gpa|graduation|"
    r"graduated|completion|completed|semester|distinction|honou?r)\b",
    re.IGNORECASE,
)
LOCATION_NOISE_PATTERN = re.compile(
    r"\b(?:linkedin|github|portfolio|summary|about|education|skills?|project|experience|"
    r"python|sql|tensorflow|keras|scikit|opencv|densenet|flask|numpy|pandas|excel|vlookup|"
    r"xlookup|power bi|tableau|mysql|sqlite|snowflake|aws|cnn|machine learning|deep learning|"
    r"dashboard|analysis|engineering|recommendation|attendance|system|university|college|"
    r"semester|specialization)\b",
    re.IGNORECASE,
)


def _clean_space(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalize_line(value):
    return _clean_space(re.sub(r"[\u2022\u2023\u25e6\u2043\u2219]", " ", value))


def _lower_tokens(value):
    return [
        token for token in re.findall(r"[a-z0-9][a-z0-9+#.\-/]{1,}", str(value or "").lower())
        if len(token) > 2 and token not in STOP_WORDS
    ]


def _cosine_similarity(left_text, right_text):
    left = Counter(_lower_tokens(left_text))
    right = Counter(_lower_tokens(right_text))
    if not left or not right:
        return 0.0
    dot = sum(left[token] * right.get(token, 0) for token in left)
    left_norm = math.sqrt(sum(value * value for value in left.values()))
    right_norm = math.sqrt(sum(value * value for value in right.values()))
    if not left_norm or not right_norm:
        return 0.0
    return dot / (left_norm * right_norm)


def _unique(items, limit=10):
    output = []
    seen = set()
    for item in items:
        clean = _clean_space(item)
        key = clean.lower()
        if clean and key not in seen:
            seen.add(key)
            output.append(clean)
        if len(output) >= limit:
            break
    return output


def _extract_section_map(text):
    sections = {key: [] for key in SECTION_ALIASES}
    current = None
    for raw_line in str(text or "").splitlines():
        line = _normalize_line(raw_line)
        if not line:
            continue
        lowered = line.lower().rstrip(":")
        matched_section = None
        for section_name, aliases in SECTION_ALIASES.items():
            if lowered in aliases or any(alias in lowered for alias in aliases if len(alias) >= 5):
                matched_section = section_name
                break
        if matched_section:
            current = matched_section
            continue
        if current:
            sections[current].append(line)
    return {key: "\n".join(value).strip() for key, value in sections.items()}


def _guess_name(text, file_name=""):
    heading_blacklist = {
        "resume", "curriculum vitae", "cv", "profile", "summary", "experience", "education",
        "skills", "contact", "projects", "certifications",
    }
    lines = [_normalize_line(line) for line in str(text or "").splitlines()[:12]]
    for line in lines:
        lowered = line.lower()
        if not line or any(token in lowered for token in heading_blacklist):
            continue
        if "@" in line or "http" in lowered or re.search(r"\d", line):
            continue
        words = re.findall(r"[A-Za-z][A-Za-z'\-]+", line)
        if 2 <= len(words) <= 4:
            return " ".join(word if not word.isupper() else word.title() for word in words)
    safe_file_name = re.sub(r"\.[^.]+$", "", str(file_name or "")).strip()
    return safe_file_name or "Unknown Candidate"


def _extract_emails(text):
    return _unique(match.group(0) for match in EMAIL_PATTERN.finditer(str(text or "")))


def _extract_phones(text):
    values = []
    for match in PHONE_PATTERN.finditer(str(text or "")):
        digits = re.sub(r"\D", "", match.group(0))
        if 10 <= len(digits) <= 13:
            values.append(match.group(0))
    return _unique(values, limit=3)


def _looks_like_location_value(value):
    clean = _normalize_line(value)
    lowered = clean.lower()
    if not clean:
        return False
    if lowered in {"remote", "hybrid", "onsite"}:
        return True
    if len(clean) > 60 or len(clean.split()) > 7:
        return False
    if EMAIL_PATTERN.search(clean) or PHONE_PATTERN.search(clean) or re.search(r"\d", clean):
        return False
    if ":" in clean or "|" in clean or "/" in clean:
        return False
    if LOCATION_NOISE_PATTERN.search(lowered):
        return False
    return bool(re.fullmatch(r"[A-Za-z .'\-]+(?:,\s*[A-Za-z .'\-]+){0,2}", clean))


def _extract_location(text):
    lines = [_normalize_line(line) for line in str(text or "").splitlines()[:12]]
    location_pattern = re.compile(r"^[A-Za-z .'\-]{2,40},\s*[A-Za-z .'\-]{2,40}$")
    for line in lines:
        lowered = line.lower()
        if "|" in line:
            location_parts = []
            for part in [piece.strip() for piece in line.split("|") if piece.strip()]:
                if re.search(r"\b(?:male|female|indian|citizenship|nationality)\b", part.lower()):
                    continue
                if _looks_like_location_value(part):
                    location_parts.append(part)
            if location_parts:
                return ", ".join(location_parts[:2])
        if lowered.startswith(("location:", "address:", "based in")):
            parts = line.split(":", 1)
            if len(parts) == 2:
                candidate = parts[1].strip()
                if _looks_like_location_value(candidate):
                    return candidate
        if location_pattern.match(line) and _looks_like_location_value(line):
            return line
    return ""


def _extract_titles(text):
    titles = []
    for line in str(text or "").splitlines():
        clean = _normalize_line(line)
        if len(clean) > 90:
            continue
        title_match = TITLE_PATTERN.search(clean) or GENERIC_TITLE_PATTERN.search(clean)
        if title_match:
            titles.append(title_match.group(0))
    if not titles:
        matches = list(TITLE_PATTERN.finditer(str(text or ""))) + list(GENERIC_TITLE_PATTERN.finditer(str(text or "")))
        titles.extend(match.group(0) for match in matches)
    return _unique(titles, limit=4)


def _extract_companies(text):
    patterns = [
        re.compile(r"\b(?:at|@)\s+([A-Z][A-Za-z0-9&.,'\- ]{2,40})"),
        re.compile(r"\b(?:client|company|employer)\s*:\s*([A-Z][A-Za-z0-9&.,'\- ]{2,40})", re.IGNORECASE),
    ]
    companies = []
    for pattern in patterns:
        companies.extend(match.group(1) for match in pattern.finditer(str(text or "")))
    return _unique(companies, limit=4)


def _extract_education(text, sections):
    section_lines = [
        _normalize_line(line)
        for line in str(sections.get("education", "") or "").splitlines()
        if _normalize_line(line)
    ]
    values = []

    degree_index = next((idx for idx, line in enumerate(section_lines) if DEGREE_PATTERN.search(line)), None)
    if degree_index is not None:
        degree_line = section_lines[degree_index]
        if degree_index + 1 < len(section_lines):
            follow_up = section_lines[degree_index + 1]
            if EDUCATION_CONTEXT_PATTERN.search(follow_up) and not EDUCATION_INSTITUTION_PATTERN.search(degree_line):
                degree_line = f"{degree_line} {follow_up}"
        values.append(degree_line)

    institution_line = next((line for line in section_lines if EDUCATION_INSTITUTION_PATTERN.search(line)), "")
    if institution_line:
        values.append(institution_line)

    if values:
        return _unique(values, limit=2)

    fallback_lines = [
        _normalize_line(line)
        for line in str(text or "").splitlines()[:40]
        if _normalize_line(line)
    ]
    for index, line in enumerate(fallback_lines):
        if not DEGREE_PATTERN.search(line):
            continue
        if LOCATION_NOISE_PATTERN.search(line.lower()) and not EDUCATION_CONTEXT_PATTERN.search(line):
            continue
        values.append(line)
        if index + 1 < len(fallback_lines):
            follow_up = fallback_lines[index + 1]
            if EDUCATION_INSTITUTION_PATTERN.search(follow_up):
                values.append(follow_up)
        break

    return _unique(values, limit=2) if values else []


def _extract_years_experience(text, sections):
    current_year = datetime.utcnow().year
    source_text = "\n".join(part for part in (sections.get("experience", ""), sections.get("projects", "")) if part).strip() or str(text or "")
    explicit_years = [int(match.group(1)) for match in EXPLICIT_YEARS_PATTERN.finditer(str(text or ""))]
    span_years = []
    for match in YEAR_RANGE_PATTERN.finditer(source_text):
        start_year = int(match.group(1))
        raw_end = match.group(2).lower()
        end_year = current_year if raw_end in {"present", "current", "now"} else int(raw_end)
        if 1980 <= start_year <= current_year and start_year <= end_year <= current_year:
            span_years.append(end_year - start_year)
    best_years = max(explicit_years + span_years + [0])
    return max(0, min(best_years, 30))


def _extract_skill_frequency(text):
    lowered = str(text or "").lower()
    counts = {}
    for skill_name, patterns in SKILL_PATTERNS.items():
        total = 0
        for pattern in patterns:
            total += len(re.findall(pattern, lowered, re.IGNORECASE))
        if total:
            counts[skill_name] = total
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _top_keywords(text, limit=10):
    counter = Counter(_lower_tokens(text))
    return [token.replace("-", " ").replace("/", " ") for token, _ in counter.most_common(limit)]


def _extract_target_role(jd_text):
    lines = [_normalize_line(line) for line in str(jd_text or "").splitlines() if _normalize_line(line)]
    if len(lines) == 1 and 1 <= len(lines[0].split()) <= 10:
        return lines[0]
    for line in lines[:4]:
        if len(line) <= 90 and TITLE_PATTERN.search(line):
            return TITLE_PATTERN.search(line).group(0)
    for line in lines[:3]:
        if 1 <= len(line.split()) <= 12:
            return line
    return "Target role not detected"


def _extract_required_years(jd_text):
    values = [int(match.group(1)) for match in EXPLICIT_YEARS_PATTERN.finditer(str(jd_text or ""))]
    return max(values) if values else 0


def _score_experience(candidate_years, required_years, has_experience_section):
    if required_years:
        gap = candidate_years - required_years
        base = 100 if gap >= 0 else max(28, 100 + (gap * 15))
        if candidate_years == 0:
            base = 24
        return max(0, min(base, 100))
    inferred = 38 + min(candidate_years * 9, 42)
    if has_experience_section:
        inferred += 10
    return max(35, min(inferred, 95))


def _score_evidence(sections, achievements, companies, titles):
    section_count = sum(1 for value in sections.values() if value)
    bullet_like_lines = sum(1 for line in str(sections.get("experience", "")).splitlines() if line.strip().startswith(("-", "*")))
    score = 34 + section_count * 8 + min(len(achievements) * 8, 24) + min(len(companies) * 4, 12) + min(len(titles) * 5, 15)
    if bullet_like_lines:
        score += 6
    return max(25, min(score, 100))


def _score_communication(text, sections, emails, phones):
    length = len(_clean_space(text))
    score = 45
    if emails:
        score += 10
    if phones:
        score += 8
    if sections.get("summary"):
        score += 10
    if sections.get("skills"):
        score += 8
    if 1200 <= length <= 9000:
        score += 12
    elif length < 900:
        score -= 12
    return max(20, min(score, 96))


def _score_leadership(text):
    leadership_hits = len(re.findall(r"\b(?:lead|led|manage|managed|mentor|mentored|owner|owned|strategy|stakeholder)\b", str(text or "").lower()))
    return max(25, min(30 + leadership_hits * 9, 95))


def _score_problem_solving(text):
    hits = len(re.findall(r"\b(?:built|designed|optimized|improved|reduced|increased|solved|resolved|automated|launched)\b", str(text or "").lower()))
    return max(30, min(34 + hits * 7, 96))


def _achievements_from_text(text, sections):
    sources = [sections.get("experience", ""), sections.get("projects", ""), str(text or "")]
    lines = []
    for source in sources:
        for raw_line in str(source).splitlines():
            clean = _normalize_line(raw_line)
            digits = re.sub(r"\D", "", clean)
            if PHONE_PATTERN.fullmatch(clean) or (len(digits) >= 10 and len(re.findall(r"[A-Za-z]", clean)) < 5):
                continue
            if clean and re.search(r"[%$₹€£]|\b\d+(?:\.\d+)?\b", clean) and re.search(r"\b(?:assist|document|complete|monitor|provide|administer|improve|support|coordinate|deliver|manage|lead|reduce|increase|build|launch)\b", clean.lower()):
                lines.append(clean)
    return _unique(lines, limit=4)


def _strong_sections(sections, emails, phones, achievements):
    output = []
    if sections.get("experience"):
        output.append("Experience section is present")
    if sections.get("skills"):
        output.append("Dedicated skills section")
    if sections.get("projects"):
        output.append("Project evidence included")
    if sections.get("summary"):
        output.append("Professional summary included")
    if emails and phones:
        output.append("Complete contact information")
    if achievements:
        output.append("Quantified impact statements")
    return output[:5]


def _weak_sections(sections, emails, phones, achievements):
    output = []
    if not sections.get("summary"):
        output.append("Professional summary missing")
    if not sections.get("projects"):
        output.append("Project evidence is thin")
    if not achievements:
        output.append("Limited quantified impact")
    if not emails:
        output.append("Email address missing")
    if not phones:
        output.append("Phone number missing")
    if not sections.get("skills"):
        output.append("Skills section missing")
    return output[:5]


def _fit_band(score):
    if score >= 82:
        return "High alignment"
    if score >= 68:
        return "Strong but needs validation"
    if score >= 52:
        return "Mixed alignment"
    return "Low alignment"


def _confidence_label(evidence_score, matched_skills, missing_skills):
    if evidence_score >= 78 and len(matched_skills) >= max(2, len(missing_skills)):
        return "High"
    if evidence_score >= 58:
        return "Medium"
    return "Low"


def _recommendation(score):
    if score >= 82:
        return "Advance to structured interview"
    if score >= 68:
        return "Advance with targeted validation"
    if score >= 52:
        return "Hold for secondary review"
    return "Do not prioritize yet"


def _search_priority(score):
    if score >= 82:
        return "Fast-track"
    if score >= 68:
        return "Interview with validation"
    if score >= 52:
        return "Keep in pipeline"
    return "Continue sourcing"


def _role_context(target_role):
    role = _clean_space(target_role)
    lowered = role.lower()
    if not role or lowered == "target role not detected":
        return "this role"
    if len(role.split()) == 1:
        return f"this {lowered} role"
    return lowered


def _seniority_label(years, titles):
    title_blob = " ".join(titles).lower()
    if "principal" in title_blob or "staff" in title_blob or "head" in title_blob:
        return "Principal / Head"
    if "senior" in title_blob or years >= 7:
        return "Senior"
    if years >= 3:
        return "Mid-level"
    if years >= 1:
        return "Early career"
    return "Experience not clear"


def _build_interview_questions(matched_skills, missing_skills, target_role, achievements):
    questions = []
    role_context = _role_context(target_role)
    if missing_skills:
        questions.append(f"Walk through a real project where you applied {missing_skills[0]} in a way that maps directly to {role_context}.")
    if len(matched_skills) >= 1:
        questions.append(f"What was the highest-impact outcome you delivered using {matched_skills[0]}, and how did you measure success?")
    if achievements:
        questions.append("Choose one quantified result on the resume and explain the exact steps, tradeoffs, and tools behind it.")
    questions.append(f"What risks would you expect to handle in your first 90 days in {role_context}?")
    return _unique(questions, limit=4)


def _build_next_steps(score, missing_skills, confidence):
    steps = []
    if score >= 82:
        steps.append("Move the candidate to a structured interview loop with role-specific scorecards.")
        steps.append("Use reference checks to validate the strongest production claims.")
    elif score >= 68:
        steps.append("Run a focused technical or case-based screen before final-round interviews.")
        steps.append("Pressure-test the most material skill gap in the first interview.")
    else:
        steps.append("Keep sourcing active while deciding whether this profile is worth a narrow validation round.")
    if missing_skills:
        steps.append(f"Validate depth in {missing_skills[0]} before making a shortlist decision.")
    if confidence != "High":
        steps.append("Cross-check chronology and role scope during screening because the resume evidence is incomplete.")
    return _unique(steps, limit=4)


def analyze_resume_document(job_description, resume_text, file_name=""):
    safe_jd = _clean_space(job_description)
    safe_resume = str(resume_text or "")
    sections = _extract_section_map(safe_resume)
    emails = _extract_emails(safe_resume)
    phones = _extract_phones(safe_resume)
    location = _extract_location(safe_resume)
    titles = _extract_titles(safe_resume)
    companies = _extract_companies(safe_resume)
    education = _extract_education(safe_resume, sections)
    experience_years = _extract_years_experience(safe_resume, sections)
    resume_skill_frequency = _extract_skill_frequency(safe_resume)
    jd_skill_frequency = _extract_skill_frequency(safe_jd)
    required_skills = list(jd_skill_frequency.keys())[:10]
    if not required_skills:
        required_skills = _top_keywords(safe_jd, limit=8)

    matched_skills = [skill for skill in required_skills if skill in resume_skill_frequency]
    if not matched_skills:
        resume_tokens = set(_lower_tokens(safe_resume))
        matched_skills = [skill for skill in required_skills if set(_lower_tokens(skill)) & resume_tokens][:8]
    missing_skills = [skill for skill in required_skills if skill not in matched_skills][:8]

    achievements = _achievements_from_text(safe_resume, sections)
    semantic_fit = round(_cosine_similarity(safe_jd, safe_resume) * 100)
    skill_coverage = round((len(matched_skills) / len(required_skills)) * 100) if required_skills else semantic_fit
    experience_fit = round(_score_experience(experience_years, _extract_required_years(safe_jd), bool(sections.get("experience"))))
    evidence_quality = round(_score_evidence(sections, achievements, companies, titles))
    communication = round(_score_communication(safe_resume, sections, emails, phones))

    overall_score = round(
        (skill_coverage * 0.34)
        + (semantic_fit * 0.24)
        + (experience_fit * 0.18)
        + (evidence_quality * 0.14)
        + (communication * 0.10)
    )
    overall_score = max(18, min(overall_score, 98))

    target_role = _extract_target_role(safe_jd)
    fit_band = _fit_band(overall_score)
    confidence = _confidence_label(evidence_quality, matched_skills, missing_skills)
    recommendation = _recommendation(overall_score)
    search_priority = _search_priority(overall_score)
    seniority = _seniority_label(experience_years, titles)
    role_context = _role_context(target_role)

    strong_sections = _strong_sections(sections, emails, phones, achievements)
    weak_sections = _weak_sections(sections, emails, phones, achievements)
    suggested_keywords = _unique(missing_skills + [skill for skill in required_skills if skill not in missing_skills], limit=8)
    top_resume_skills = list(resume_skill_frequency.keys())[:10]
    if not top_resume_skills:
        top_resume_skills = _top_keywords(safe_resume, limit=8)

    risk_flags = []
    if missing_skills:
        risk_flags.append(f"Core gap: {missing_skills[0]}")
    if experience_years == 0:
        risk_flags.append("Experience level is difficult to verify from the resume")
    if not achievements:
        risk_flags.append("Resume lacks strong quantified outcomes")
    if not emails or not phones:
        risk_flags.append("Contact details are incomplete")
    if weak_sections:
        risk_flags.append(weak_sections[0])
    risk_flags = _unique(risk_flags, limit=5)

    evidence_highlights = _unique(
        achievements
        + [f"Matched JD skill: {skill}" for skill in matched_skills[:3]]
        + ([f"Detected experience span: {experience_years}+ years"] if experience_years else [])
        + [f"Prior roles include {titles[0]}" for _ in [0] if titles]
    , limit=5)

    executive_summary_parts = []
    if matched_skills:
        executive_summary_parts.append(
            f"{_guess_name(safe_resume, file_name)} shows {fit_band.lower()} for {role_context}, with direct evidence in {', '.join(matched_skills[:3])}."
        )
    else:
        executive_summary_parts.append(
            f"{_guess_name(safe_resume, file_name)} is a {fit_band.lower()} match for {role_context}, but the resume does not clearly surface direct keyword overlap."
        )
    if missing_skills:
        executive_summary_parts.append(
            f"The main decision risk is missing or weak evidence for {', '.join(missing_skills[:2])}."
        )
    elif achievements:
        executive_summary_parts.append("The resume includes measurable outcomes, which supports a deeper interview.")
    else:
        executive_summary_parts.append("The profile is directionally promising, but the evidence quality is lighter than a final-round profile.")
    executive_summary = " ".join(executive_summary_parts)

    leadership = round(_score_leadership(safe_resume))
    problem_solving = round(_score_problem_solving(safe_resume))
    technical = round((skill_coverage * 0.55) + (semantic_fit * 0.45))

    return {
        "extracted_data": {
            "name": _guess_name(safe_resume, file_name),
            "email": emails[0] if emails else "N/A",
            "phone": phones[0] if phones else "N/A",
            "location": location or "Unknown",
            "education": education if education else "Not clearly stated",
            "job_titles": titles if titles else [],
            "companies": companies if companies else [],
            "skills": top_resume_skills,
            "total_experience_years": f"{experience_years}+ years" if experience_years else "Not clearly stated",
        },
        "analysis": {
            "overall_score": overall_score,
            "matched_skills": matched_skills[:8],
            "missing_skills": missing_skills[:8],
            "strong_sections": strong_sections,
            "weak_sections": weak_sections,
            "suggested_keywords": suggested_keywords,
            "verdict": "Strong Candidate" if overall_score >= 80 else "Promising Candidate" if overall_score >= 60 else "Needs Validation" if overall_score >= 45 else "Low Fit",
        },
        "intelligence": {
            "target_role": target_role,
            "fit_band": fit_band,
            "seniority": seniority,
            "confidence": confidence,
            "hiring_recommendation": recommendation,
            "search_priority": search_priority,
            "executive_summary": executive_summary,
            "score_breakdown": {
                "skill_coverage": skill_coverage,
                "semantic_fit": semantic_fit,
                "experience_fit": experience_fit,
                "evidence_quality": evidence_quality,
                "communication": communication,
            },
            "risk_flags": risk_flags,
            "evidence_highlights": evidence_highlights,
            "interview_questions": _build_interview_questions(matched_skills, missing_skills, target_role, achievements),
            "next_steps": _build_next_steps(overall_score, missing_skills, confidence),
        },
        "visualization_data": {
            "skill_frequency": dict(list(resume_skill_frequency.items())[:8]) or {"General Fit": max(1, overall_score // 20)},
            "radar_stats": {
                "Technical": technical,
                "Leadership": leadership,
                "Problem Solving": problem_solving,
                "Communication": communication,
                "Experience": experience_fit,
            },
        },
        "meta": {
            "required_skills_considered": required_skills[:10],
            "resume_length": len(_clean_space(safe_resume)),
        },
    }

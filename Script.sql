-- ===========================================================================
-- TALENT-OS: MySQL Database Initialization Script
-- ===========================================================================



-- ===========================================================================
-- TABLE CREATION
-- ===========================================================================

-- 1. Candidates Table: Stores analyzed resume information
CREATE TABLE IF NOT EXISTS candidates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    skills TEXT,
    experience VARCHAR(50),
    education VARCHAR(100),
    resume_score INT,
    upload_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 2. Platform Users Table: Stores authenticated Google profiles
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
    auth_provider VARCHAR(40) DEFAULT 'google',
    last_login_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    login_count INT DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 3. Jobs Table: Stores job descriptions and mission briefs
CREATE TABLE IF NOT EXISTS jobs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    job_title VARCHAR(100),
    job_description TEXT,
    required_skills TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 4. Match Results Table: Stores candidate vs job match scores
CREATE TABLE IF NOT EXISTS match_results (
    id INT AUTO_INCREMENT PRIMARY KEY,
    candidate_id INT,
    job_id INT,
    match_score INT,
    matched_skills TEXT,
    missing_skills TEXT,
    FOREIGN KEY (candidate_id) REFERENCES candidates(id) ON DELETE CASCADE,
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

-- 5. Financial Documents Table: For Financial Analyzer datasets
CREATE TABLE IF NOT EXISTS financial_docs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_name VARCHAR(255),
    summary TEXT,
    insights TEXT,
    upload_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 6. Chat History Table: Stores AI conversations and directives
CREATE TABLE IF NOT EXISTS chat_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_query TEXT,
    ai_response TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ===========================================================================
-- OPTIONAL: Insert some test data to verify the setup
-- ===========================================================================

INSERT INTO jobs (job_title, job_description, required_skills)
VALUES ('Data Scientist', 'Looking for an AI specialist.', 'Python, ML, SQL')
ON DUPLICATE KEY UPDATE id = id;

INSERT INTO candidates (name, email, skills, experience, education, resume_score)
VALUES ('Piyush Patani', 'piyush@gmail.com', 'Python, ML, SQL', '2 years', 'M.Sc. IT', 77)
ON DUPLICATE KEY UPDATE id = id;

select * from candidates c 
select * from match_results c
select * from jobs c
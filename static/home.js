const authLaunchButton = document.getElementById('auth-launch-button');
const authModal = document.getElementById('auth-modal');
const googleLoginButton = document.getElementById('google-login-button');
const googleLoginLabel = document.getElementById('google-login-label');
const authModeSignupButton = document.getElementById('auth-mode-signup');
const authModeLoginButton = document.getElementById('auth-mode-login');
const signupPanel = document.getElementById('signup-panel');
const loginPanel = document.getElementById('login-panel');
const otpPanel = document.getElementById('otp-panel');
const authStatus = document.getElementById('auth-status');
const signupNameField = document.getElementById('signup-name-field');
const signupEmailField = document.getElementById('signup-email-field');
const signupPasswordField = document.getElementById('signup-password-field');
const signupRequestButton = document.getElementById('signup-request-button');
const signupOtpField = document.getElementById('signup-otp-field');
const otpEmailTarget = document.getElementById('otp-email-target');
const otpDeliveryNote = document.getElementById('otp-delivery-note');
const verifyOtpButton = document.getElementById('verify-otp-button');
const resendOtpButton = document.getElementById('resend-otp-button');
const loginEmailField = document.getElementById('login-email-field');
const loginPasswordField = document.getElementById('login-password-field');
const emailLoginButton = document.getElementById('email-login-button');
const appUrl = document.body?.dataset.appUrl || '/app';
let pendingSignupEmail = '';

function openAuthModal(mode = 'signup') {
    setAuthMode(mode);
    authModal?.classList.add('is-open');
    authModal?.setAttribute('aria-hidden', 'false');
    document.body.classList.add('modal-open');
    window.requestAnimationFrame(() => {
        const focusTarget = mode === 'login' ? loginEmailField : signupNameField;
        focusTarget?.focus();
    });
}

function closeAuthModal() {
    authModal?.classList.remove('is-open');
    authModal?.setAttribute('aria-hidden', 'true');
    document.body.classList.remove('modal-open');
}

function setAuthStatus(message, tone = 'info') {
    if (!authStatus) return;
    authStatus.textContent = message || '';
    authStatus.classList.remove('is-success', 'is-error', 'is-visible');
    if (!message) return;
    if (tone === 'success') authStatus.classList.add('is-success');
    if (tone === 'error') authStatus.classList.add('is-error');
    authStatus.classList.add('is-visible');
}

function setButtonLoading(button, isLoading, loadingLabel) {
    if (!button) return;
    if (!button.dataset.defaultLabel) {
        button.dataset.defaultLabel = button.textContent.trim();
    }
    button.disabled = !!isLoading;
    button.textContent = isLoading ? loadingLabel : button.dataset.defaultLabel;
}

function setAuthMode(mode) {
    const signupActive = mode !== 'login';
    authModeSignupButton?.classList.toggle('active', signupActive);
    authModeLoginButton?.classList.toggle('active', !signupActive);
    signupPanel?.classList.toggle('is-hidden', !signupActive);
    loginPanel?.classList.toggle('is-hidden', signupActive);
    otpPanel?.classList.toggle('is-hidden', !signupActive || !pendingSignupEmail);
    if (!signupActive && otpDeliveryNote) otpDeliveryNote.textContent = '';
    setAuthStatus('');
}

async function postJson(url, payload) {
    const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    });

    let data = {};
    try {
        data = await response.json();
    } catch (error) {
        data = {};
    }

    if (!response.ok || data.success === false) {
        throw new Error(data.error || data.message || `Request failed with status ${response.status}.`);
    }
    return data;
}

async function requestSignupOtp() {
    const fullName = signupNameField?.value.trim() || '';
    const email = signupEmailField?.value.trim() || '';
    const password = signupPasswordField?.value || '';

    if (fullName.length < 2) {
        setAuthStatus('Enter your full name to continue.', 'error');
        signupNameField?.focus();
        return;
    }
    if (!email) {
        setAuthStatus('Enter your email address to receive the verification code.', 'error');
        signupEmailField?.focus();
        return;
    }
    if (password.length < 8) {
        setAuthStatus('Create a password with at least 8 characters.', 'error');
        signupPasswordField?.focus();
        return;
    }

    setButtonLoading(signupRequestButton, true, 'Sending code...');
    try {
        const data = await postJson('/auth/email/request-otp', {
            full_name: fullName,
            email,
            password
        });
        pendingSignupEmail = data.email || email;
        if (otpEmailTarget) otpEmailTarget.textContent = pendingSignupEmail;
        if (otpDeliveryNote) {
            otpDeliveryNote.textContent = data.delivery === 'smtp'
                ? 'Check the inbox and spam folder for the Tensai.AI verification email.'
                : 'SMTP is not fully configured yet, so the verification code was written to the backend console for development.';
        }
        otpPanel?.classList.remove('is-hidden');
        setAuthStatus(data.message || 'Verification code sent. Check your inbox for the 6-digit code.', 'success');
        signupOtpField?.focus();
    } catch (error) {
        setAuthStatus(error.message, 'error');
    } finally {
        setButtonLoading(signupRequestButton, false, 'Sending code...');
    }
}

async function verifySignupOtp() {
    const otp = signupOtpField?.value.trim() || '';
    if (!pendingSignupEmail) {
        setAuthStatus('Start signup first so Tensai.AI knows where to send the verification code.', 'error');
        return;
    }
    if (otp.length !== 6) {
        setAuthStatus('Enter the 6-digit verification code from your email.', 'error');
        signupOtpField?.focus();
        return;
    }

    setButtonLoading(verifyOtpButton, true, 'Verifying...');
    try {
        const data = await postJson('/auth/email/verify-otp', {
            email: pendingSignupEmail,
            otp
        });
        setAuthStatus(data.message || 'Email verified. Opening your workspace...', 'success');
        window.setTimeout(() => {
            window.location.href = data.redirect || appUrl;
        }, 220);
    } catch (error) {
        setAuthStatus(error.message, 'error');
    } finally {
        setButtonLoading(verifyOtpButton, false, 'Verifying...');
    }
}

async function resendSignupOtp() {
    if (!pendingSignupEmail) {
        setAuthStatus('Fill the signup form first to request a verification code.', 'error');
        return;
    }
    await requestSignupOtp();
}

async function loginWithEmail() {
    const email = loginEmailField?.value.trim() || '';
    const password = loginPasswordField?.value || '';

    if (!email) {
        setAuthStatus('Enter your email address to log in.', 'error');
        loginEmailField?.focus();
        return;
    }
    if (!password) {
        setAuthStatus('Enter your password to log in.', 'error');
        loginPasswordField?.focus();
        return;
    }

    setButtonLoading(emailLoginButton, true, 'Logging in...');
    try {
        const data = await postJson('/auth/email/login', { email, password });
        setAuthStatus(data.message || 'Login successful. Opening your workspace...', 'success');
        window.setTimeout(() => {
            window.location.href = data.redirect || appUrl;
        }, 180);
    } catch (error) {
        setAuthStatus(error.message, 'error');
    } finally {
        setButtonLoading(emailLoginButton, false, 'Logging in...');
    }
}

authLaunchButton?.addEventListener('click', () => openAuthModal('signup'));
authModeSignupButton?.addEventListener('click', () => setAuthMode('signup'));
authModeLoginButton?.addEventListener('click', () => setAuthMode('login'));
signupRequestButton?.addEventListener('click', requestSignupOtp);
verifyOtpButton?.addEventListener('click', verifySignupOtp);
resendOtpButton?.addEventListener('click', resendSignupOtp);
emailLoginButton?.addEventListener('click', loginWithEmail);

if (googleLoginButton) {
    googleLoginButton.addEventListener('click', () => {
        googleLoginButton.classList.add('is-loading');
        googleLoginLabel.textContent = 'Connecting Google...';
        window.setTimeout(() => {
            window.location.href = googleLoginButton.dataset.loginUrl;
        }, 160);
    });
}

[signupNameField, signupEmailField, signupPasswordField].forEach((field) => {
    field?.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') {
            event.preventDefault();
            requestSignupOtp();
        }
    });
});

signupOtpField?.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') {
        event.preventDefault();
        verifySignupOtp();
    }
});

[loginEmailField, loginPasswordField].forEach((field) => {
    field?.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') {
            event.preventDefault();
            loginWithEmail();
        }
    });
});

document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') closeAuthModal();
});

setAuthMode('signup');

const observer = new IntersectionObserver((entries) => {
    entries.forEach((entry) => {
        if (entry.isIntersecting) {
            entry.target.classList.add('is-visible');
            observer.unobserve(entry.target);
        }
    });
}, { threshold: 0.12 });

document.querySelectorAll('.reveal').forEach((element) => {
    if (!element.classList.contains('is-visible')) {
        observer.observe(element);
    }
});

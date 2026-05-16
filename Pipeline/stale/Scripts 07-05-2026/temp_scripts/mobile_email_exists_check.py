import re
import dns.resolver

# ----------------------------
# Phone rules
# ----------------------------

INDIAN_MOBILE_REGEX = re.compile(r"^[6-9][0-9]{9}$")

FAKE_PHONE_NUMBERS = {
    "0000000000", "1111111111", "2222222222", "3333333333",
    "4444444444", "5555555555", "6666666666", "7777777777",
    "8888888888", "9999999999", "1234567890", "9876543210"
}

def is_repeated_digit_number(phone: str) -> bool:
    return len(set(phone)) == 1

def is_obvious_sequence(phone: str) -> bool:
    return phone in {"0123456789", "1234567890", "9876543210"}

def validate_phone_local(raw_phone):
    result = {
        "raw_phone": raw_phone,
        "phone_normalized": None,
        "phone_e164": None,
        "phone_status": None,
        "phone_warnings": [],
        "is_phone_valid_local": False
    }

    if raw_phone is None:
        result["phone_status"] = "PHONE_EMPTY"
        return result

    phone = str(raw_phone).strip()

    if phone == "":
        result["phone_status"] = "PHONE_EMPTY"
        return result

    # Handle leading minus
    if phone.startswith("-"):
        candidate = phone[1:].strip()

        if candidate.isdigit():
            phone = candidate
            result["phone_warnings"].append("LEADING_MINUS_REMOVED")
        else:
            result["phone_status"] = "PHONE_INVALID_NON_DIGIT"
            return result

    if not phone.isdigit():
        result["phone_status"] = "PHONE_INVALID_NON_DIGIT"
        return result

    if len(phone) != 10:
        result["phone_status"] = "PHONE_INVALID_LENGTH"
        return result

    if not INDIAN_MOBILE_REGEX.match(phone):
        result["phone_status"] = "PHONE_INVALID_START_DIGIT"
        return result

    if phone in FAKE_PHONE_NUMBERS:
        result["phone_status"] = "PHONE_INVALID_SEQUENCE"
        return result

    if is_repeated_digit_number(phone):
        result["phone_status"] = "PHONE_INVALID_REPEATED_DIGIT"
        return result

    if is_obvious_sequence(phone):
        result["phone_status"] = "PHONE_INVALID_SEQUENCE"
        return result

    result["phone_normalized"] = phone
    result["phone_e164"] = "+91" + phone
    result["phone_status"] = "PHONE_VALID_LOCAL"
    result["is_phone_valid_local"] = True

    return result


# ----------------------------
# Email rules
# ----------------------------

EMAIL_REGEX = re.compile(
    r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$"
)

INVISIBLE_CHARS_REGEX = re.compile(r"[\u200b\u200c\u200d\ufeff]")
WHITESPACE_REGEX = re.compile(r"\s+")

# Practical allowed character set for your POC.
# Anything outside this should be rejected, not silently removed.
INVALID_EMAIL_VISIBLE_CHARS_REGEX = re.compile(r"[^a-z0-9@._%+\-]")

DISPOSABLE_DOMAINS = {
    "mailinator.com",
    "10minutemail.com",
    "tempmail.com",
    "guerrillamail.com"
}

COMMON_DOMAIN_TYPOS = {
    "gamil.com": "gmail.com",
    "gmial.com": "gmail.com",
    "gnail.com": "gmail.com",
    "gmai.com": "gmail.com",
    "yaho.com": "yahoo.com",
    "yhoo.com": "yahoo.com",
    "hotmial.com": "hotmail.com",
    "outlok.com": "outlook.com",
    "redifmail.com": "rediffmail.com"
}

def normalize_email_basic(raw_email):
    if raw_email is None:
        return None

    email = str(raw_email).strip().lower()
    email = INVISIBLE_CHARS_REGEX.sub("", email)
    email = WHITESPACE_REGEX.sub("", email)

    return email

def dns_record_exists(domain: str, record_type: str) -> bool:
    try:
        answers = dns.resolver.resolve(domain, record_type)
        return len(answers) > 0
    except Exception:
        return False

def validate_email_local(raw_email):
    result = {
        "raw_email": raw_email,
        "email_normalized": None,
        "email_status": None,
        "email_warnings": [],
        "domain": None,
        "suggested_email": None,
        "dns_found": False,
        "mx_found": False,
        "is_email_valid_local": False
    }

    email = normalize_email_basic(raw_email)

    if not email:
        result["email_status"] = "EMAIL_EMPTY"
        return result

    result["email_normalized"] = email

    # Reject visible illegal chars like / \ , ; : etc.
    if INVALID_EMAIL_VISIBLE_CHARS_REGEX.search(email):
        result["email_status"] = "EMAIL_INVALID_CHARACTERS"
        return result

    if not EMAIL_REGEX.match(email):
        result["email_status"] = "EMAIL_INVALID_FORMAT"
        return result

    local_part, domain = email.split("@", 1)
    result["domain"] = domain

    if domain in COMMON_DOMAIN_TYPOS:
        suggested_domain = COMMON_DOMAIN_TYPOS[domain]
        result["suggested_email"] = f"{local_part}@{suggested_domain}"
        result["email_status"] = "EMAIL_DOMAIN_SUSPECTED_TYPO"
        return result

    if domain in DISPOSABLE_DOMAINS:
        result["email_status"] = "EMAIL_DISPOSABLE_DOMAIN"
        return result

    dns_found = (
        dns_record_exists(domain, "A") or
        dns_record_exists(domain, "AAAA") or
        dns_record_exists(domain, "MX")
    )

    result["dns_found"] = dns_found

    if not dns_found:
        result["email_status"] = "EMAIL_DOMAIN_NO_DNS"
        return result

    mx_found = dns_record_exists(domain, "MX")
    result["mx_found"] = mx_found

    if not mx_found:
        result["email_status"] = "EMAIL_NO_MX"
        return result

    result["email_status"] = "EMAIL_VALID_LOCAL"
    result["is_email_valid_local"] = True

    return result
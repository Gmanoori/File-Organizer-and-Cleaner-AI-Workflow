import re
import hashlib

def normalize_address(address: str) -> str:
    if not address:
        return ""
    
    # Lowercase
    address = address.lower()
    
    # Remove punctuation (keep alphanumeric and spaces)
    address = re.sub(r'[^\w\s]', ' ', address)
    
    # Collapse whitespace and trim
    address = " ".join(address.split())
    
    return address

def generate_hash(normalized_address: str) -> str:
    return hashlib.sha256(normalized_address.encode('utf-8')).hexdigest()



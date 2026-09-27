from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

print("Generating 2048-bit RSA key pair...")

# 1. Generate private key
private_key = rsa.generate_private_key(
    public_exponent=65537,
    key_size=2048,
)

# Choose a secure passphrase for your key
passphrase = b"sushovan_windows_snowflake_key"  # Change this to your password

# 2. Serialize and encrypt private key to PKCS#8 format
pem_private = private_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.BestAvailableEncryption(passphrase)
)

with open("rsa_key.p8", "wb") as f:
    f.write(pem_private)

# 3. Extract and serialize public key
public_key = private_key.public_key()
pem_public = public_key.public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo
)

with open("rsa_key.pub", "wb") as f:
    f.write(pem_public)

print("Success! Generated 'rsa_key.p8' and 'rsa_key.pub'.")
print("\n--- COPY THIS FOR SNOWFLAKE ---")
print(pem_public.decode('utf-8'))
print("---------------------------------")
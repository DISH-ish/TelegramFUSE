import os
import secrets
import base64
from dotenv import load_dotenv
from fuse_impl import runFs
from TelegramFUSE import TelegramFileClient

def init():
    load_dotenv()

    api_id = os.getenv("APP_ID")
    api_hash = os.getenv("APP_HASH")
    channel_link = os.getenv("CHANNEL_LINK")
    session_name = os.getenv("SESSION_NAME")
    encryption_key = os.getenv("ENCRYPTION_KEY", "")
    
    if not encryption_key.strip(): # Check if encryption key is empty
        print("ENCRYPTION_KEY is not set or empty.")
        response = input("Do you want to generate an encryption key? (y/n): ").strip().lower()
        if response in ['y', 'yes']: # Generate a key
            new_key = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()
            with open('.env', 'r') as f: # Replace the line in the .env file
                lines = f.readlines()
            with open('.env', 'w') as f:
                for line in lines:
                    if line.strip().startswith('ENCRYPTION_KEY='):
                        f.write(f'ENCRYPTION_KEY={new_key}\n')
                    else:
                        f.write(line)
            print(f"✓ Encryption key generated and saved to .env")
            print(f"  Key: {new_key}")
            os.environ["ENCRYPTION_KEY"] = new_key  # Set the key in current environment
        else:
            print("Running without encryption.")

    client = TelegramFileClient(session_name, api_id, api_hash, channel_link)
    runFs(client)

if __name__ == "__main__":
    init()

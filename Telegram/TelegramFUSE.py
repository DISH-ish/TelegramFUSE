import logging
logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
from telethon import TelegramClient, events, sync, utils
from dotenv import load_dotenv
import os
from io import BytesIO
from cryptography.fernet import Fernet
from collections import defaultdict
from cachetools import LRUCache, TTLCache
import gc
import time

load_dotenv()

# Constants
FILE_MAX_SIZE_BYTES = int(2 * 1e9)  # 2GB
CACHE_MAXSIZE = 1e9  # 1GB cache
CACHE_MAX_FILE_SIZE = 100 * 1024 * 1024  # Don't cache files larger than 100MB
CACHE_TTL = 300  # 5 minutes TTL for cached items

def getsizeofelt(val):
    try:
        return len(val)
    except:
        return 1

class TelegramFileClient():
    def __init__(self, session_name, api_id, api_hash, channel_link):
        self.client = TelegramClient(session_name, api_id, api_hash)
        self.client.start()
        self.channel_entity = self.client.get_entity(channel_link)
        # key to use for encryption, if not set, not encrypted.
        self.encryption_key = os.getenv("ENCRYPTION_KEY")
        
        # Use TTLCache for automatic expiration
        self.cached_files = TTLCache(maxsize=CACHE_MAXSIZE, ttl=CACHE_TTL, getsizeof=getsizeofelt)
        self.fname_to_msgs = defaultdict(tuple)

        print(f"Cache configured: maxsize={CACHE_MAXSIZE}, TTL={CACHE_TTL}s")
        print(f"Using encryption: {self.encryption_key is not None}")

    def upload_file(self, bytesio, fh, file_name=None, progress_callback=None):
        # Invalidate cache as soon as we upload file
        if fh in self.cached_files:
            self.cached_files.pop(fh)
            print("Cleaned up cache for file", fh)

        # file_bytes is bytesio obj
        file_bytes = bytesio.read()
        original_size = len(file_bytes)
        
        if self.encryption_key is not None:
            print(f"Encrypting {file_name or fh}: {original_size} bytes -> ", end="")
            f = Fernet(bytes(self.encryption_key, 'utf-8'))
            file_bytes = f.encrypt(file_bytes)
            encrypted_size = len(file_bytes)
            print(f"{encrypted_size} bytes (+{encrypted_size - original_size} bytes overhead)")
        else:
            encrypted_size = original_size

        # Total size is the encrypted size (or original if no encryption)
        total_upload_size = len(file_bytes)

        chunks = []
        if total_upload_size > FILE_MAX_SIZE_BYTES:
            # Calculate the number of chunks needed
            num_chunks = (total_upload_size + FILE_MAX_SIZE_BYTES - 1) // FILE_MAX_SIZE_BYTES
            print(f"Splitting file into {num_chunks} chunks")
            
            # Split the file into chunks
            for i in range(num_chunks):
                start = i * FILE_MAX_SIZE_BYTES
                end = min((i + 1) * FILE_MAX_SIZE_BYTES, total_upload_size)
                chunk = file_bytes[start:end]
                chunks.append(chunk)
        else:
            # File is within the size limit, no need to split
            chunks = [file_bytes]

        upload_results = []
        fname = file_name or f"file_{fh}"
        
        uploaded_so_far = [0]  # Use list for mutability
        
        for i, chunk in enumerate(chunks):
            chunk_name = f"{fname}_part{i}.txt"
            chunk_size = len(chunk)
            
            print(f"Uploading chunk {i+1}/{len(chunks)} ({chunk_size} bytes)")
            
            if progress_callback:
                # Capture current values for this chunk
                current_chunk_start = uploaded_so_far[0]
                
                def create_progress_callback(chunk_start, fname, original, encrypted, total):
                    last_reported = [0]
                    def chunk_progress(sent_bytes, _):
                        overall_sent = chunk_start + sent_bytes
                        percent = int((overall_sent / total) * 100)
                        if original != encrypted:
                            display_sent = int(overall_sent * (original / encrypted))
                            if display_sent > original:
                                display_sent = original
                        else:
                            display_sent = overall_sent
                        
                        # Report progress every 10%
                        if percent >= last_reported[0] + 10:
                            print(f"Uploading {fname}: {percent}% ({display_sent}/{original} bytes)")
                            last_reported[0] = percent
                        progress_callback(overall_sent, total)
                    
                    return chunk_progress
                
                chunk_progress_cb = create_progress_callback(
                    current_chunk_start, fname, original_size, 
                    encrypted_size, total_upload_size
                )
            else:
                chunk_progress_cb = None
            
            print(f"Uploading chunk {i+1}/{len(chunks)} ({chunk_size} bytes)")
            
            f = self.client.upload_file(chunk, file_name=chunk_name, 
                                       part_size_kb=512, 
                                       progress_callback=chunk_progress_cb)
            result = self.client.send_file(self.channel_entity, f)
            upload_results.append(result)
            uploaded_so_far[0] += chunk_size

        self.fname_to_msgs[file_name] = tuple([m.id for m in upload_results])
        
        # Only cache if file is not too large
        if total_upload_size <= CACHE_MAX_FILE_SIZE:
            self.cached_files[fh] = file_bytes
            print(f"Cached file {fh} ({total_upload_size} bytes). Cache size: {self.cached_files.currsize}/{self.cached_files.maxsize}")
        else:
            print(f"File {fh} too large for cache ({total_upload_size} > {CACHE_MAX_FILE_SIZE})")
        
        return upload_results

    def get_cached_file(self, fh):
        if fh in self.cached_files:
            print(f"Cache hit for file {fh}")
            return self.cached_files[fh]
        return None

    def download_file(self, fh, msgIds):
        # Check cache first
        cached = self.get_cached_file(fh)
        if cached is not None:
            return cached
        
        print(f"Downloading file {fh} from Telegram ({len(msgIds)} parts)")
        
        msgs = self.get_messages(msgIds)
        buf = BytesIO()
        total_encrypted_size = 0
        downloaded_size = 0
        
        # Try to get total size from messages
        for m in msgs:
            if hasattr(m, 'document') and hasattr(m.document, 'size'):
                total_encrypted_size += m.document.size
        
        for i, m in enumerate(msgs):
            part_size = m.document.size if (hasattr(m, 'document') and hasattr(m.document, 'size')) else 0
            print(f"Downloading part {i+1}/{len(msgs)} ({part_size} bytes)")
            
            # Create progress callback for this part with proper closure
            if part_size > 0:
                def make_progress_callback(part_idx, part_total):
                    last_reported = [0]
                    def progress_callback(received_bytes, total):
                        percent = int((received_bytes / total) * 100)
                        if percent >= last_reported[0] + 10:  # Report every 10%
                            print(f"Part {part_idx+1}: {percent}%")
                            last_reported[0] = percent
                    return progress_callback
                
                progress_cb = make_progress_callback(i, part_size)
            else:
                progress_cb = None
            
            result = self.download_message(m, progress_callback=progress_cb)
            buf.write(result)
            downloaded_size += len(result)
        
        numBytes = buf.getbuffer().nbytes
        print(f"Downloaded file {fh} is {numBytes} bytes")
        buf.seek(0)
        readBytes = buf.read()

        if self.encryption_key is not None:
            print(f"Decrypting {fh}")
            f = Fernet(bytes(self.encryption_key, 'utf-8'))
            readBytes = f.decrypt(readBytes)

        barr = bytearray(readBytes)
        
        # Only cache if file is not too large
        # Note: We cache the encrypted data, so use encrypted size
        if numBytes <= CACHE_MAX_FILE_SIZE:
            self.cached_files[fh] = barr
            print(f"Cached downloaded file {fh}")
        else:
            print(f"File {fh} too large for cache ({numBytes} > {CACHE_MAX_FILE_SIZE})")
            
        return barr

    def get_messages(self, ids):
        result = self.client.get_messages(self.channel_entity, ids=ids)
        return result

    def download_message(self, msg, progress_callback=None):
        return msg.download_media(bytes, progress_callback=progress_callback)
    
    def delete_messages(self, ids):
        if ids:
            print(f"Deleting {len(ids)} messages from Telegram")
            return self.client.delete_messages(self.channel_entity, message_ids=ids)
        return None

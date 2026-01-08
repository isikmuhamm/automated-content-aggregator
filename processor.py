"""
Content Processor Module

This module handles email content extraction, PDF processing using Poppler,
image processing, and normalization of unstructured document data into
structured JSON format for the publishing layer.
"""

import os
import re
import io
import json
import email
import base64
import quopri
import hashlib
import logging
from PIL import Image
from pathlib import Path
from email.header import decode_header
from email.utils import parseaddr
from pdf2image import convert_from_path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_config():
    """
    Load configuration from a JSON file.
    
    Returns:
        Dictionary containing configuration parameters.
        
    Raises:
        FileNotFoundError: If config.json does not exist.
    """
    config_path = Path("config.json")
    if not config_path.exists():
        raise FileNotFoundError("config.json file not found!")
    
    with open(config_path, "r", encoding="utf-8") as config_file:
        return json.load(config_file)


def save_config(config):
    """
    Save configuration to a JSON file.
    
    Args:
        config: Configuration dictionary to save.
    """
    config_path = Path("config.json")
    with open(config_path, "w", encoding="utf-8") as config_file:
        json.dump(config, config_file, indent=2)


def save_processed_uid(uid, config):
    """
    Save a processed UID to the configuration file.
    
    Args:
        uid: Email UID that was processed.
        config: Configuration dictionary to update.
    """
    # Get processed UIDs as a single line setting
    processed_uids = config.get("processed_uids", "")
    
    # Split UIDs by comma into a list
    uid_list = processed_uids.split(",") if processed_uids else []

    if uid not in uid_list:
        uid_list.append(uid)

    # Rejoin UIDs as single line and save to config
    config["processed_uids"] = ",".join(uid_list)
    save_config(config)


def decode_string(input_bytes, default_charset='utf-8'):
    """
    Decode bytes to string with multiple charset fallbacks.
    
    Args:
        input_bytes: Bytes or string to decode.
        default_charset: Primary charset to try.
        
    Returns:
        Decoded string.
    """
    if isinstance(input_bytes, str):
        return input_bytes
    elif input_bytes is None:
        return ''
    
    charsets = [default_charset, 'utf-8', 'iso-8859-9', 'latin-1', 'cp1254', 'ascii']
    for charset in charsets:
        try:
            return input_bytes.decode(charset)
        except (UnicodeDecodeError, AttributeError):
            continue
    
    return input_bytes.decode('utf-8', errors='ignore')


def decode_mime_words(header_string):
    """
    Decode MIME-encoded header words.
    
    Args:
        header_string: MIME-encoded header string.
        
    Returns:
        Decoded header string.
    """
    if not header_string:
        return ''
    return ''.join(
        decode_string(word, charset or 'utf-8')
        for word, charset in decode_header(header_string)
    )


def decode_subject(msg):
    """
    Decode email subject from message object.
    
    Args:
        msg: Email message object.
        
    Returns:
        Decoded subject string.
    """
    try:
        subject = decode_mime_words(msg.get("Subject", ""))
        return subject
    except Exception as error:
        logger.error(f"Error decoding subject: {error}")
        return "Subject could not be decoded"


def decode_sender(msg):
    """
    Decode sender information from message object.
    
    Args:
        msg: Email message object.
        
    Returns:
        Formatted sender string with name and email.
    """
    try:
        from_header = msg.get("From", "")
        if not from_header:
            return "No sender information"
        
        from_string = decode_mime_words(from_header)
        
        name, email_address = parseaddr(from_string)
        if name:
            return f"{name} <{email_address}>"
        return email_address
    except Exception as error:
        logger.error(f"Error decoding sender information: {error}")
        return "Sender information could not be decoded"


def get_email_content(msg):
    """
    Extract text, HTML content and attachments from email message.
    
    Args:
        msg: Email message object.
        
    Returns:
        Tuple of (text_content_list, html_content_list, attachments_list).
    """
    text_content = []
    html_content = []
    attachments = []
    
    def extract_content(part):
        try:
            content = part.get_payload(decode=True)
            charset = part.get_content_charset() or 'utf-8'
            
            if content is None:
                return

            if part.get('Content-Transfer-Encoding', '').lower() == 'quoted-printable':
                content = quopri.decodestring(content).decode(charset, errors='ignore')
            elif part.get('Content-Transfer-Encoding', '').lower() == 'base64':
                content = base64.b64decode(content)
            
            decoded_content = decode_string(content, charset)
            
            if part.get_content_type() == 'text/plain':
                text_content.append(decoded_content)
            elif part.get_content_type() == 'text/html':
                html_content.append(decoded_content)
            elif part.get_filename():
                attachments.append((part.get_filename(), content))
        except Exception as error:
            logger.error(f"Error extracting content: {error}")
            logger.error(f"Problematic content: {content[:100] if content else 'Empty content'}...")
    
    if msg.is_multipart():
        for part in msg.walk():
            extract_content(part)
    else:
        extract_content(msg)
    
    return text_content, html_content, attachments


class ImageProcessor:
    """
    Handles image processing including deduplication, format conversion,
    and PDF page extraction.
    """
    
    def __init__(self, output_dir: str):
        """
        Initialize the image processor.
        
        Args:
            output_dir: Directory to save processed images.
        """
        self.output_dir = output_dir
        self.processed_hashes = {}
        
    def calculate_image_hash(self, image_data: bytes) -> str:
        """
        Calculate MD5 hash of image data for deduplication.
        
        Args:
            image_data: Raw image bytes.
            
        Returns:
            MD5 hash string.
        """
        return hashlib.md5(image_data).hexdigest()
    
    def process_single_image(self, image_data: bytes, uid: str, index: int, prefix: str):
        """
        Process and save a single image.
        
        Args:
            image_data: Raw image bytes.
            uid: Email UID for filename.
            index: Image index for filename.
            prefix: Filename prefix.
            
        Returns:
            Path to saved image or None if already processed.
        """
        image_hash = self.calculate_image_hash(image_data)
        
        if image_hash in self.processed_hashes:
            logger.info(f"This image was already saved: {self.processed_hashes[image_hash]}")
            return self.processed_hashes[image_hash]
            
        try:
            img = Image.open(io.BytesIO(image_data))
            output_path = os.path.join(self.output_dir, f"{uid}_{prefix}{index}.jpg")
            
            if img.format != 'JPEG':
                img = img.convert('RGB')
            img.save(output_path, 'JPEG')
            
            self.processed_hashes[image_hash] = output_path
            return output_path
            
        except Exception as error:
            logger.error(f"Error processing image: {error}")
            return None
    
    def process_images(self, images, uid: str, prefix: str = ''):
        """
        Process multiple images.
        
        Args:
            images: List of image data bytes.
            uid: Email UID for filenames.
            prefix: Filename prefix.
            
        Returns:
            List of processed file paths.
        """
        processed_files = []
        
        for i, image_data in enumerate(images):
            file_path = self.process_single_image(image_data, uid, i, prefix)
            if file_path:
                processed_files.append(file_path)
                
        return processed_files

    def process_pdf(self, file_path: str, uid: str):
        """
        Convert PDF pages to images.
        
        Args:
            file_path: Path to PDF file.
            uid: Email UID for filenames.
            
        Returns:
            List of processed image file paths.
        """
        try:
            logger.info(f"Processing PDF: {file_path}")
            config = load_config()
            poppler_path = config.get("poppler_path", "")
            logger.info(f"Using Poppler path: {poppler_path}")
            
            if not os.path.exists(file_path):
                logger.error(f"PDF file not found: {file_path}")
                return []
            
            images = convert_from_path(file_path, first_page=1, last_page=4, poppler_path=poppler_path)
            logger.info(f"Number of pages converted: {len(images)}")
            
            processed_files = []
            
            for i, image in enumerate(images):
                logger.info(f"Processing page {i+1}...")
                img_buffer = io.BytesIO()
                image.save(img_buffer, format='JPEG')
                img_data = img_buffer.getvalue()
                
                output_file_path = self.process_single_image(img_data, uid, i, 'pdf_')
                if output_file_path:
                    processed_files.append(output_file_path)
                    logger.info(f"Page {i+1} successfully processed and saved: {output_file_path}")
                else:
                    logger.warning(f"Page {i+1} could not be processed or saved.")
            
            logger.info(f"PDF processing complete. Total files processed: {len(processed_files)}")
            return processed_files
            
        except Exception as error:
            logger.error(f"Error processing PDF: {error}")
            logger.exception("Error details:")
        return []


def sanitize_filename(filename):
    """
    Clean filename by removing invalid characters.
    
    Args:
        filename: Original filename string.
        
    Returns:
        Sanitized filename safe for filesystem.
    """
    # Remove or replace invalid characters
    cleaned = re.sub(r'[\r\n]+', ' ', filename)  # Replace line breaks with spaces
    cleaned = re.sub(r'[<>:"/\\|?*]', '_', cleaned)  # Replace other invalid chars with underscore
    cleaned = cleaned.strip()  # Remove leading/trailing whitespace
    return cleaned if cleaned else "nameless"  # Return default name if empty


def process_email_content(uid, output_dir):
    """
    Process email content from raw storage and extract all data.
    
    Args:
        uid: Email UID to process.
        output_dir: Directory to save processed content.
        
    Returns:
        List of processed file paths.
    """
    raw_content_dir = Path("rawcontent")
    raw_content_path = raw_content_dir / f"{uid}.eml"
    
    if not raw_content_path.exists():
        logger.warning(f"Raw content not found: {raw_content_path}")
        return []
    
    with open(raw_content_path, 'rb') as input_file:
        raw_content = input_file.read()
    
    msg = email.message_from_bytes(raw_content)
    image_processor = ImageProcessor(output_dir)
    processed_files = []
    
    sender = decode_sender(msg)
    recipient = msg.get("To", "")
    date = msg.get("Date", "")
    subject = decode_subject(msg)
    
    text_contents, html_contents, attachments = get_email_content(msg)
    
    # Process images and PDFs
    for part in msg.walk():
        if part.get_content_maintype() == 'image':
            content = part.get_payload(decode=True)
            processed_file = image_processor.process_single_image(content, uid, len(processed_files), 'img_')
            if processed_file:
                processed_files.append(processed_file)
                
        elif part.get_filename() and part.get_filename().lower().endswith('.pdf'):
            original_filename = part.get_filename()
            sanitized_filename = sanitize_filename(original_filename)
            temp_pdf_path = os.path.join(output_dir, f"temp_{sanitized_filename}")
            
            logger.info(f"Processing PDF file: {original_filename}")
            logger.info(f"Sanitized filename: {sanitized_filename}")
            
            try:
                with open(temp_pdf_path, 'wb') as temp_file:
                    temp_file.write(part.get_payload(decode=True))
                
                processed_files.extend(image_processor.process_pdf(temp_pdf_path, uid))
            except Exception as error:
                logger.error(f"Error processing PDF file: {error}")
            finally:
                if os.path.exists(temp_pdf_path):
                    try:
                        os.remove(temp_pdf_path)
                    except Exception as error:
                        logger.error(f"Error deleting temporary PDF file: {error}")
    
    # Save processed content as JSON
    content_dir = Path(output_dir)
    content_dir.mkdir(exist_ok=True)
    
    json_file_path = content_dir / f"{uid}.json"
    with open(json_file_path, "w", encoding="utf-8") as output_file:
        json.dump({
            'sender': sender,
            'recipient': recipient,
            'date': date,
            'subject': subject,
            'text_contents': text_contents,
            'html_contents': html_contents,
            'attachments': [name for name, _ in attachments],
            'processed_files': processed_files
        }, output_file, ensure_ascii=False, indent=2)
    
    logger.info(f"Processed content saved: {json_file_path}")
    return processed_files


def main():
    """
    Main entry point for the content processor.
    Processes all collected emails and extracts their content.
    """
    try:
        config = load_config()
        output_dir = "content"
        os.makedirs(output_dir, exist_ok=True)

        email_uids = config.get("collected_uids", "").split(",")

        for uid in email_uids:
            if not uid.strip():
                logger.warning("Skipping empty UID.")
                continue

            logger.info(f"Processing UID {uid}...")
            processed_files = process_email_content(uid, output_dir)
            if processed_files:
                logger.info(f"Processed files: {', '.join(processed_files)}")
                save_processed_uid(uid, config)
            else:
                logger.warning(f"No attachments found for UID {uid}, skipping processing.")
            
            logger.info("-" * 100)

    except Exception as error:
        logger.error(f"Error during main processing: {error}")


if __name__ == "__main__":
    main()
"""
Email Collector Module

This module handles IMAP connections to email servers, monitors inboxes
for unread emails, filters personal emails, and saves raw email content
for downstream processing.
"""

import imaplib
import email
import json
import os
from pathlib import Path
import logging

try:
    import msvcrt  # Windows-specific module
except ImportError:
    msvcrt = None  # Not available on Linux/macOS

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


def save_raw_content(uid, raw_content):
    """
    Save raw email content to disk for later processing.
    
    Args:
        uid: Unique identifier of the email.
        raw_content: Raw email bytes to save.
    """
    try:
        raw_content_dir = Path("rawcontent")
        raw_content_dir.mkdir(exist_ok=True)
        
        file_path = raw_content_dir / f"{uid}.eml"
        with open(file_path, "wb") as output_file:
            output_file.write(raw_content)
        
        logger.info(f"Raw content saved: {file_path}")
    except Exception as error:
        logger.error(f"Error saving raw content: {error}")


def is_personal_email(msg, user_email):
    """
    Check if an email is a personal email addressed directly to the user.
    
    Args:
        msg: Email message object.
        user_email: User's email address for comparison.
        
    Returns:
        True if the email is personal, False otherwise.
    """
    to_field = msg.get("To", "")
    cc_field = msg.get("Cc", "")
    bcc_field = msg.get("Bcc", "")
    
    return (user_email in to_field or "undisclosed-recipients" in to_field or
            user_email in cc_field or user_email in bcc_field or 
            to_field is None or to_field == "")


def collect_unread_emails(mail_connection, config):
    """
    Collect unread emails from the inbox and save them for processing.
    
    Args:
        mail_connection: Active IMAP connection.
        config: Configuration dictionary with email settings.
        
    Returns:
        List of newly collected email UIDs.
    """
    mail_connection.select("inbox")
    status, messages = mail_connection.uid('search', None, "UNSEEN")
    email_ids = messages[0].split()

    new_emails = []

    # Get collected UIDs as a single line setting
    collected_uids = config.get("collected_uids", "").split(",")

    for email_id in email_ids:
        email_id_str = email_id.decode()

        if email_id_str in collected_uids:
            mail_connection.uid('STORE', email_id, '+FLAGS', '\\Seen')
            continue

        status, msg_data = mail_connection.uid('fetch', email_id, '(BODY.PEEK[])')
        if status != 'OK':
            logger.warning(f"Could not fetch data for Mail ID {email_id_str}")
            continue

        email_body = msg_data[0][1]
        msg = email.message_from_bytes(email_body)

        if is_personal_email(msg, config["email"]):
            continue

        new_emails.append(email_id_str)

        # Add UID to config.json (single line format)
        if collected_uids == [""]:
            collected_uids = [email_id_str]
        else:
            collected_uids.append(email_id_str)

        config["collected_uids"] = ",".join(collected_uids)
        save_config(config)

        # Save raw content
        save_raw_content(email_id_str, email_body)

        mail_connection.uid('STORE', email_id, '+FLAGS', '\\Seen')

    return new_emails


def main():
    """
    Main entry point for the email collector.
    Connects to the IMAP server and collects unread newsletter emails.
    """
    mail_connection = None
    try:
        config = load_config()
        mail_connection = imaplib.IMAP4_SSL(config["imap_server"])
        mail_connection.login(config["email"], config["password"])

        new_emails = collect_unread_emails(mail_connection, config)

        if new_emails:
            logger.info(f"New newsletter emails collected: {', '.join(new_emails)}")
        else:
            logger.info("No new newsletter emails found.")

    except Exception as error:
        logger.error(f"An error occurred: {error}")
    
    finally:
        try:
            if mail_connection:
                mail_connection.logout()
        except:
            pass


if __name__ == "__main__":
    main()

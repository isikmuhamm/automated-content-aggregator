"""
Content Publisher Module

This module handles the publishing of processed content to various platforms.
Supported targets: Console (dry run), Telegram Bot, Discord Webhook, and Twitter/X (v2).
"""

import os
import json
import logging
import urllib.request
import urllib.parse
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_config():
    """
    Load configuration from a JSON file.
    """
    config_path = Path("config.json")
    if not config_path.exists():
        raise FileNotFoundError("config.json file not found!")
    
    with open(config_path, "r", encoding="utf-8") as config_file:
        return json.load(config_file)


def save_config(config):
    """
    Save configuration to a JSON file.
    """
    config_path = Path("config.json")
    with open(config_path, "w", encoding="utf-8") as config_file:
        json.dump(config, config_file, indent=2)


def save_published_uid(uid, config):
    """
    Save a published UID to the configuration.
    """
    published_uids = config.get("published_uids", "")
    uid_list = [u.strip() for u in published_uids.split(",") if u.strip()]
    
    if uid not in uid_list:
        uid_list.append(uid)
        
    config["published_uids"] = ",".join(uid_list)
    save_config(config)


def send_multipart_request(url, fields, files=None):
    """
    Send a multipart/form-data POST request using urllib.
    
    Args:
        url: Destination URL.
        fields: Dict of text fields.
        files: Dict of file fields, where key is field name and value is file path.
        
    Returns:
        Response body as bytes.
    """
    import uuid
    boundary = f"Boundary-{uuid.uuid4().hex}"
    
    body = bytearray()
    
    # Add fields
    for name, value in fields.items():
        body.extend(f"--{boundary}\r\n".encode())
        body.extend(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode())
        body.extend(f"{value}\r\n".encode())
        
    # Add files
    if files:
        for name, file_path in files.items():
            path = Path(file_path)
            if not path.exists():
                logger.warning(f"File not found for upload: {file_path}")
                continue
            body.extend(f"--{boundary}\r\n".encode())
            body.extend(f'Content-Disposition: form-data; name="{name}"; filename="{path.name}"\r\n'.encode())
            # Detect JPEG or fallback to octet-stream
            content_type = "image/jpeg" if path.suffix.lower() in [".jpg", ".jpeg"] else "application/octet-stream"
            body.extend(f"Content-Type: {content_type}\r\n\r\n".encode())
            with open(path, "rb") as f:
                body.extend(f.read())
            body.extend(b"\r\n")
            
    body.extend(f"--{boundary}--\r\n".encode())
    
    req = urllib.request.Request(url, data=body)
    req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")
    req.add_header("User-Agent", "AutomatedContentAggregator/1.0")
    
    with urllib.request.urlopen(req) as response:
        return response.read()


def publish_console(message, files):
    """
    Mock publisher that prints content to the console.
    """
    logger.info("=== CONSOLE PUBLISHER (DRY RUN) ===")
    logger.info(f"Message Content:\n{message}")
    if files:
        logger.info("Attached Files:")
        for file_path in files:
            logger.info(f"  - {file_path}")
    logger.info("===================================")
    return True


def publish_telegram(token, chat_id, message, files):
    """
    Publish content to a Telegram channel or chat.
    """
    if not token or not chat_id:
        logger.error("Telegram bot token or chat ID is missing in configuration.")
        return False
        
    try:
        if files:
            # Send the first file as a photo with caption
            photo_path = files[0]
            url = f"https://api.telegram.org/bot{token}/sendPhoto"
            fields = {
                "chat_id": chat_id,
                "caption": message[:1024]  # Telegram photo caption limit is 1024
            }
            send_multipart_request(url, fields, files={"photo": photo_path})
            logger.info("Successfully published photo to Telegram.")
        else:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            data = urllib.parse.urlencode({
                "chat_id": chat_id,
                "text": message[:4096]  # Telegram message limit is 4096
            }).encode('utf-8')
            req = urllib.request.Request(url, data=data)
            with urllib.request.urlopen(req) as response:
                response.read()
            logger.info("Successfully published message to Telegram.")
        return True
    except Exception as error:
        logger.error(f"Error publishing to Telegram: {error}")
        return False


def publish_discord(webhook_url, message, files):
    """
    Publish content to Discord via Webhook.
    """
    if not webhook_url:
        logger.error("Discord Webhook URL is missing in configuration.")
        return False
        
    try:
        payload = {"content": message[:2000]}  # Discord limit is 2000
        if files:
            fields = {"payload_json": json.dumps(payload)}
            send_multipart_request(webhook_url, fields, files={"file": files[0]})
            logger.info("Successfully published to Discord with file.")
        else:
            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(webhook_url, data=data)
            req.add_header("Content-Type", "application/json")
            req.add_header("User-Agent", "AutomatedContentAggregator/1.0")
            with urllib.request.urlopen(req) as response:
                response.read()
            logger.info("Successfully published message to Discord.")
        return True
    except Exception as error:
        logger.error(f"Error publishing to Discord: {error}")
        return False


def publish_twitter(config, message, files):
    """
    Publish content to Twitter/X using API v2 via Tweepy.
    """
    try:
        import tweepy
    except ImportError:
        logger.error("Tweepy library is not installed. Install it using 'pip install tweepy'.")
        return False
        
    api_key = config.get("twitter_api_key")
    api_secret = config.get("twitter_api_secret")
    access_token = config.get("twitter_access_token")
    access_token_secret = config.get("twitter_access_token_secret")
    
    if not all([api_key, api_secret, access_token, access_token_secret]):
        logger.error("Twitter keys/tokens are missing in configuration.")
        return False
        
    try:
        # Client for API v2 posting
        client = tweepy.Client(
            consumer_key=api_key,
            consumer_secret=api_secret,
            access_token=access_token,
            access_token_secret=access_token_secret
        )
        
        media_ids = []
        if files:
            # We still need API v1.1 endpoint to upload media
            auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_token_secret)
            api_v1 = tweepy.API(auth)
            for file_path in files[:4]:  # Twitter allows max 4 images
                if os.path.exists(file_path):
                    logger.info(f"Uploading media to Twitter: {file_path}")
                    media = api_v1.media_upload(file_path)
                    media_ids.append(media.media_id)
                    
        if media_ids:
            client.create_tweet(text=message[:280], media_ids=media_ids)
        else:
            client.create_tweet(text=message[:280])
            
        logger.info("Successfully posted tweet to Twitter/X.")
        return True
    except Exception as error:
        logger.error(f"Error publishing to Twitter: {error}")
        return False


def build_message(data):
    """
    Construct a clean formatted message from processed email content.
    """
    subject = data.get("subject", "No Subject")
    sender = data.get("sender", "Unknown Sender")
    
    # Beautify sender name
    clean_sender = sender
    if " <" in sender:
        clean_sender = sender.split(" <")[0]
        
    text_contents = data.get("text_contents", [])
    body_excerpt = ""
    if text_contents:
        first_text = text_contents[0].strip()
        # Clean double spaces/newlines
        first_text = " ".join(first_text.split())
        if len(first_text) > 200:
            body_excerpt = first_text[:200] + "..."
        else:
            body_excerpt = first_text
            
    msg = f"📢 {subject}\n"
    msg += f"👤 Gönderen: {clean_sender}\n\n"
    if body_excerpt:
        msg += f"📝 Özet:\n{body_excerpt}\n"
    return msg


def main():
    """
    Main entry point for the publisher.
    """
    try:
        config = load_config()
        content_dir = Path("content")
        
        if not content_dir.exists():
            logger.info("Content directory does not exist. Run processor.py first.")
            return

        processed_uids = [u.strip() for u in config.get("processed_uids", "").split(",") if u.strip()]
        published_uids = [u.strip() for u in config.get("published_uids", "").split(",") if u.strip()]
        
        target = config.get("publisher_target", "console").lower()
        logger.info(f"Publisher target mode: {target.upper()}")
        
        unpublished_count = 0
        
        for uid in processed_uids:
            if uid in published_uids:
                continue
                
            json_path = content_dir / f"{uid}.json"
            if not json_path.exists():
                logger.warning(f"Metadata file not found: {json_path}")
                continue
                
            unpublished_count += 1
            logger.info(f"Publishing UID {uid}...")
            
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                
            message = build_message(data)
            files = data.get("processed_files", [])
            
            success = False
            if target == "console":
                success = publish_console(message, files)
            elif target == "telegram":
                success = publish_telegram(
                    config.get("telegram_bot_token"),
                    config.get("telegram_chat_id"),
                    message,
                    files
                )
            elif target == "discord":
                success = publish_discord(
                    config.get("discord_webhook_url"),
                    message,
                    files
                )
            elif target == "twitter":
                success = publish_twitter(config, message, files)
            else:
                logger.error(f"Unknown publisher target: {target}")
                continue
                
            if success:
                save_published_uid(uid, config)
                logger.info(f"UID {uid} successfully registered as published.")
            else:
                logger.error(f"Failed to publish UID {uid}.")
                
            logger.info("-" * 100)
            
        if unpublished_count == 0:
            logger.info("All processed emails are already published.")
            
    except Exception as error:
        logger.error(f"Error during main publishing process: {error}")


if __name__ == "__main__":
    main()

"""
Integration Test for Automated Content Aggregator

This script creates a mock email, configures a test config.json (backing up
any existing configuration), and runs processor.py and publish.py to verify
the pipeline is fully operational without requiring real email server credentials.
"""

import os
import json
import shutil
import base64
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TestPipeline")

# Generate a valid mock JPEG image using Pillow
from PIL import Image
import io

img = Image.new('RGB', (10, 10), color='red')
img_buf = io.BytesIO()
img.save(img_buf, format='JPEG')
MOCK_JPEG_B64 = base64.b64encode(img_buf.getvalue()).decode('utf-8')

# Base64 for "Hello World, this is a mock newsletter!"
MOCK_TEXT_B64 = "SGVsbG8gV29ybGQsIHRoaXMgaXMgYSBtb2NrIG5ld3NsZXR0ZXIh"

def get_mock_eml():
    return f"""From: Newsletter <news@mock.com>
To: subscriber@example.com
Subject: Mock Newsletter
Date: Sun, 07 Jun 2026 12:00:00 +0000
Content-Type: multipart/mixed; boundary="boundary-12345"

--boundary-12345
Content-Type: text/plain; charset="utf-8"
Content-Transfer-Encoding: base64

{MOCK_TEXT_B64}

--boundary-12345
Content-Type: image/jpeg; name="test.jpg"
Content-Transfer-Encoding: base64
Content-Disposition: attachment; filename="test.jpg"

{MOCK_JPEG_B64}

--boundary-12345--
"""


def run_test():
    # Setup paths
    config_path = Path("config.json")
    config_backup_path = Path("config.json.backup")
    raw_dir = Path("rawcontent")
    content_dir = Path("content")
    
    # 1. Backup existing config.json
    has_config_backup = False
    if config_path.exists():
        logger.info("Backing up existing config.json...")
        shutil.copy(config_path, config_backup_path)
        has_config_backup = True
        
    try:
        # 2. Write a mock config.json for the test
        test_config = {
            "email": "test@example.com",
            "password": "password123",
            "imap_server": "imap.example.com",
            "poppler_path": "",
            "collected_uids": "mock123",
            "processed_uids": "",
            "published_uids": "",
            "publisher_target": "console"
        }
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(test_config, f, indent=2)
            
        # 3. Create rawcontent directory and mock EML file
        raw_dir.mkdir(exist_ok=True)
        eml_path = raw_dir / "mock123.eml"
        with open(eml_path, "w", encoding="utf-8") as f:
            f.write(get_mock_eml())
        logger.info(f"Created mock email file: {eml_path}")
        
        # Ensure clean state in content folder
        mock_json_path = content_dir / "mock123.json"
        if mock_json_path.exists():
            mock_json_path.unlink()
            
        # Remove any leftover mock image files
        for f in content_dir.glob("mock123_img_*.jpg"):
            f.unlink()

        # 4. Import and run processor
        logger.info("Running processor.main()...")
        import processor
        # Reload configuration in the processor to capture our test_config
        processor.main()
        
        # 5. Assert processor output
        if not mock_json_path.exists():
            raise AssertionError("FAIL: processor did not create output metadata JSON file.")
            
        with open(mock_json_path, "r", encoding="utf-8") as f:
            output_data = json.load(f)
            
        assert output_data.get("subject") == "Mock Newsletter", "FAIL: Subject mismatch"
        assert "news@mock.com" in output_data.get("sender"), "FAIL: Sender mismatch"
        assert len(output_data.get("text_contents", [])) > 0, "FAIL: Text contents empty"
        assert "Hello World" in output_data["text_contents"][0], "FAIL: Text decoding failed"
        assert len(output_data.get("processed_files", [])) > 0, "FAIL: Image attachment was not processed"
        
        logger.info("SUCCESS: processor outputs verified successfully.")
        
        # 6. Import and run publisher
        logger.info("Running publish.main()...")
        import publish
        publish.main()
        
        # 7. Verify config changes
        with open(config_path, "r", encoding="utf-8") as f:
            post_publish_config = json.load(f)
            
        published_list = post_publish_config.get("published_uids", "").split(",")
        assert "mock123" in published_list, "FAIL: UID was not marked as published in config."
        
        logger.info("SUCCESS: publisher state verified successfully.")
        logger.info("=== ALL INTEGRATION TESTS PASSED ===")
        
    finally:
        # Cleanup mock files we created for testing
        if eml_path.exists():
            eml_path.unlink()
        if mock_json_path.exists():
            mock_json_path.unlink()
        for f in content_dir.glob("mock123_img_*.jpg"):
            f.unlink()
            
        # Restore configuration backup
        if has_config_backup:
            logger.info("Restoring backed up config.json...")
            shutil.move(config_backup_path, config_path)
        elif config_path.exists():
            config_path.unlink()


if __name__ == "__main__":
    run_test()

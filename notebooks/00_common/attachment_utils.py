import shutil
import os

def handle_attachment_file(src_path: str, dest_dir: str) -> str:
    """
    Copy an attachment or image file from src_path to dest_dir.
    Returns the new file path.
    """
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir, exist_ok=True)
    dest_path = os.path.join(dest_dir, os.path.basename(src_path))
    shutil.copy2(src_path, dest_path)
    return dest_path

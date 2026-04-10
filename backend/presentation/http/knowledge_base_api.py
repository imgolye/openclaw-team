#!/usr/bin/env python3
"""Knowledge Base API endpoints for desktop client."""

from __future__ import annotations

import base64
import json
import os
import shutil
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path


# In-memory storage with file persistence
_kb_collections_db = {}
_kb_files_db = {}
_db_lock = threading.Lock()
_db_initialized = False


def _get_storage_dir(openclaw_dir):
    """Get the storage directory for knowledge base data."""
    storage_dir = Path(openclaw_dir) / ".mission-control" / "knowledge-base"
    storage_dir.mkdir(parents=True, exist_ok=True)
    return storage_dir


def _get_files_dir(openclaw_dir):
    """Get the files storage directory."""
    files_dir = _get_storage_dir(openclaw_dir) / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    return files_dir


def _get_collections_file(openclaw_dir):
    """Get the collections storage file path."""
    return _get_storage_dir(openclaw_dir) / "collections.json"


def _get_files_meta_file(openclaw_dir):
    """Get the files metadata storage file path."""
    return _get_storage_dir(openclaw_dir) / "files_meta.json"


def _load_from_disk(openclaw_dir):
    """Load knowledge base data from disk storage."""
    global _kb_collections_db, _kb_files_db, _db_initialized
    
    if _db_initialized:
        return
    
    with _db_lock:
        if _db_initialized:
            return
        
        collections_file = _get_collections_file(openclaw_dir)
        files_meta_file = _get_files_meta_file(openclaw_dir)
        
        if collections_file.exists():
            try:
                with open(collections_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        _kb_collections_db = data
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Failed to load collections: {e}")
        
        if files_meta_file.exists():
            try:
                with open(files_meta_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        _kb_files_db = data
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Failed to load files metadata: {e}")
        
        _db_initialized = True


def _save_to_disk(openclaw_dir):
    """Save knowledge base data to disk storage."""
    with _db_lock:
        collections_file = _get_collections_file(openclaw_dir)
        files_meta_file = _get_files_meta_file(openclaw_dir)
        
        try:
            with open(collections_file, 'w', encoding='utf-8') as f:
                json.dump(_kb_collections_db, f, ensure_ascii=False, indent=2)
        except IOError as e:
            print(f"Warning: Failed to save collections: {e}")
        
        try:
            with open(files_meta_file, 'w', encoding='utf-8') as f:
                json.dump(_kb_files_db, f, ensure_ascii=False, indent=2)
        except IOError as e:
            print(f"Warning: Failed to save files metadata: {e}")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _generate_id():
    return str(uuid.uuid4())


def _get_openclaw_dir(handler):
    """Get openclaw directory from handler."""
    return getattr(handler.server, 'openclaw_dir', os.getcwd())


def _get_file_size_str(size_bytes):
    """Convert bytes to human readable string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def _get_file_type(filename):
    """Get file type from extension."""
    ext = Path(filename).suffix.lower()
    type_map = {
        '.pdf': 'pdf',
        '.doc': 'doc',
        '.docx': 'doc',
        '.md': 'doc',
        '.txt': 'doc',
        '.png': 'image',
        '.jpg': 'image',
        '.jpeg': 'image',
        '.gif': 'image',
        '.webp': 'image',
    }
    return type_map.get(ext, 'default')


def _update_collection_stats(openclaw_dir, collection_id):
    """Update collection statistics."""
    collection = _kb_collections_db.get(collection_id)
    if not collection:
        return
    
    files = [f for f in _kb_files_db.values() if f.get("collectionId") == collection_id]
    total_size = sum(f.get("size", 0) for f in files)
    
    collection["count"] = len(files)
    collection["size"] = total_size
    collection["sizeStr"] = _get_file_size_str(total_size)
    collection["updatedAt"] = _now_iso()
    _save_to_disk(openclaw_dir)


def _serialize_collection(collection):
    item = dict(collection or {})
    size_value = int(item.get("size") or 0)
    item["size"] = size_value
    item["sizeStr"] = item.get("sizeStr") or _get_file_size_str(size_value)
    return item


def _serialize_file_record(file_record, collection=None):
    item = dict(file_record or {})
    collection_id = str(item.get("collectionId") or "").strip()
    size_value = int(item.get("size") or 0)
    item["size"] = size_value
    item["sizeStr"] = item.get("sizeStr") or _get_file_size_str(size_value)
    if isinstance(collection, dict):
        item["collectionName"] = str(collection.get("name") or "").strip()
        item["collectionIcon"] = str(collection.get("icon") or "").strip()
    if collection_id and item.get("id"):
        item["downloadPath"] = f"/api/knowledge-base/collections/{collection_id}/files/{item['id']}/download"
        item["previewPath"] = f"/api/knowledge-base/collections/{collection_id}/files/{item['id']}/content"
    return item


def handle_kb_get_collections(handler, services):
    """GET /api/knowledge-base/collections - List all collections."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    collections = [_serialize_collection(item) for item in _kb_collections_db.values()]
    handler._send_json({"ok": True, "collections": collections})
    return True


def handle_kb_create_collection(handler, services):
    """POST /api/knowledge-base/collections - Create new collection."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    
    try:
        body = json.loads(handler._read_body() or "{}")
    except json.JSONDecodeError:
        handler._send_json({"ok": False, "error": "invalid_json", "message": "无效的 JSON 数据"}, status=400)
        return True

    collection_id = _generate_id()
    now = _now_iso()
    
    collection = {
        "id": collection_id,
        "name": body.get("name", "未命名集合"),
        "icon": body.get("icon", "📚"),
        "description": body.get("description", ""),
        "count": 0,
        "size": 0,
        "sizeStr": "0 B",
        "createdAt": now,
        "updatedAt": now,
    }
    
    _kb_collections_db[collection_id] = collection
    _save_to_disk(openclaw_dir)
    
    handler._send_json({"ok": True, "collection": _serialize_collection(collection)}, status=201)
    return True


def handle_kb_get_collection(handler, services, collection_id):
    """GET /api/knowledge-base/collections/{id} - Get collection detail."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    collection = _kb_collections_db.get(collection_id)
    if not collection:
        handler._send_json({"ok": False, "error": "not_found", "message": "集合不存在"}, status=404)
        return True
    handler._send_json({"ok": True, "collection": _serialize_collection(collection)})
    return True


def handle_kb_update_collection(handler, services, collection_id):
    """PUT /api/knowledge-base/collections/{id} - Update collection."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    
    collection = _kb_collections_db.get(collection_id)
    if not collection:
        handler._send_json({"ok": False, "error": "not_found", "message": "集合不存在"}, status=404)
        return True
    
    try:
        body = json.loads(handler._read_body() or "{}")
    except json.JSONDecodeError:
        handler._send_json({"ok": False, "error": "invalid_json", "message": "无效的 JSON 数据"}, status=400)
        return True
    
    allowed_fields = ["name", "icon", "description"]
    for field in allowed_fields:
        if field in body:
            collection[field] = body[field]
    
    collection["updatedAt"] = _now_iso()
    _save_to_disk(openclaw_dir)
    
    handler._send_json({"ok": True, "collection": _serialize_collection(collection)})
    return True


def handle_kb_delete_collection(handler, services, collection_id):
    """DELETE /api/knowledge-base/collections/{id} - Delete collection."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    
    collection = _kb_collections_db.pop(collection_id, None)
    if not collection:
        handler._send_json({"ok": False, "error": "not_found", "message": "集合不存在"}, status=404)
        return True
    
    # Delete all files in the collection
    files_to_delete = [f_id for f_id, f in _kb_files_db.items() if f.get("collectionId") == collection_id]
    files_dir = _get_files_dir(openclaw_dir)
    
    for f_id in files_to_delete:
        file_record = _kb_files_db.pop(f_id, None)
        if file_record:
            # Delete actual file
            file_path = files_dir / f_id
            try:
                if file_path.exists():
                    file_path.unlink()
            except OSError as e:
                print(f"Warning: Failed to delete file {f_id}: {e}")
    
    _save_to_disk(openclaw_dir)
    handler._send_json({"ok": True, "message": "集合已删除"})
    return True


def handle_kb_get_files(handler, services, collection_id):
    """GET /api/knowledge-base/collections/{id}/files - List files in collection."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    
    collection = _kb_collections_db.get(collection_id)
    if not collection:
        handler._send_json({"ok": False, "error": "not_found", "message": "集合不存在"}, status=404)
        return True
    
    files = [_serialize_file_record(f, collection=collection) for f in _kb_files_db.values() if f.get("collectionId") == collection_id]
    files.sort(key=lambda x: x.get("updatedAt", ""), reverse=True)
    
    handler._send_json({"ok": True, "files": files})
    return True


def handle_kb_upload_file(handler, services, collection_id):
    """POST /api/knowledge-base/collections/{id}/files - Upload file to collection."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    
    collection = _kb_collections_db.get(collection_id)
    if not collection:
        handler._send_json({"ok": False, "error": "not_found", "message": "集合不存在"}, status=404)
        return True
    
    try:
        body = json.loads(handler._read_body() or "{}")
    except json.JSONDecodeError:
        handler._send_json({"ok": False, "error": "invalid_json", "message": "无效的 JSON 数据"}, status=400)
        return True
    
    file_id = _generate_id()
    now = _now_iso()
    
    # Handle base64 encoded file content
    file_content = body.get("content", "")
    file_data = base64.b64decode(file_content) if file_content else b""
    size_bytes = len(file_data)
    
    # Save file to disk
    files_dir = _get_files_dir(openclaw_dir)
    file_path = files_dir / file_id
    
    try:
        with open(file_path, 'wb') as f:
            f.write(file_data)
    except IOError as e:
        handler._send_json({"ok": False, "error": "save_failed", "message": f"保存文件失败: {e}"}, status=500)
        return True
    
    file_record = {
        "id": file_id,
        "collectionId": collection_id,
        "name": body.get("name", "未命名文件"),
        "type": _get_file_type(body.get("name", "")),
        "size": size_bytes,
        "sizeStr": _get_file_size_str(size_bytes),
        "contentType": body.get("contentType", "application/octet-stream"),
        "status": "indexed",  # Directly mark as indexed for now
        "createdAt": now,
        "updatedAt": now,
        "indexedAt": now,
    }
    
    _kb_files_db[file_id] = file_record
    _update_collection_stats(openclaw_dir, collection_id)
    
    handler._send_json({"ok": True, "file": _serialize_file_record(file_record, collection=collection)}, status=201)
    return True


def handle_kb_delete_file(handler, services, collection_id, file_id):
    """DELETE /api/knowledge-base/collections/{id}/files/{fileId} - Delete file."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    
    collection = _kb_collections_db.get(collection_id)
    if not collection:
        handler._send_json({"ok": False, "error": "not_found", "message": "集合不存在"}, status=404)
        return True
    
    file_record = _kb_files_db.pop(file_id, None)
    if not file_record or file_record.get("collectionId") != collection_id:
        handler._send_json({"ok": False, "error": "not_found", "message": "文件不存在"}, status=404)
        return True
    
    # Delete actual file
    files_dir = _get_files_dir(openclaw_dir)
    file_path = files_dir / file_id
    try:
        if file_path.exists():
            file_path.unlink()
    except OSError as e:
        print(f"Warning: Failed to delete file {file_id}: {e}")
    
    _update_collection_stats(openclaw_dir, collection_id)
    handler._send_json({"ok": True, "message": "文件已删除"})
    return True


def handle_kb_get_file(handler, services, collection_id, file_id):
    """GET /api/knowledge-base/collections/{id}/files/{fileId} - Get file detail."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    
    collection = _kb_collections_db.get(collection_id)
    if not collection:
        handler._send_json({"ok": False, "error": "not_found", "message": "集合不存在"}, status=404)
        return True
    
    file_record = _kb_files_db.get(file_id)
    if not file_record or file_record.get("collectionId") != collection_id:
        handler._send_json({"ok": False, "error": "not_found", "message": "文件不存在"}, status=404)
        return True
    
    handler._send_json({"ok": True, "file": _serialize_file_record(file_record, collection=collection)})
    return True


def handle_kb_preview_file(handler, services, collection_id, file_id):
    """GET /api/knowledge-base/collections/{id}/files/{fileId}/content - Preview file inline."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)

    collection = _kb_collections_db.get(collection_id)
    if not collection:
        handler._send_json({"ok": False, "error": "not_found", "message": "集合不存在"}, status=404)
        return True

    file_record = _kb_files_db.get(file_id)
    if not file_record or file_record.get("collectionId") != collection_id:
        handler._send_json({"ok": False, "error": "not_found", "message": "文件不存在"}, status=404)
        return True

    files_dir = _get_files_dir(openclaw_dir)
    file_path = files_dir / file_id
    if not file_path.exists():
        handler._send_json({"ok": False, "error": "file_not_found", "message": "文件内容不存在"}, status=404)
        return True

    try:
        with open(file_path, 'rb') as f:
            file_data = f.read()
        handler._send_bytes(
            file_data,
            file_record.get("contentType", "application/octet-stream"),
            extra_headers=[("Content-Disposition", f'inline; filename="{file_record.get("name", "preview")}"')],
        )
        return True
    except IOError as e:
        handler._send_json({"ok": False, "error": "read_failed", "message": f"读取文件失败: {e}"}, status=500)
        return True


def handle_kb_download_file(handler, services, collection_id, file_id):
    """GET /api/knowledge-base/collections/{id}/files/{fileId}/download - Download file."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    
    collection = _kb_collections_db.get(collection_id)
    if not collection:
        handler._send_json({"ok": False, "error": "not_found", "message": "集合不存在"}, status=404)
        return True
    
    file_record = _kb_files_db.get(file_id)
    if not file_record or file_record.get("collectionId") != collection_id:
        handler._send_json({"ok": False, "error": "not_found", "message": "文件不存在"}, status=404)
        return True
    
    # Read file content
    files_dir = _get_files_dir(openclaw_dir)
    file_path = files_dir / file_id
    
    if not file_path.exists():
        handler._send_json({"ok": False, "error": "file_not_found", "message": "文件内容不存在"}, status=404)
        return True
    
    try:
        with open(file_path, 'rb') as f:
            file_data = f.read()
        
        # Send file as download
        filename = file_record.get("name", "download")
        content_type = file_record.get("contentType", "application/octet-stream")
        
        handler._send_bytes(
            file_data,
            content_type,
            extra_headers=[("Content-Disposition", f'attachment; filename="{filename}"')]
        )
        return True
    except IOError as e:
        handler._send_json({"ok": False, "error": "read_failed", "message": f"读取文件失败: {e}"}, status=500)
        return True


def handle_kb_search(handler, services):
    """GET /api/knowledge-base/search - Search across all collections."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    
    query = handler._query().get("q", [""])[0] or ""
    
    if not query:
        handler._send_json({"ok": True, "results": []})
        return True
    
    results = []
    for file_record in _kb_files_db.values():
        if query.lower() in file_record.get("name", "").lower():
            collection = _kb_collections_db.get(str(file_record.get("collectionId") or "").strip())
            results.append(_serialize_file_record(file_record, collection=collection))
    
    handler._send_json({"ok": True, "results": results})
    return True


def handle_kb_route(handler, services, path):
    """Route knowledge base API requests."""
    method = handler.command
    
    # Search
    if path == "/api/knowledge-base/search" and method == "GET":
        return handle_kb_search(handler, services)
    
    # Collections list
    if path == "/api/knowledge-base/collections":
        if method == "GET":
            return handle_kb_get_collections(handler, services)
        elif method == "POST":
            return handle_kb_create_collection(handler, services)
    
    # Single collection operations
    if path.startswith("/api/knowledge-base/collections/"):
        parts = path.split("/")
        if len(parts) >= 5:
            collection_id = parts[4]
            
            # Files in collection
            if len(parts) == 6 and parts[5] == "files":
                if method == "GET":
                    return handle_kb_get_files(handler, services, collection_id)
                elif method == "POST":
                    return handle_kb_upload_file(handler, services, collection_id)
            
            # Single file operations
            if len(parts) >= 7 and parts[5] == "files":
                file_id = parts[6]
                
                if len(parts) == 8 and parts[7] == "download" and method == "GET":
                    return handle_kb_download_file(handler, services, collection_id, file_id)
                if len(parts) == 8 and parts[7] == "content" and method == "GET":
                    return handle_kb_preview_file(handler, services, collection_id, file_id)
                
                if len(parts) == 7:
                    if method == "GET":
                        return handle_kb_get_file(handler, services, collection_id, file_id)
                    elif method == "DELETE":
                        return handle_kb_delete_file(handler, services, collection_id, file_id)
            
            # Collection CRUD
            if len(parts) == 5:
                if method == "GET":
                    return handle_kb_get_collection(handler, services, collection_id)
                elif method == "PUT":
                    return handle_kb_update_collection(handler, services, collection_id)
                elif method == "DELETE":
                    return handle_kb_delete_collection(handler, services, collection_id)
    
    return False

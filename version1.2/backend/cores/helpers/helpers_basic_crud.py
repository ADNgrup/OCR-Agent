"""
helpers_basic_crud.py
~~~~~~~~~~~~~~~~~~~~~
Bộ helper chuẩn hóa các thao tác CRUD cơ bản với MongoDB.

Sử dụng:
    from cores.helpers.helpers_basic_crud import crud

    # Tạo document
    doc_id = crud.insert_one(db, "kvm_sources", {"name": "cam1", "host": "10.0.0.1", "port": 443})

    # Lấy theo id
    source = crud.find_by_id(db, "kvm_sources", doc_id)

    # Danh sách có filter, sort, phân trang
    sources = crud.find_many(db, "kvm_sources", filter={"enabled": True}, sort=[("name", 1)], skip=0, limit=20)

    # Cập nhật
    crud.update_by_id(db, "kvm_sources", doc_id, {"enabled": False})

    # Xoá
    crud.delete_by_id(db, "kvm_sources", doc_id)
"""

from __future__ import annotations

from typing import Any

from bson import ObjectId
from bson.errors import InvalidId
from pymongo.database import Database
from pymongo.results import DeleteResult, InsertOneResult, UpdateResult

from utils.common import now_utc


# ---------------------------------------------------------------------------
# ObjectId helpers
# ---------------------------------------------------------------------------

def to_object_id(value: str | ObjectId) -> ObjectId:
    """Chuyển chuỗi sang ``ObjectId``. Raise ``ValueError`` nếu không hợp lệ."""
    if isinstance(value, ObjectId):
        return value
    try:
        return ObjectId(value)
    except (InvalidId, TypeError) as exc:
        raise ValueError(f"Invalid ObjectId: {value!r}") from exc


def to_str_id(value: Any) -> str:
    """Chuyển ``_id`` (ObjectId hoặc khác) thành chuỗi."""
    return str(value)


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------

def serialize_doc(doc: dict | None, *, id_field: str = "id") -> dict | None:
    """Chuyển ``_id`` (ObjectId) → trường ``id`` (str) để trả về qua API.

    Nếu *doc* là ``None`` thì trả ``None``.
    Các trường có giá trị ``ObjectId`` lồng bên trong cũng được chuyển sang str.
    """
    if doc is None:
        return None
    out: dict[str, Any] = {}
    for key, val in doc.items():
        if key == "_id":
            out[id_field] = to_str_id(val)
        elif isinstance(val, ObjectId):
            out[key] = to_str_id(val)
        else:
            out[key] = val
    return out


def serialize_docs(docs: list[dict], *, id_field: str = "id") -> list[dict]:
    """``serialize_doc`` cho danh sách documents."""
    return [serialize_doc(d, id_field=id_field) for d in docs]  # type: ignore[misc]


# ---------------------------------------------------------------------------
# CREATE
# ---------------------------------------------------------------------------

def insert_one(
    db: Database,
    collection: str,
    document: dict,
    *,
    auto_timestamps: bool = True,
) -> str:
    """Thêm một document. Trả về ``str(inserted_id)``.

    Nếu *auto_timestamps* = True (mặc định), tự thêm ``created_at`` và ``updated_at``.
    """
    if auto_timestamps:
        now = now_utc()
        document.setdefault("created_at", now)
        document.setdefault("updated_at", now)

    result: InsertOneResult = db[collection].insert_one(document)
    return to_str_id(result.inserted_id)


def insert_many(
    db: Database,
    collection: str,
    documents: list[dict],
    *,
    auto_timestamps: bool = True,
    ordered: bool = True,
) -> list[str]:
    """Thêm nhiều documents. Trả về danh sách ``str(inserted_id)``."""
    if auto_timestamps:
        now = now_utc()
        for doc in documents:
            doc.setdefault("created_at", now)
            doc.setdefault("updated_at", now)

    result = db[collection].insert_many(documents, ordered=ordered)
    return [to_str_id(oid) for oid in result.inserted_ids]


# ---------------------------------------------------------------------------
# READ
# ---------------------------------------------------------------------------

def find_by_id(
    db: Database,
    collection: str,
    doc_id: str | ObjectId,
    *,
    projection: dict | None = None,
) -> dict | None:
    """Tìm 1 document theo ``_id``. Trả ``None`` nếu không tìm thấy."""
    try:
        oid = to_object_id(doc_id)
    except ValueError:
        return None
    return db[collection].find_one({"_id": oid}, projection)


def find_one(
    db: Database,
    collection: str,
    filter: dict | None = None,
    *,
    projection: dict | None = None,
    sort: list[tuple[str, int]] | None = None,
) -> dict | None:
    """Tìm 1 document theo filter tuỳ ý."""
    cursor = db[collection].find(filter or {}, projection)
    if sort:
        cursor = cursor.sort(sort)
    return cursor.limit(1).next() if db[collection].count_documents(filter or {}, limit=1) else None


def find_many(
    db: Database,
    collection: str,
    filter: dict | None = None,
    *,
    projection: dict | None = None,
    sort: list[tuple[str, int]] | None = None,
    skip: int = 0,
    limit: int = 0,
) -> list[dict]:
    """Truy vấn nhiều documents với sort / skip / limit.

    ``limit=0`` nghĩa là không giới hạn (default MongoDB).
    """
    cursor = db[collection].find(filter or {}, projection)
    if sort:
        cursor = cursor.sort(sort)
    if skip:
        cursor = cursor.skip(skip)
    if limit:
        cursor = cursor.limit(limit)
    return list(cursor)


def count(
    db: Database,
    collection: str,
    filter: dict | None = None,
) -> int:
    """Đếm documents khớp filter."""
    return db[collection].count_documents(filter or {})


def exists(
    db: Database,
    collection: str,
    filter: dict,
) -> bool:
    """Kiểm tra có tồn tại ít nhất 1 document khớp filter."""
    return db[collection].count_documents(filter, limit=1) > 0


# ---------------------------------------------------------------------------
# READ – phân trang
# ---------------------------------------------------------------------------

def paginate(
    db: Database,
    collection: str,
    filter: dict | None = None,
    *,
    projection: dict | None = None,
    sort: list[tuple[str, int]] | None = None,
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    """Truy vấn có phân trang, trả về dạng:

    .. code-block:: python

        {
            "items": [...],
            "total": 120,
            "page": 1,
            "page_size": 20,
            "total_pages": 6,
        }
    """
    _filter = filter or {}
    total = db[collection].count_documents(_filter)

    page = max(1, page)
    page_size = max(1, min(page_size, 500))
    total_pages = max(1, -(-total // page_size))  # ceil division

    skip = (page - 1) * page_size
    items = find_many(
        db, collection, _filter,
        projection=projection, sort=sort,
        skip=skip, limit=page_size,
    )
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


# ---------------------------------------------------------------------------
# UPDATE
# ---------------------------------------------------------------------------

def update_by_id(
    db: Database,
    collection: str,
    doc_id: str | ObjectId,
    update_fields: dict,
    *,
    auto_updated_at: bool = True,
) -> bool:
    """Cập nhật document theo ``_id`` bằng ``$set``. Trả ``True`` nếu đã cập nhật."""
    try:
        oid = to_object_id(doc_id)
    except ValueError:
        return False

    if auto_updated_at:
        update_fields.setdefault("updated_at", now_utc())

    result: UpdateResult = db[collection].update_one(
        {"_id": oid},
        {"$set": update_fields},
    )
    return result.modified_count > 0


def update_one(
    db: Database,
    collection: str,
    filter: dict,
    update: dict,
    *,
    upsert: bool = False,
    auto_updated_at: bool = True,
) -> UpdateResult:
    """Cập nhật 1 document theo filter với toàn bộ update expression (``$set``, ``$inc``, …).

    Nếu *auto_updated_at* và update chứa ``$set``, tự thêm ``updated_at``.
    """
    if auto_updated_at and "$set" in update:
        update["$set"].setdefault("updated_at", now_utc())

    return db[collection].update_one(filter, update, upsert=upsert)


def update_many(
    db: Database,
    collection: str,
    filter: dict,
    update: dict,
    *,
    upsert: bool = False,
    auto_updated_at: bool = True,
) -> int:
    """Cập nhật nhiều documents. Trả ``modified_count``."""
    if auto_updated_at and "$set" in update:
        update["$set"].setdefault("updated_at", now_utc())

    result = db[collection].update_many(filter, update, upsert=upsert)
    return result.modified_count


def upsert_one(
    db: Database,
    collection: str,
    filter: dict,
    document: dict,
    *,
    auto_timestamps: bool = True,
) -> str:
    """Insert nếu chưa có, update nếu đã tồn tại. Trả ``str(upserted_id | matched _id)``."""
    now = now_utc()
    set_on_insert: dict[str, Any] = {}
    if auto_timestamps:
        document.setdefault("updated_at", now)
        set_on_insert["created_at"] = now

    update: dict[str, Any] = {"$set": document}
    if set_on_insert:
        update["$setOnInsert"] = set_on_insert

    result = db[collection].update_one(filter, update, upsert=True)
    if result.upserted_id:
        return to_str_id(result.upserted_id)
    # Đã match → lấy _id của document đang có
    existing = db[collection].find_one(filter, {"_id": 1})
    return to_str_id(existing["_id"]) if existing else ""


# ---------------------------------------------------------------------------
# DELETE
# ---------------------------------------------------------------------------

def delete_by_id(
    db: Database,
    collection: str,
    doc_id: str | ObjectId,
) -> bool:
    """Xoá 1 document theo ``_id``. Trả ``True`` nếu đã xoá."""
    try:
        oid = to_object_id(doc_id)
    except ValueError:
        return False
    result: DeleteResult = db[collection].delete_one({"_id": oid})
    return result.deleted_count > 0


def delete_many(
    db: Database,
    collection: str,
    filter: dict,
) -> int:
    """Xoá nhiều documents khớp filter. Trả ``deleted_count``."""
    result = db[collection].delete_many(filter)
    return result.deleted_count


# ---------------------------------------------------------------------------
# AGGREGATE helpers
# ---------------------------------------------------------------------------

def aggregate(
    db: Database,
    collection: str,
    pipeline: list[dict],
) -> list[dict]:
    """Chạy aggregation pipeline, trả danh sách kết quả."""
    return list(db[collection].aggregate(pipeline))


def distinct_values(
    db: Database,
    collection: str,
    field: str,
    filter: dict | None = None,
) -> list[Any]:
    """Lấy danh sách giá trị duy nhất của 1 trường."""
    return db[collection].distinct(field, filter or {})


# ---------------------------------------------------------------------------
# Module-level namespace (optional convenience)
# ---------------------------------------------------------------------------

class _CrudNamespace:
    """Gom tất cả hàm CRUD vào 1 object ``crud`` để dùng:

        from cores.helpers.helpers_basic_crud import crud
        crud.insert_one(db, "col", {...})
    """

    # ObjectId
    to_object_id = staticmethod(to_object_id)
    to_str_id = staticmethod(to_str_id)

    # Serialize
    serialize_doc = staticmethod(serialize_doc)
    serialize_docs = staticmethod(serialize_docs)

    # Create
    insert_one = staticmethod(insert_one)
    insert_many = staticmethod(insert_many)

    # Read
    find_by_id = staticmethod(find_by_id)
    find_one = staticmethod(find_one)
    find_many = staticmethod(find_many)
    count = staticmethod(count)
    exists = staticmethod(exists)
    paginate = staticmethod(paginate)

    # Update
    update_by_id = staticmethod(update_by_id)
    update_one = staticmethod(update_one)
    update_many = staticmethod(update_many)
    upsert_one = staticmethod(upsert_one)

    # Delete
    delete_by_id = staticmethod(delete_by_id)
    delete_many = staticmethod(delete_many)

    # Aggregate
    aggregate = staticmethod(aggregate)
    distinct_values = staticmethod(distinct_values)


crud = _CrudNamespace()

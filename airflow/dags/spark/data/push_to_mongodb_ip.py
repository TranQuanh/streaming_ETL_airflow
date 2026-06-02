import json
import os
from pymongo import MongoClient, UpdateOne

# ============================================================
# CẤU HÌNH KẾT NỐI MONGODB
# ============================================================
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://admin:123@127.0.0.1:27017/?authSource=admin")
DB_NAME = "glamira"
COLLECTION_NAME = "geoip"
INPUT_FILE = "ip_list.json"

def main():
    # 1. Đọc và parse file JSON dữ liệu thô
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Không tìm thấy file dữ liệu đầu vào: {INPUT_FILE}")
        return

    print(f"[READ] Đang đọc dữ liệu từ file {INPUT_FILE}...")
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    # 2. Xử lý bóc tách cấu trúc Key-Value động thành danh sách các document phẳng
    documents_to_upsert = []
    
    for ip_key, geo_content in raw_data.items():
        # Lấy bản sao dữ liệu để tránh ghi đè gốc
        doc = geo_content.copy()
        
        # Gán luôn IP làm khóa chính _id của MongoDB để tối ưu bộ nhớ và tránh trùng lặp
        doc["_id"] = ip_key 
        
        # Tạo ra câu lệnh Bulk Write (Cập nhật nếu trùng _id, chưa có thì thêm mới)
        operations = UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True)
        documents_to_upsert.append(operations)

    if not documents_to_upsert:
        print("⚠️ Không tìm thấy bản ghi nào hợp lệ trong file JSON.")
        return

    # 3. Kết nối và tiến hành đổ dữ liệu vào MongoDB bằng cơ chế Bulk Write
    print(f"[MONGO] Đang kết nối tới MongoDB và nạp dữ liệu vào database: '{DB_NAME}'...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    print(f"[INSERT] Đang thực thi nạp {len(documents_to_upsert)} bản ghi GeoIP...")
    result = collection.bulk_write(documents_to_upsert)

    # 4. In kết quả thông báo thành công
    print("\n========================================================")
    print("✅ ĐÃ HOÀN THÀNH QUY TRÌNH ĐẨY DỮ LIỆU VÀO MONGODB!")
    print(f"   - Số dòng thêm mới (Inserted): {result.upserted_count}")
    print(f"   - Số dòng cập nhật (Modified): {result.modified_count}")
    print("========================================================")
    
    client.close()

if __name__ == "__main__":
    main()
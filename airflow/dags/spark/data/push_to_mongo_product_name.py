import json
from pymongo import MongoClient, UpdateOne

# 1. Đọc dữ liệu từ file JSON
json_file_path = 'json_product_name.json'
with open(json_file_path, 'r', encoding='utf-8') as f:
    product_data = json.load(f)

# 2. Chuỗi kết nối bảo mật sử dụng IP 127.0.0.1 để tránh bẫy IPv6 trên Ubuntu
uri = "mongodb://admin:123@127.0.0.1:27017/?authSource=admin"

try:
    client = MongoClient(uri)
    
    # CHÚ Ý: Đã đổi sang database 'glamira' theo đúng yêu cầu của bạn
    db = client["glamira"]        
    collection = db["products"]    # Tên collection chứa thông tin sản phẩm

    # 3. Gom dữ liệu để bắn bulk_write cho tối ưu tốc độ
    operations = []
    for prod_id, prod_name in product_data.items():
        operations.append(
            UpdateOne(
                {"_id": prod_id},                             # Lấy luôn ID sản phẩm làm khóa chính chống trùng
                {"$set": {"product_name": prod_name}},
                upsert=True                                   # Chưa có thì chèn mới, có rồi thì cập nhật tên
            )
        )

    # 4. Thực thi lệnh đẩy dữ liệu lên Mongo
    if operations:
        result = collection.bulk_write(operations)
        
        print("==== ĐÃ ĐẨY DỮ LIỆU VÀO DATABASE GLAMIRA ====")
        print(f"👉 Số sản phẩm mới (Insert): {result.upserted_count}")
        print(f"👉 Số sản phẩm cập nhật (Update): {result.modified_count}")

except Exception as e:
    print(f"❌ Có lỗi xảy ra: {e}")

finally:
    if 'client' in locals():
        client.close()
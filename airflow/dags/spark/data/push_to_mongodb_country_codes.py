import csv
from pymongo import MongoClient, UpdateOne

# 1. Đường dẫn tới file CSV dữ liệu quốc gia
csv_file_path = 'country_code.csv'

# 2. Chuỗi kết nối bảo mật tới MongoDB trong Docker
uri = "mongodb://admin:123@127.0.0.1:27017/?authSource=admin"

try:
    client = MongoClient(uri)
    db = client["glamira"]            # Vào đúng database 'glamira'
    collection = db["countries"]      # Tạo collection tên là 'countries'

    operations = []

    # 3. Đọc file CSV
    with open(csv_file_path, mode='r', encoding='utf-8') as f:
        csv_reader = csv.DictReader(f)
        
        for row in csv_reader:
            # Lấy mã alpha-2 làm khóa chính _id
            country_id = row.get('alpha-2')
            
            # 🔥 FIX LỖI: Nếu dòng dữ liệu có dấu phẩy thừa, xóa key None để Mongo không bị crash
            row.pop(None, None)
            
            # Đảm bảo có ID thì mới xử lý
            if country_id:
                operations.append(
                    UpdateOne(
                        {"_id": country_id},
                        {"$set": row},
                        upsert=True
                    )
                )

    # 4. Thực thi ghi dữ liệu hàng loạt (Bulk Write)
    if operations:
        result = collection.bulk_write(operations)
        
        print("==== ĐÃ ĐẨY DỮ LIỆU COUNTRY CODES VÀO MONGO ====")
        print(f"👉 Số quốc gia mới được chèn (Insert): {result.upserted_count}")
        print(f"👉 Số quốc gia được cập nhật (Update): {result.modified_count}")
    else:
        print("⚠ Không tìm thấy dữ liệu hợp lệ trong file CSV!")

except Exception as e:
    print(f"❌ Có lỗi xảy ra: {e}")

finally:
    if 'client' in locals():
        client.close()
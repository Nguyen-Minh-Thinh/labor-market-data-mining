import requests
import time
from pymongo import MongoClient


# --- CẤU HÌNH ---
client = MongoClient("mongodb://localhost:27017/")
db = client["crawl_recruitment"]      
collection = db["vietnamworks_jobs"]  


#  URL API
API_URL = "https://ms.vietnamworks.com/job-search/v1.0/search"


# Headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Content-Type': 'application/json',
    'Referer': 'https://www.vietnamworks.com/',
    'Origin': 'https://www.vietnamworks.com',
}

# Payload
base_payload = {
    "userId": 0,
    "query": "",
    "filter": [
        {
            "field": "jobFunction",
            "value": "[{\"parentId\":5,\"childrenIds\":[-1]}]"
        }
    ],
    "ranges": [],
    "order": [],
    "hitsPerPage": 50,
    "page": 0,
    "retrieveFields": [
        "address", "benefits", "jobTitle", "salaryMax", "isSalaryVisible",
        "jobLevelVI", "isShowLogo", "salaryMin", "companyLogo", "userId",
        "jobLevel", "jobLevelId", "jobId", "jobUrl", "companyId",
        "approvedOn", "isAnonymous", "alias", "expiredOn", "industries",
        "industriesV3", "workingLocations", "services", "companyName",
        "salary", "onlineOn", "simpleServices", "visibilityDisplay",
        "isShowLogoInSearch", "priorityOrder", "skills",
        "profilePublishedSiteMask", "jobDescription", "jobRequirement",
        "prettySalary", "requiredCoverLetter", "languageSelectedVI",
        "languageSelected", "languageSelectedId", "typeWorkingId",
        "createdOn", "isAdrLiteJob"
    ],
    "summaryVersion": ""
}

def scrape_vietnamworks_simple():
    session = requests.Session()
    
    # Số lượng bản ghi muốn lấy
    total_records_needed = 1000
    hits_per_page = 50
    total_pages = (total_records_needed // hits_per_page) + 1
    
    print(f"--- Bắt đầu cào {total_records_needed} jobs vào MongoDB ---")

    for page in range(total_pages):
        try:
            # In ra màn hình để biết đang chạy đến đâu
            print(f"Đang cào trang {page + 1}/{total_pages}...", end=" ")
            
            # Cập nhật số trang
            current_payload = base_payload.copy()
            current_payload["page"] = page
            
            # Gửi request
            response = session.post(API_URL, headers=headers, json=current_payload, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                # Kiểm tra dữ liệu rỗng (hết job để cào)
                jobs = data.get('data', [])
                if not jobs:
                    print("\n>> Hết dữ liệu. Dừng lại!")
                    break
                
                # --- LƯU VÀO MONGODB ---
                if jobs:
                    collection.insert_many(jobs)
                    print(f"OK (Đã nạp {len(jobs)} records)")
                
            else:
                print(f"Lỗi HTTP {response.status_code}")
                if response.status_code == 429:
                    print(">> Bị chặn nhẹ, nghỉ 60s...")
                    time.sleep(60)

            time.sleep(1)

        except Exception as e:
            print(f"\n>> Có lỗi xảy ra ở trang {page}: {e}")
            time.sleep(5)

    print("\nĐã hoàn thành công việc!")

if __name__ == "__main__":
    scrape_vietnamworks_simple()

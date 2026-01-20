import requests
import time
import random
import json
from pymongo import MongoClient, ReplaceOne
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# ================= CẤU HÌNH CHUNG =================
MONGO_URI = "mongodb://mongodb:27017/"
DB_NAME = "crawl_recruitment"
COLLECTION_NAME = "vietnamworks_jobs" 

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# ================= PHẦN 1: CÀO API (Lấy danh sách Job) =================

def step_1_scrape_api():
    print("\n=== [BƯỚC 1/3] CÀO DỮ LIỆU TỪ API ===")
    
    API_URL = "https://ms.vietnamworks.com/job-search/v1.0/search"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Content-Type': 'application/json',
        'Referer': 'https://www.vietnamworks.com/',
        'Origin': 'https://www.vietnamworks.com',
    }

    base_payload = {
        "userId": 0, "query": "",
        "filter": [{"field": "jobFunction", "value": "[{\"parentId\":5,\"childrenIds\":[-1]}]"}],
        "ranges": [], "order": [], "hitsPerPage": 50, "page": 0,
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
            "createdOn", "isAdrLiteJob", "yearsOfExperience", "highestDegreeId"
        ],
        "summaryVersion": ""
    }

    session = requests.Session()
    total_records_needed = 10000
    hits_per_page = 50
    total_pages = (total_records_needed // hits_per_page) + 1
    
    print(f"Target: {total_records_needed} jobs -> Collection: {COLLECTION_NAME}")

    for page in range(total_pages):
        try:
            print(f"API Page {page + 1}/{total_pages}...", end=" ")
            current_payload = base_payload.copy()
            current_payload["page"] = page
            
            response = session.post(API_URL, headers=headers, json=current_payload, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                jobs = data.get('data', [])
                
                if not jobs:
                    print("\n>> Hết dữ liệu API. Dừng Bước 1.")
                    break
                
                ops = []
                for job in jobs:
                    if not collection.find_one({"jobId": job["jobId"]}):
                         collection.insert_one(job)

                print(f"OK (Đã xử lý {len(jobs)} records)")
            else:
                print(f"Lỗi HTTP {response.status_code}")
                if response.status_code == 429:
                    print(">> Bị chặn nhẹ, nghỉ 60s...")
                    time.sleep(60)
            
            time.sleep(1)

        except Exception as e:
            print(f"\n>> Lỗi ở trang {page}: {e}")
            time.sleep(5)
    
    print(">>> Hoàn thành Bước 1.")


# ================= PHẦN 2: CÀO CHI TIẾT (Selenium) =================

def _suppress_del_error(self):
    try: self.quit()
    except: pass
uc.Chrome.__del__ = _suppress_del_error

def clean_text(text):
    if not text: return ""
    return " ".join(text.split())

def element_to_text_list(element_html):
    if not element_html: return []
    soup_tmp = BeautifulSoup(str(element_html), "html.parser")
    for tag in soup_tmp.find_all(["br", "li", "p", "div", "tr", "ul"]): 
        tag.insert_after("\n")
    text = soup_tmp.get_text(separator="\n")
    return [clean_text(line) for line in text.split("\n") if len(clean_text(line)) > 1]

def get_job_content(driver, url):
    try:
        driver.get(url)
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
        except: return None

        # Click xem thêm
        try:
            expand_xpaths = ["//span[contains(text(), 'Xem đầy đủ')]", "//span[contains(text(), 'Xem thêm')]",
                             "//div[contains(text(), 'Xem thêm')]", "//button[contains(., 'Xem thêm')]", 
                             "//*[contains(@class, 'read-more')]"]
            for xpath in expand_xpaths:
                try:
                    btns = driver.find_elements(By.XPATH, xpath)
                    for btn in btns:
                        if btn.is_displayed():
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
                            time.sleep(1) 
                            driver.execute_script("arguments[0].click();", btn)
                            time.sleep(1) 
                except: pass
        except: pass

        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        def get_section_text(keywords):
            header = soup.find(lambda t: t.name in ['h2', 'h3', 'h4', 'strong'] and any(k in t.text for k in keywords))
            if header:
                content = header.find_next_sibling()
                if not content: content = header.parent.find_next_sibling()
                return element_to_text_list(str(content))
            return []

        def get_sidebar_info(keyword):
            target_text = soup.find(string=lambda t: t and keyword.lower() in t.lower())
            if target_text:
                label_tag = target_text.parent  
                value_tag = label_tag.find_next_sibling("p")
                if value_tag: return clean_text(value_tag.get_text())
                next_tag = label_tag.find_next_sibling()
                if next_tag: return clean_text(next_tag.get_text())
            return "Không yêu cầu / Khác"

        return {
            "clean_description": get_section_text(["Mô tả", "Description", "Trách nhiệm"]),
            "clean_requirement": get_section_text(["Yêu cầu", "Requirement", "Kỹ năng", "Skills"]),
            "clean_min_degree": get_sidebar_info("TRÌNH ĐỘ HỌC VẤN TỐI THIỂU")
        }
    except Exception as e:
        print(f"Lỗi Selenium: {e}")
        return None

def step_2_enrich_selenium():
    print("\n=== [BƯỚC 2/3] CÀO CHI TIẾT VỚI SELENIUM ===")

    # Test Lấy 5 cái
    # TEST_LIMIT = 5
    
    options = uc.ChromeOptions()
    options.page_load_strategy = 'normal'

    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    options.binary_location = "/usr/bin/chromium"

    driver = uc.Chrome(
        options=options,
        use_subprocess=True,
        driver_executable_path="/opt/airflow/chromedriver"
    )

    try:
        # Chỉ cào những job chưa có cờ is_full_crawled
        filter_query = {"is_full_crawled": {"$ne": True}}
        total_docs = collection.count_documents(filter_query)
        print(f"Tìm thấy {total_docs} jobs cần cập nhật nội dung.")

        cursor = collection.find(filter_query)

        for i, doc in enumerate(cursor):
            # Test lấy 5 cái
            # if i >= TEST_LIMIT:
            #     print(f"\n>>> ĐÃ ĐẠT GIỚI HẠN TEST ({TEST_LIMIT} jobs). Dừng bước 2 để chuyển sang bước 3.")
            #     break
            
            job_id = doc.get("jobId")
            url = doc.get("jobUrl")
            title = doc.get("jobTitle", "No Title")

            print(f"[{i+1}/{total_docs}] ID: {job_id} | {title[:30]}...", end=" ")

            if not url:
                print("-> Skip (No URL)")
                continue

            data = get_job_content(driver, url)

            if data:
                collection.update_one(
                    {"_id": doc["_id"]},
                    {
                        "$set": {
                            "jobDescription": data["clean_description"],
                            "jobRequirement": data["clean_requirement"],
                            "minDegree": data["clean_min_degree"],
                            "is_full_crawled": True,
                            "crawl_at": time.time()
                        }
                    }
                )
                has_desc = "Có Desc" if data["clean_description"] else "Thiếu Desc"
                print(f"-> OK ({has_desc})")
            else:
                collection.update_one({"_id": doc["_id"]}, {"$set": {"is_full_crawled": "Error"}})
                print("-> Fail")

            time.sleep(random.uniform(2, 4))

    except Exception as e:
        print(f"Lỗi Step 2: {e}")
    finally:
        try: driver.quit()
        except: pass
        print(">>> Hoàn thành Bước 2.")


FIELD_MAPPING = {
    "jobId": "job_id", "jobTitle": "job_title", "jobUrl": "job_url",
    "companyName": "company_name", "salaryCurrency": "salary_currency",
    "salary": "salary", "salaryMin": "salary_min", "salaryMax": "salary_max",
    "prettySalary": "pretty_salary", "jobDescription": "job_description",
    "jobRequirement": "job_requirement", "yearsOfExperience": "years_of_experience",
    "minDegree": "min_degree", "workingLocations": "working_locations",
    "address": "address", "languageSelectedVI": "language_selected_vi",
    "jobLevelVI": "job_level_vi", "createdOn": "created_on",
    "approvedOn": "approved_on", "expiredOn": "expired_on", "crawl_at": "crawl_at"
}

def step_3_clean_and_standardize():
    print("\n=== [BƯỚC 3/3] Lưu ===")
    
    cursor = collection.find({}) 
    
    bulk_ops = []
    processed_count = 0
    
    print("Đang xử lý mapping dữ liệu...")

    for job in cursor:
        try:
            if "job_id" in job and "jobId" not in job:
                continue

            clean_job = {}
            # Giữ lại _id cũ để replace đúng chỗ
            clean_job["_id"] = job["_id"]

            # 1. Map fields đơn giản
            for src, target in FIELD_MAPPING.items():
                if src in job:
                    clean_job[target] = job[src]

            # 2. Xử lý Logic phức tạp (Copy y nguyên logic cũ)
            
            # Industries V3
            raw_v3 = job.get("industriesV3", [])
            clean_job["industries_v3"] = []
            if isinstance(raw_v3, list):
                clean_job["industries_v3"] = [
                    item.get("industryV3NameVI") for item in raw_v3 
                    if isinstance(item, dict) and item.get("industryV3NameVI")
                ]

            # Industries Legacy
            raw_legacy = job.get("industries", [])
            legacy_names = []
            if isinstance(raw_legacy, list):
                legacy_names = [
                    item.get("industryNameVI") for item in raw_legacy 
                    if isinstance(item, dict) and item.get("industryNameVI")
                ]

            # Merge Industries
            clean_job["industries"] = list(set(legacy_names + clean_job["industries_v3"]))

            # Skills
            raw_skills = job.get("skills", [])
            clean_job["skills"] = []
            if isinstance(raw_skills, list):
                clean_job["skills"] = [
                    s.get("skillName") for s in raw_skills 
                    if isinstance(s, dict) and s.get("skillName")
                ]

            # Benefits
            raw_benefits = job.get("benefits", [])
            clean_job["benefits"] = {}
            if isinstance(raw_benefits, list):
                for b in raw_benefits:
                    if isinstance(b, dict):
                        key = b.get("benefitNameVI")
                        val = b.get("benefitValue")
                        if key: clean_job["benefits"][key] = val

            # Job Functions
            grp = job.get("groupJobFunctionsV3")
            clean_job["group_job_functions"] = grp.get("groupJobFunctionV3NameVI", "") if isinstance(grp, dict) else ""

            func = job.get("jobFunctionsV3")
            clean_job["job_functions"] = func.get("jobFunctionV3NameVI", "") if isinstance(func, dict) else ""
            
            # Working Locations
            raw_locs = job.get("workingLocations", [])
            clean_job["working_locations"] = []
            if isinstance(raw_locs, list):
                cities = set()
                for loc in raw_locs:
                    if isinstance(loc, dict):
                        c = loc.get("cityNameVI") or loc.get("cityName")
                        if c: cities.add(c)
                clean_job["working_locations"] = list(cities)
            
            # Thêm vào danh sách cập nhật (Thay thế document cũ bằng document mới sạch hơn)
            bulk_ops.append(ReplaceOne({"_id": job["_id"]}, clean_job))
            
            # Thực thi Bulk Write mỗi 500 docs
            if len(bulk_ops) >= 500:
                collection.bulk_write(bulk_ops)
                processed_count += len(bulk_ops)
                print(f"-> Đã làm sạch và cập nhật {processed_count} jobs...")
                bulk_ops = []

        except Exception as e:
            print(f"Lỗi clean job {job.get('jobId', 'unknown')}: {e}")

    # Xử lý số dư còn lại
    if bulk_ops:
        collection.bulk_write(bulk_ops)
        processed_count += len(bulk_ops)
        print(f"-> Đã làm sạch và cập nhật {processed_count} jobs...")

    print(">>> Hoàn thành Bước 3.")

# ================= MAIN RUNNER =================

if __name__ == "__main__":
    print(f"=== KHỞI ĐỘNG PIPELINE DATA MINING ===")
    print(f"Database: {DB_NAME} | Collection: {COLLECTION_NAME}")
    
    # Chạy tuần tự 3 bước
    step_1_scrape_api()        
    step_2_enrich_selenium()   
    step_3_clean_and_standardize() 
    
    print("\n=== TOÀN BỘ QUÁ TRÌNH HOÀN TẤT ===")
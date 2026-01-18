from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth

import requests
from bs4 import BeautifulSoup
import re

from pymongo import MongoClient
from datetime import datetime
import time

# ================== CONFIG ==================

START_PAGE = 1
MAX_JOBS = 3000
SLEEP_EACH_PAGE = 1

BASE_URL = (
    "https://careerviet.vn/viec-lam/"
    "cntt-phan-cung-mang-cntt-phan-mem-c63,1-trang-{}-vi.html"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "vi-VN,vi;q=0.9"
}

options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

options.binary_location = "/usr/bin/chromium"

service = Service("/usr/bin/chromedriver")
driver = webdriver.Chrome(service=service, options=options)

stealth(
    driver,
    languages=["vi-VN", "vi", "en-US"],
    vendor="Google Inc.",
    platform="Win32",
    webgl_vendor="Intel Inc.",
    renderer="Intel Iris OpenGL Engine",
    fix_hairline=True,
)

wait = WebDriverWait(driver, 15)

# ================== MONGO ==================

client = MongoClient("mongodb://mongodb:27017/")
db = client["crawl_recruitment"]
collection = db["careerviet_jobs"]

collection.create_index([("job_url", 1)], unique=True)

# ================== UTILS ==================

def scroll_to_load_jobs():
    last = 0
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        items = driver.find_elements(By.CSS_SELECTOR, "div.job-item")
        if len(items) == last:
            break
        last = len(items)

def extract_p_text(p):
    if not p:
        return ""
    texts = list(p.strings)
    raw = " ".join(t.strip() for t in texts if t.strip())
    return re.sub(r"\s+", " ", raw).strip()

# ================== DETAIL (BEAUTIFULSOUP) ==================

def crawl_detail_bs(job_url):
    r = requests.get(job_url, headers=HEADERS, timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")

    data = {
        "title": "",
        "company": "",
        "salary": "",
        "experience": "",
        "level": "",
        "employment_type": "",
        "industry": "",
        "deadline": "",
        "updated_at": "",
        "job_description": [],
        "job_requirement": [],
        "benefits": [],
        "other_info": [],
        "skills": []
    }

    # TITLE
    h1 = soup.select_one("section.apply-now-banner h1.title")
    if h1:
        data["title"] = h1.get_text(strip=True)

    # COMPANY
    c = soup.select_one("section.apply-now-banner a.job-company-name")
    if c:
        data["company"] = c.get_text(strip=True)

    # INFO BOX
    def get_info(label):
        for li in soup.select("li"):
            strong = li.find("strong")
            if strong and label in strong.get_text():
                p = li.find("p")
                return extract_p_text(p)
        return ""

    data["salary"] = get_info("Lương")
    data["experience"] = get_info("Kinh nghiệm")
    data["level"] = get_info("Cấp bậc")
    data["employment_type"] = get_info("Hình thức")
    data["industry"] = get_info("Ngành nghề")
    data["deadline"] = get_info("Hết hạn nộp")
    data["updated_at"] = get_info("Ngày cập nhật")

    for row in soup.select("div.detail-row"):
        title = row.select_one(".detail-title")
        if not title:
            continue

        t = title.get_text(strip=True).lower()

        items = [
            re.sub(r"\s+", " ", li.get_text(strip=True))
            for li in row.select("li")
            if li.get_text(strip=True)
        ]

        if "mô tả" in t:
            data["job_description"] = items
        elif "yêu cầu" in t:
            data["job_requirement"] = items
        elif "phúc lợi" in t:
            data["benefits"] = items
        elif "thông tin khác" in t:
            data["other_info"] = items

    # SKILLS
    data["skills"] = [
        a.get_text(strip=True)
        for a in soup.select(".job-tags ul li a")
        if a.get_text(strip=True)
    ]

    return data

# ================== MAIN ==================

total = 0
page = START_PAGE

while True:
    print(f"\n▶ Page {page}")
    driver.get(BASE_URL.format(page))

    try:
        wait.until(EC.presence_of_element_located((By.ID, "jobs-side-list-content")))
    except:
        break

    scroll_to_load_jobs()

    job_items = driver.find_elements(By.CSS_SELECTOR, "div.job-item")
    if not job_items:
        break

    print(f"🔎 Found {len(job_items)} jobs")

    jobs_meta = []

    # ===== LIST PARSE =====
    for item in job_items:
        try:
            link = item.find_element(By.CSS_SELECTOR, "h2 a.job_link")
            job_url = link.get_attribute("href")
            title = link.text.strip()
        except:
            continue

        try:
            company = item.find_element(By.CSS_SELECTOR, "a.company-name").text.strip()
        except:
            company = ""

        try:
            salary = item.find_element(By.CSS_SELECTOR, "div.salary p").text \
                .replace("Lương:", "").strip()
        except:
            salary = ""

        locations = [
            li.text.strip()
            for li in item.find_elements(By.CSS_SELECTOR, "div.location ul li")
        ]

        jobs_meta.append({
            "job_url": job_url,
            "title": title,
            "company": company,
            "salary": salary,
            "location": locations
        })

    # ===== DETAIL PARSE =====
    for meta in jobs_meta:
        if MAX_JOBS and total >= MAX_JOBS:
            break

        try:
            detail = crawl_detail_bs(meta["job_url"])

            job = {
                "job_url": meta["job_url"],
                "title": detail["title"] or meta["title"],
                "company": detail["company"] or meta["company"],
                "salary": detail["salary"] or meta["salary"],
                "location": meta["location"],
                "experience": detail["experience"],
                "level": detail["level"],
                "employment_type": detail["employment_type"],
                "industry": detail["industry"],
                "deadline": detail["deadline"],
                "updated_at": detail["updated_at"],
                "job_description": detail["job_description"],
                "job_requirement": detail["job_requirement"],
                "benefits": detail["benefits"],
                "other_info": detail["other_info"],
                "skills": detail["skills"],
                "source": "careerviet",
                "crawl_at": datetime.now()
            }

            collection.update_one(
                {"job_url": job["job_url"]},
                {"$set": job},
                upsert=True
            )

            total += 1
            print(f"Đã thu thập {total} jobs")
        except Exception as e:
            print("✖ Job lỗi:", e)

    if MAX_JOBS and total >= MAX_JOBS:
        break

    page += 1
    time.sleep(SLEEP_EACH_PAGE)

driver.quit()
print(f"\nDONE: {total} jobs")

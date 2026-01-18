import time
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException
)

from pymongo import MongoClient


# ================== CHROME CONFIG ==================
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

options.binary_location = "/usr/bin/chromium"

service = Service("/usr/bin/chromedriver")
driver = webdriver.Chrome(service=service, options=options)


# ================== MONGODB ==================
client = MongoClient("mongodb://mongodb:27017/")
db = client["crawl_recruitment"]
collection = db["topcv_jobs"]


# ================== HELPER FUNCTIONS ==================
def clean_text_list(text):
    return [t.strip() for t in text.split("\n") if t.strip()]


def wait_element_text(parent, by, value, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        try:
            el = parent.find_element(by, value)
            if el.text.strip():
                return el
        except NoSuchElementException:
            pass
        time.sleep(0.5)
    raise TimeoutException(f"Element {value} has no text")


# ================== MAIN ==================
wait = WebDriverWait(driver, 20)
actions = ActionChains(driver)

count = 0

try:
    for i in range(1, 61):
        driver.get(
            f"https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257?page={i}"
        )

        content = wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, "wrapper-content"))
        )
        job_list = content.find_elements(By.CLASS_NAME, "job-item-search-result")

        for job in job_list:
            job_data = {}
            original_window = driver.current_window_handle

            try:
                # ===== LISTING INFO =====
                body_box = job.find_element(By.CLASS_NAME, "body-box")
                job_data["title"] = body_box.find_element(By.TAG_NAME, "span").text.strip()
                job_data["company"] = body_box.find_element(
                    By.CLASS_NAME, "company-name"
                ).text.strip()

                link = job.find_element(By.TAG_NAME, "a")
                wait.until(EC.element_to_be_clickable(link))

                windows_before = driver.window_handles
                actions.move_to_element(link).click().perform()

                WebDriverWait(driver, 10).until(
                    lambda d: len(d.window_handles) > len(windows_before)
                )

                new_window = next(
                    w for w in driver.window_handles if w not in windows_before
                )
                driver.switch_to.window(new_window)

                # ===== JOB DETAIL =====
                job_detail = wait.until(
                    EC.presence_of_element_located(
                        (By.CLASS_NAME, "job-detail__wrapper")
                    )
                )

                info_sections = wait_element_text(
                    job_detail, By.CLASS_NAME, "job-detail__info--sections"
                )

                job_data["salary"] = clean_text_list(
                    info_sections.find_element(
                        By.CLASS_NAME, "section-salary"
                    )
                    .find_element(
                        By.CLASS_NAME, "job-detail__info--section-content"
                    ).text
                )

                job_data["location"] = clean_text_list(
                    info_sections.find_element(
                        By.CLASS_NAME, "section-location"
                    )
                    .find_element(
                        By.CLASS_NAME, "job-detail__info--section-content"
                    ).text
                )

                job_data["experience"] = clean_text_list(
                    info_sections.find_element(
                        By.CLASS_NAME, "section-experience"
                    )
                    .find_element(
                        By.CLASS_NAME, "job-detail__info--section-content"
                    ).text
                )

                # ===== COMPANY INFO =====
                common_info = wait_element_text(
                    job_detail, By.CLASS_NAME, "job-detail__body-right"
                )

                company_info = common_info.find_element(
                    By.CLASS_NAME, "job-detail__company--information"
                )

                job_data["company_name"] = clean_text_list(
                    company_info.find_element(By.CLASS_NAME, "company-name").text
                )
                job_data["company_scale"] = clean_text_list(
                    company_info.find_element(By.CLASS_NAME, "company-scale").text
                )
                job_data["company_field"] = clean_text_list(
                    common_info.find_element(By.CLASS_NAME, "company-field").text
                )
                job_data["company_address"] = clean_text_list(
                    common_info.find_element(By.CLASS_NAME, "company-address").text
                )

                # ===== DEADLINE =====
                deadline_div = job_detail.find_element(
                    By.CLASS_NAME, "job-detail__info--deadline"
                )
                deadline_text = deadline_div.get_attribute("innerText")
                for s in deadline_div.find_elements(By.TAG_NAME, "span"):
                    deadline_text = deadline_text.replace(s.text, "")
                job_data["deadline"] = clean_text_list(deadline_text)

                # ===== TAGS =====
                job_data["tags"] = {}
                for group in job_detail.find_elements(
                    By.CLASS_NAME, "job-tags__group"
                ):
                    name = group.find_element(
                        By.CLASS_NAME, "job-tags__group-name"
                    ).text.strip()
                    items = [
                        i.text.strip()
                        for i in group.find_elements(By.CLASS_NAME, "item")
                        if i.text.strip()
                    ]
                    if name and items:
                        job_data["tags"][name] = items

                # ===== DESCRIPTIONS =====
                job_data["descriptions"] = {}
                for desc in job_detail.find_elements(
                    By.CLASS_NAME, "job-description__item"
                ):
                    title = desc.find_element(By.TAG_NAME, "h3").text.strip()
                    content = (
                        desc.find_element(
                            By.CLASS_NAME, "job-description__item--content"
                        )
                        .text.replace("\n", " ")
                        .strip()
                    )
                    if title:
                        job_data["descriptions"][title] = content

                job_data["source"] = "topcv"
                job_data["crawl_at"] = datetime.now()

                collection.insert_one(job_data)
                count += 1
                print(f"Đã thu thập {count} jobs")

            except Exception as e:
                print(f"Job error: {e}")

            finally:
                try:
                    if driver.current_window_handle != original_window:
                        driver.close()
                        driver.switch_to.window(original_window)
                except WebDriverException:
                    pass

            time.sleep(0.2)

        time.sleep(5)

finally:
    driver.quit()

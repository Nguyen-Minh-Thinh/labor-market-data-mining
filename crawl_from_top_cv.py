from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import time
from pymongo import MongoClient
from datetime import datetime
from selenium.webdriver.chrome.service import Service
from selenium_stealth import stealth

options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")

# Ẩn dấu hiệu automation
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

driver = webdriver.Chrome(options=options)

stealth(
    driver,
    languages=["vi-VN", "vi", "en-US", "en"],
    vendor="Google Inc.",
    platform="Win32",
    webgl_vendor="Intel Inc.",
    renderer="Intel Iris OpenGL Engine",
    fix_hairline=True,
)


client = MongoClient("mongodb://localhost:27017/")

db = client["crawl_recruitment"]      # database 
collection = db["topcv_jobs"]         # collection



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


# ================= DRIVER =================


for i in range(55, 61):
    driver = webdriver.Chrome()
    driver.get(f'https://www.topcv.vn/tim-viec-lam-cong-nghe-thong-tin-cr257?page={i}')

    wait = WebDriverWait(driver, 20)
    actions = ActionChains(driver)

    content = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'wrapper-content')))
    job_list = content.find_elements(By.CLASS_NAME, 'job-item-search-result')


    # ================= MAIN LOOP =================

    for job in job_list:
        job_data = {}
        original_window = driver.current_window_handle

        try:
            # ===== LISTING INFO =====
            body_box = job.find_element(By.CLASS_NAME, 'body-box')
            job_data["title"] = body_box.find_element(By.TAG_NAME, 'span').text.strip()
            job_data["company"] = body_box.find_element(By.CLASS_NAME, 'company-name').text.strip()

            link = job.find_element(By.TAG_NAME, "a")
            wait.until(EC.element_to_be_clickable(link))

            windows_before = driver.window_handles
            actions.move_to_element(link).click().perform()

            WebDriverWait(driver, 10).until(
                lambda d: len(d.window_handles) > len(windows_before)
            )

            new_window = next(w for w in driver.window_handles if w not in windows_before)
            driver.switch_to.window(new_window)

            # ===== JOB DETAIL =====
            job_detail = wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "job-detail__wrapper"))
            )

            # ===== INFO SECTIONS =====
            info_sections = wait_element_text(
                job_detail, By.CLASS_NAME, 'job-detail__info--sections'
            )

            job_data["salary"] = clean_text_list(
                info_sections.find_element(By.CLASS_NAME, 'section-salary')
                .find_element(By.CLASS_NAME, 'job-detail__info--section-content').text
            )

            job_data["location"] = clean_text_list(
                info_sections.find_element(By.CLASS_NAME, 'section-location')
                .find_element(By.CLASS_NAME, 'job-detail__info--section-content').text
            )

            job_data["experience"] = clean_text_list(
                info_sections.find_element(By.CLASS_NAME, 'section-experience')
                .find_element(By.CLASS_NAME, 'job-detail__info--section-content').text
            )

            # ===== COMPANY INFO =====
            common_info = wait_element_text(
                job_detail, By.CLASS_NAME, 'job-detail__body-right'
            )

            company_info = common_info.find_element(By.CLASS_NAME, 'job-detail__company--information')

            job_data["company_name"] = clean_text_list(
                company_info.find_element(By.CLASS_NAME, 'company-name').text
            )
            job_data["company_scale"] = clean_text_list(
                company_info.find_element(By.CLASS_NAME, 'company-scale').text
            )
            job_data["company_field"] = clean_text_list(
                common_info.find_element(By.CLASS_NAME, 'company-field').text
            )
            job_data["company_address"] = clean_text_list(
                common_info.find_element(By.CLASS_NAME, 'company-address').text
            )

            # ===== DEADLINE (NO SPAN) =====
            deadline_div = job_detail.find_element(By.CLASS_NAME, "job-detail__info--deadline")
            deadline_text = deadline_div.get_attribute("innerText")
            for s in deadline_div.find_elements(By.TAG_NAME, "span"):
                deadline_text = deadline_text.replace(s.text, "")
            job_data["deadline"] = clean_text_list(deadline_text)

            # ===== TAGS =====
            job_data["tags"] = {}
            for group in job_detail.find_elements(By.CLASS_NAME, "job-tags__group"):
                name = group.find_element(By.CLASS_NAME, "job-tags__group-name").text.strip()
                items = [
                    i.text.strip()
                    for i in group.find_elements(By.CLASS_NAME, "item")
                    if i.text.strip()
                ]
                if name and items:
                    job_data["tags"][name] = items

            # ===== DESCRIPTIONS =====
            job_data["descriptions"] = {}
            for desc in job_detail.find_elements(By.CLASS_NAME, "job-description__item"):
                title = desc.find_element(By.TAG_NAME, "h3").text.strip()
                content = desc.find_element(By.CLASS_NAME, "job-description__item--content") \
                    .text.replace("\n", " ").strip()
                if title:
                    job_data["descriptions"][title] = content

            # print(job_data)
            job_data["source"] = "topcv"
            job_data["crawl_at"] = datetime.now()
            collection.insert_one(job_data)

        except Exception as e:
            print(f"Job error: {e}")

        finally:
            try:
                if driver.current_window_handle != original_window:
                    driver.close()
                    driver.switch_to.window(original_window)
            except WebDriverException:
                pass
        time.sleep(1)
    time.sleep(10)
    driver.quit()

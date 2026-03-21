from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
import os
import time
import glob


# Configuration
URL = "https://www.nseindia.com/market-data/live-equity-market?symbol=NIFTY%2050"
DOWNLOAD_DIR = "./Stock_Data_Pipeline/nifty50_csv"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def main():
    chrome_options = Options()
    # chrome_options.add_argument("--headless")  # REMOVE for debugging
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")  # Full size to avoid layout issues
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    prefs = {
        "download.default_directory": os.path.abspath(DOWNLOAD_DIR),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "safebrowsing.disable_download_protection": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        print("🔄 Loading NSE page...")
        driver.get(URL)
        
        # Extra long wait for full page load (NSE is slow)
        time.sleep(15)
        
        print("📄 Page title:", driver.title)
        
        # Wait for download link to exist (your inspection)
        wait = WebDriverWait(driver, 40)
        download_link = wait.until(EC.presence_of_element_located((By.ID, "dnldEquityStock")))
        print("✅ Found download link (id=dnldEquityStock)")
        
        # Scroll into view + wait to be clickable
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", download_link)
        time.sleep(3)
        
        # TRY 1: Normal click (might be blocked)
        try:
            download_link.click()
            print("✅ Normal click worked!")
        except Exception as e:
            print("❌ Normal click failed:", str(e)[:100])
            # TRY 2: JavaScript click (bypasses ALL overlays/popups)
            driver.execute_script("arguments[0].click();", download_link)
            print("✅ JavaScript click executed!")
        
        print("⏳ Waiting for download (5s)...")
        time.sleep(5)  # NSE downloads are slow
        
        # Rename latest CSV
        files = glob.glob(os.path.join(DOWNLOAD_DIR, "*.csv"))
        if files:
            latest_file = max(files, key=os.path.getctime)
            today_str = datetime.now().strftime("%Y%m%d_%H%M")
            old_path = latest_file
            new_path = os.path.join(DOWNLOAD_DIR, f"nifty50_{today_str}.csv")
            os.rename(old_path, new_path)
            print("✅ SUCCESS! Saved:", new_path)
            print("📊 Size:", os.path.getsize(new_path), "bytes")
        else:
            print("❌ No CSV found. Check:", DOWNLOAD_DIR)
            print("Latest files:", os.listdir(DOWNLOAD_DIR)[-5:])
            
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
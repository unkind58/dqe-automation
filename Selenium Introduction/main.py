import time
import os
import pandas as pd
import itertools


from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.service import Service


class SeleniumWebDriverContextManager:
    def __init__(self,chromedriver_path, file_path):
        self.chromedriver_path = chromedriver_path
        self.file_path = file_path 
        self.driver = None

    def __enter__(self):
        service = Service(executable_path=self.chromedriver_path)
        self.driver = webdriver.Chrome(service=service)
        return self.driver


    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            print(f"Exception type: {exc_type}")
            print(f"Exception value: {exc_value}")
            print(f"Traceback: {traceback}")

        if self.driver:
            self.driver.quit()


if __name__ == "__main__":
    chromedriver_path = os.path.abspath("chromedriver/chromedriver.exe")
    file_path = os.path.abspath("report/report.html")
    
    screenshots_folder = "screenshots"
    csv_folder = "csv_doughnut"
    tables_folder = "tables"
    os.makedirs(tables_folder, exist_ok=True)
    os.makedirs(csv_folder, exist_ok=True)
    os.makedirs(screenshots_folder, exist_ok=True)
    
    with SeleniumWebDriverContextManager(chromedriver_path, file_path) as driver:
        driver.get(f"file://{file_path}")
        time.sleep(4)
    
        """ Table scraping part """
        element = driver.find_element(By.CLASS_NAME, "table")
        raw_text = element.text.strip().split('\n')

        # Predefined column names
        headers = ["Facility Type", "Visit Date", "Average Time Spent"]
        
        # Fetch indexes 
        facility_type = raw_text.index(headers[0])
        visit_date = raw_text.index(headers[1])
        avg_time_spent = raw_text.index(headers[2])

        # Fetch rows for each of the columns
        facility_type_col = raw_text[:facility_type]
        visit_date_col = raw_text[facility_type+1:visit_date]
        avg_time_spent_col = raw_text[visit_date+1:avg_time_spent]
        rows = list(zip(facility_type_col, visit_date_col, avg_time_spent_col))
        
        # Create data frame and save to CSV
        df = pd.DataFrame(rows, columns=headers)
        table_path = os.path.join(tables_folder, "table.csv")
        df.to_csv(table_path, index=False, encoding="utf-8")

        """ Doughnut Chart part """
               
        element = driver.find_element(By.CLASS_NAME, "pielayer")
        
        # Create index generator
        counter = itertools.count()
        idx = next(counter)
 
        # Overall screenshot
        screenshot_path = os.path.join(screenshots_folder, f"screenshot{idx}.png")
        element.screenshot(screenshot_path)

        # Need to filter out all possible combinations
        filtering_combinations = [
            (0, 1),
            (1, 2),
            (0, 2)
        ]

        for idx, (i, j) in enumerate(filtering_combinations):
            idx = next(counter)
            legend_toggles = driver.find_elements(By.CSS_SELECTOR, "rect.legendtoggle")
            legend_labels = driver.find_elements(By.CSS_SELECTOR, "text.legendtext")
            print(f"Clicking toggles: {i} ({legend_labels[i].text}), {j} ({legend_labels[j].text})")

            legend_toggles[i].click()
            time.sleep(1) 
            legend_toggles[j].click()
            time.sleep(2) 

            slice_texts = driver.find_elements(By.CSS_SELECTOR, "text.slicetext")
            for slice_text in slice_texts:
                tspans = slice_text.find_elements(By.TAG_NAME, "tspan")
                pie_data = []
                label = tspans[0].text
                value = tspans[1].text
                pie_data.append((label, value))
                df_doughnut = pd.DataFrame(pie_data, columns=["Facility Type", "Min Average Time Spent"])
                csv_path = os.path.join(csv_folder, f"doughnut{idx}.csv")
                df_doughnut.to_csv(csv_path, index=False, encoding="utf-8")
                    
            # Screenshot of filtered element
            element = driver.find_element(By.CLASS_NAME, "pielayer")
            screenshot_path = os.path.join(screenshots_folder, f"screenshot{idx}.png")
            element.screenshot(screenshot_path)
            time.sleep(2)
            
            # Restore filters
            driver.refresh()
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "pielayer"))
)

            time.sleep(1)

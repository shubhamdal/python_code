from selenium import webdriver
from selenium.webdriver.common.keys import Keys

driver = webdriver.Chrome(executable_path='C:\Program Files (x86)\Google\Chrome\Application\Chrome.exe')
print(driver.current_url)

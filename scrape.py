import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time

# Telegram Bot Info
TELEGRAM_BOT_TOKEN = '<YourBOTToken>'
TELEGRAM_CHAT_ID = '<YourChatID>'


def send_telegram_message(message):
  """Send a message to a Telegram chat via bot."""
  url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage?chat_id={TELEGRAM_CHAT_ID}&text={message}"
  response = requests.get(url)
  return response.json()



def get_text(url):
  # Open the webpage
  driver.get(url)

  # Check if the consent button is present and clickable
  consent_button_xpath = '/html/body/div[3]/div/div[2]/div/span[2]/button[2]'
  consent_buttons = driver.find_elements(By.XPATH, consent_button_xpath)

  # If the consent button is found, click it
  if len(consent_buttons) > 0:
    try:
      consent_buttons[0].click()
      print("Consent button clicked.")
    except Exception as e:
      print(f"Error clicking the consent button: {e}")

  # Now wait for the article to be present
  article_text = ""  # Initialize article_text
  try:
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "article")))
    print("Article tag is present.")

    # Once the article tag is confirmed to be present, you can extract its content
    article = driver.find_element(By.TAG_NAME, 'article')
    article_text = article.text  # This gets all the text contained within the <article> element
    print(article_text)
  except Exception as e:
    print(f"Error finding the article tag: {e}")

  return article_text




# Initialize the WebDriver
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)
known_links = set()



def check_for_new_links(known_links):

  # Open the News Announcements Page
  url = 'https://sudovi.me/ascg/kategorija/Qa' 
  driver.get(url)

  # Click consent button if present
  consent_button_xpath = '/html/body/div[3]/div/div[2]/div/span[2]/button[2]'
  consent_buttons = driver.find_elements(By.XPATH, consent_button_xpath)
  if len(consent_buttons) > 0:
    try:
      consent_buttons[0].click()
      print("Consent button clicked.")
    except Exception as e:
      print(f"Error clicking the consent button: {e}")

  # Wait for the page to load and for links to be present
  WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'Kwon')]")))
  print("Content related to 'Kwon' is present.")

  # Find all links containing the text 'Kwon'
  links = driver.find_elements(By.XPATH, "//a[contains(., 'Kwon')]")
  hrefs = [link.get_attribute('href') for link in links]
  new_links = []

  for href in hrefs:
    if href not in known_links:
      new_links.append(href)
      known_links.add(href)
      # get_text(href)  # Navigate to each href and get the text there.

  return new_links

# Main loop
try:
  while True:
    new_links = check_for_new_links(known_links)
    for link in new_links:
      message = f"Found new link: {link}"
      print(message)
      # send_telegram_message(message)
    time.sleep(60 * 10)  # Check every 10 minutes
finally:
  driver.quit()
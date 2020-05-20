from selenium import webdriver
from pygame import mixer

print("Enter the URL which you want to monitor")
#url=input()
url="www.google.com"
url="https://"+url
#url="https://bucallprocess.callsource.com/leadscore/checkForCalls.do"
print("You entered: "+url)
driver = webdriver.Chrome(r"C:/Users/shuchawla/Documents/Python_Scripting/chromedriver.exe")
driver.get(url)
mixer.init()
mixer.music.load('C:/Users/shuchawla/Downloads/aa.mp3')
while(1):
    try:
        print("Got element:"+str(driver.find_elements_by_class_name("gb_lc")))
        mixer.music.play()
        break
    except:
        print("Couldn't find exception")

print("Enter s to stop")
a=''
while(a!='s'):
    a=input()
    if(a=='s'):
        mixer.music.stop()
    else:
        continue
    

import schedule
import time
import subprocess

def job():
    # Call your script or function here
    subprocess.run(["python3", "/workspaces/Convious/extract_and_load.py"])

# Schedule the job every 6 hours
schedule.every(6).hours.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
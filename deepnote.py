import re
from urllib.parse import unquote
import requests
import schedule
import pytz
import time

def extract_variables_from_link(link):
    # Define the regular expression pattern to match the link format
    pattern = r"https://deepnote.com/workspace/([^/]+)-([^/]+)/project/([^/]+)/notebook/([^/]+)-([^/]+)"

    # Use re.match to find the variables in the link
    match = re.match(pattern, link)

    if match:
        # Extract variables from the matched groups
        workspace_name, workspace_id, project_name_encoded, notebook_name, notebook_id = match.groups()

        # Decode the project name (it might be URL-encoded)
        project_name = unquote(project_name_encoded)
        project_id = "-".join(project_name.split("-")[-5:])

        return {
            "Workspace Name": workspace_name,
            "Workspace ID": workspace_id,
            "Project Name": project_name,

            "Project ID": project_id,
            "Notebook Name": notebook_name,
            "Notebook ID": notebook_id
        }
    else:
        return None
def excute(project_id,notebook_id,api,jsn):
  urlOfExcute = f"https://api.deepnote.com/v1/projects/{project_id}/notebooks/{notebook_id}/execute"
  res = requests.post(urlOfExcute,headers={"Authorization":api})
  print(f"timeInterval : {jsn['timeInterval']} \n{jsn['myProject']} -> {jsn['projects']} \n Link : {jsn['link']}")
  print(res.text)
  print()

scheduled_tasks = {}  # Dictionary to store scheduled tasks

# Function to schedule tasks based on timeInterval
def schedule_tasks(timeInterval, projects):
    tasks = []  # List to store scheduled tasks for this timeInterval
    for myProject, project_data in projects.items():
        for project_name, project_details in project_data.items():
            link = project_details["link"]
            api = project_details["api"]
            resData = extract_variables_from_link(link)
            project_id = resData["Project ID"]
            notebook_id = resData["Notebook ID"]
            jsn = {
                "myProject": myProject,
                "projects": project_name,
                "timeInterval": timeInterval,
                "link": link,
            }

            if timeInterval == "hourly":
                task = schedule.every(project_details["time"]).hour.do(
                    lambda project_id=project_id, notebook_id=notebook_id, api=api, jsn=jsn: excute(project_id, notebook_id, api, jsn)
                )
                tasks.append(task)
            elif timeInterval == "minutely":
                task = schedule.every(project_details["time"]).minutes.do(
                    lambda project_id=project_id, notebook_id=notebook_id, api=api, jsn=jsn: excute(project_id, notebook_id, api, jsn)
                )
                tasks.append(task)
            elif timeInterval == "daily":
                task = schedule.every().day.at(project_details["time"], pytz.timezone("Asia/Yangon")).do(
                    lambda project_id=project_id, notebook_id=notebook_id, api=api, jsn=jsn: excute(project_id, notebook_id, api, jsn)
                )
                tasks.append(task)
    
    scheduled_tasks[timeInterval] = tasks  # Store scheduled tasks for this timeInterval

# Schedule tasks for each timeInterval
for timeInterval, projects in myProjects.items():
    schedule_tasks(timeInterval, projects)

# Run the scheduled tasks
while True:
    schedule.run_pending()  # Run pending tasks
    time.sleep(1)  # Sleep for 1 sec (adjust as needed)

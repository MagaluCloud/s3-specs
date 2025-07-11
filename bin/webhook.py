#!/usr/bin/env python3
import requests
import sys
import os
import glob

def send_notification(webhook_url, failed_string, failed_string_see_more, git_run_url, num_falhas, filename, object_path, profile):
    app_message = {
        'cardsV2': [{
            'cardId': 'createCardMessage',
            'card': {
                'header': {
                    'title': f'Test Results: on profile {profile}',
                    'subtitle': f'{num_falhas} failed' if num_falhas else 'All tests passed',
                    'imageUrl': 'https://avatars.githubusercontent.com/u/146738539?s=200&v=4',
                    'imageType': 'CIRCLE'
                },
                'sections': [
                    {
                        "header": f'Test File: {os.path.basename(filename)}',
                        "collapsible": True,
                        "uncollapsibleWidgetsCount": 1 if num_falhas else 0,
                        "widgets": [
                            {
                                "textParagraph": {
                                    "text": failed_string if failed_string else "No failed tests"
                                }
                            },
                            {
                                "textParagraph": {
                                    "text": failed_string_see_more if failed_string_see_more else "No assertion errors"
                                }
                            },
                            {
                                'buttonList': {
                                    'buttons': [
                                        {
                                            'text': 'View run on GitHub',
                                            'onClick': {
                                                'openLink': {
                                                    'url': git_run_url
                                                }
                                            }
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                ]
            }
        }]
    }

    message_headers = {"Content-Type": "application/json; charset=UTF-8"}
    response = requests.post(webhook_url, json=app_message, headers=message_headers)
    print(f"Notification sent for {filename}. Status: {response.status_code}")

def process_file(file_path):
    with open(file_path, "r") as file:
        lines = file.readlines()

    failed_line = [line.strip() for line in lines if line.startswith("FAILED")]
    failed_string = "\n".join(failed_line)

    failed_line_see_more = [line.strip() for line in lines if line.startswith("assert")]
    failed_string_see_more = "\n".join(failed_line_see_more)

    return failed_string, failed_string_see_more

def count_fails(file_path):
    with open(file_path, "r") as f:
        return f.read().count("\nFAILED ")

def main():
    if len(sys.argv) != 8:
        print("Usage: python script.py WEBHOOK_URL LOG_PATH GITHUB_REPOSITORY GITHUB_RUN_ID OBJECT_PATH PROFILE GITHUB_JOB")
        return

    webhook_url = sys.argv[1]
    log_path = sys.argv[2]
    github_repository = sys.argv[3]
    github_run_id = sys.argv[4]
    object_path = sys.argv[6]
    profile = sys.argv[7]
    git_run_url = f"https://github.com/{github_repository}/actions/runs/{github_run_id}"

    if os.path.isfile(log_path):
        files_to_process = [log_path]
    elif os.path.isdir(log_path):
        files_to_process = glob.glob(os.path.join(log_path, "*.log")) + glob.glob(os.path.join(log_path, "*.tap"))
    else:
        print(f"Error: Path {log_path} does not exist")
        return

    if not files_to_process:
        print("No log files found to process")
        return

    any_failures = False
    first_file = files_to_process[0]

    for file_path in files_to_process:
        print(f"\nProcessing file: {file_path}")
        try:
            num_falhas = count_fails(file_path)
            failed_string, failed_string_see_more = process_file(file_path)

            print(f"Found {num_falhas} failures")
            print("Failed tests:", failed_string or "None")
            print("Assertion errors:", failed_string_see_more or "None")

            if num_falhas > 0:
                any_failures = True
                send_notification(
                    webhook_url,
                    failed_string,
                    failed_string_see_more,
                    git_run_url,
                    num_falhas,
                    file_path,
                    object_path,
                    profile
                )
 
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
            send_notification(
                webhook_url,
                f"Exception while processing file: {file_path}",
                str(e),
                git_run_url,
                1,
                file_path,
                object_path
            )
            any_failures = True 

if __name__ == "__main__":
    main()

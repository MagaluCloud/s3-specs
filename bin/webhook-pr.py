#!/usr/bin/env python3
import argparse
import requests

def send_gchat_notification(webhook_url, pr_number, pr_title, pr_url, repo_name):
    message = {
        "cardsV2": [{
            "cardId": "prReadyCard",
            "card": {
                "header": {
                    "title": "Pull Request Ready for Review",
                    "subtitle": f"Repository: {repo_name}",
                    "imageUrl": "https://avatars.githubusercontent.com/u/146738539?s=200&v=4",
                    "imageType": "CIRCLE"
                },
                "sections": [{
                    "widgets": [
                        {
                            "textParagraph": {
                                "text": f"<b>#{pr_number}</b> - {pr_title}"
                            }
                        },
                        {
                            "buttonList": {
                                "buttons": [{
                                    "text": "View Pull Request",
                                    "onClick": {
                                        "openLink": {
                                            "url": pr_url
                                        }
                                    }
                                }]
                            }
                        }
                    ]
                }]
            }
        }]
    }

    headers = {"Content-Type": "application/json; charset=UTF-8"}
    response = requests.post(webhook_url, json=message, headers=headers)

    print(f"Notification sent. Status: {response.status_code}")
    print("Response:", response.text)

def main():
    parser = argparse.ArgumentParser(description="Send PR ready-for-review notification to Google Chat.")
    parser.add_argument("--webhook-url", required=True, help="Google Chat webhook URL")
    parser.add_argument("--pr-number", required=True, help="Pull Request number")
    parser.add_argument("--pr-title", required=True, help="Pull Request title")
    parser.add_argument("--pr-url", required=True, help="Pull Request URL")
    parser.add_argument("--repo-name", required=True, help="Repository full name (owner/repo)")

    args = parser.parse_args()

    send_gchat_notification(
        webhook_url=args.webhook_url,
        pr_number=args.pr_number,
        pr_title=args.pr_title,
        pr_url=args.pr_url,
        repo_name=args.repo_name
    )

if __name__ == "__main__":
    main()
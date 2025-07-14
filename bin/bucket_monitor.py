#!/usr/bin/env python3
import subprocess
import json
import yaml
import requests
import sys
import os
from typing import Dict, List, Tuple

def get_rclone_profiles() -> List[str]:
    """Get list of configured rclone profiles"""
    try:
        result = subprocess.run(
            ['rclone', 'listremotes'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            print(f"Error getting rclone profiles: {result.stderr}")
            return []
        
        profiles = [line.strip().rstrip(':') for line in result.stdout.strip().split('\n') if line.strip()]
        print(f"DEBUG: Found rclone profiles: {profiles}")
        return profiles
        
    except subprocess.TimeoutExpired:
        print("Timeout while getting rclone profiles")
        return []
    except Exception as e:
        print(f"Error getting rclone profiles: {e}")
        return []

def get_bucket_count_for_profile(profile_name: str) -> int:
    """Get bucket count for a specific profile using rclone lsd"""
    try:
        result = subprocess.run(
            ['rclone', 'lsd', f'{profile_name}:'],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"Error running rclone for profile {profile_name}: {result.stderr}")
            return 0
        
        bucket_count = len([line for line in result.stdout.strip().split('\n') if line.strip()])
        print(f"Profile {profile_name}: {bucket_count} buckets")
        return bucket_count
        
    except subprocess.TimeoutExpired:
        print(f"Timeout while checking profile {profile_name}")
        return 0
    except Exception as e:
        print(f"Error checking profile {profile_name}: {e}")
        return 0

def get_all_bucket_counts(profiles: List[str]) -> Tuple[Dict[str, int], int]:
    """Get bucket counts for all profiles"""
    bucket_counts = {}
    total_buckets = 0
    
    for profile_name in profiles:
        count = get_bucket_count_for_profile(profile_name)
        bucket_counts[profile_name] = count
        total_buckets += count
    
    return bucket_counts, total_buckets

def send_notification(webhook_url: str, bucket_counts: Dict[str, int], total_buckets: int, 
                      threshold: int, git_run_url: str):
    """Send notification to Google Chat"""
    
    profile_details = []
    for profile, count in bucket_counts.items():
        profile_details.append(f"• {profile}: {count} buckets")
    
    profile_text = "\n".join(profile_details)
    
    max_buckets = 10000
    percentage = (total_buckets / max_buckets) * 100
    
    app_message = {
        'cardsV2': [{
            'cardId': 'bucketMonitorAlert',
            'card': {
                'header': {
                    'title': 'Bucket Count Alert',
                    'subtitle': f'Total: {total_buckets} buckets ({percentage:.1f}% of limit)',
                    'imageUrl': 'https://avatars.githubusercontent.com/u/146738539?s=200&v=4',
                    'imageType': 'CIRCLE'
                },
                'sections': [
                    {
                        "header": f'Threshold Exceeded: {total_buckets} >= {threshold}',
                        "collapsible": True,
                        "uncollapsibleWidgetsCount": 1,
                        "widgets": [
                            {
                                "textParagraph": {
                                    "text": f"<b>Bucket count by profile:</b>\n{profile_text}"
                                }
                            },
                            {
                                "textParagraph": {
                                    "text": (
                                        f"<b>Summary:</b>\n"
                                        f"• Total buckets: {total_buckets}\n"
                                        f"• Threshold: {threshold}\n"
                                        f"• Limit: {max_buckets}\n"
                                        f"• Usage: {percentage:.1f}%\n\n"
                                        f"<b>Action Required:</b> Cleanup must be run to reduce bucket count."
                                    )
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
    print(f"Notification sent. Status: {response.status_code}")

def main():
    if len(sys.argv) != 4:
        print("Usage: bucket_monitor.py <webhook_url> <profiles_file> <git_run_url>")
        sys.exit(1)

    webhook_url = sys.argv[1]
    profiles_file = sys.argv[2]
    git_run_url = sys.argv[3]

    threshold = int(10000 * 0.8)

    print(f"Bucket Monitor - Threshold: {threshold}")
    print(f"Loading profiles from: {profiles_file}")

    all_profiles = get_rclone_profiles()
    if not all_profiles:
        print("No profiles loaded. Exiting.")
        sys.exit(0)

    print(f"Found {len(all_profiles)} profiles to check")

    bucket_counts, _ = get_all_bucket_counts(all_profiles)
    
    print(f"\nBucket count summary:")
    for profile, count in bucket_counts.items():
        print(f"  {profile}: {count}")

    prod_profiles = ["br-ne1", "br-se1", "homologacao"]
    prod_total = sum(bucket_counts.get(p, 0) for p in prod_profiles)

    if prod_total >= threshold:
        print(f"\nALERT: br-ne1 + br-se1 + homologacao = {prod_total} >= {threshold}")
        counts = {p: bucket_counts.get(p, 0) for p in prod_profiles}
        send_notification(webhook_url, counts, prod_total, threshold, git_run_url)
    else:
        print(f"\nOK: br-ne1 + br-se1 + homologacao = {prod_total} is below threshold ({threshold})")

if __name__ == "__main__":
    main()

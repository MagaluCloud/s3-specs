import os, pytest, logging
from datetime import datetime, timedelta
from botocore.exceptions import ClientError, ParamValidationError
from s3_specs.docs.s3_helpers import run_example
# ---
# jupyter:
#   kernelspec:
#     name: s3-specs
#     display_name: S3 Specs
#   language_info:
#     name: python
# ---

# # Lifecycle Configuration (Object Lifecycle Management)
#
# S3 Lifecycle configuration allows bucket owners to manage objects throughout their lifecycle
# by automatically transitioning objects to different storage classes or expiring them.
#
# **Lifecycle Configuration** is defined at the bucket level and consists of one or more
# `Rule` blocks, each describing when and how objects should be processed based on their
# age, prefix, or other criteria.
#
# A typical use case for lifecycle management is:
# - Automatically delete log files after 30 days
# - Archive old documents to cheaper storage classes
# - Clean up temporary files or incomplete uploads
#
# Lifecycle rules help optimize storage costs by automatically managing object retention
# and storage class transitions without manual intervention.
#
# Each rule can specify:
# - **ID**: Optional identifier for the rule
# - **Prefix**: Filter objects by key prefix (empty = all objects)
# - **Status**: Whether the rule is Enabled or Disabled
# - **Expiration**: When objects should be deleted (Days or Date)
#
# **Important:** Unlike CORS, lifecycle rules directly affect object storage and deletion.
# Misconfigured rules can result in unintended data loss.

# + tags=["parameters"]
pytestmark = [pytest.mark.homologacao, pytest.mark.lifecycle]
config = os.getenv("CONFIG", "../params/br-ne1.yaml")


def is_client_error_with_code(obj, code: str) -> bool:
    """Check if the given object is a ClientError with the specified error code."""
    if not isinstance(obj, ClientError):
        return False
    actual_code = obj.response.get("Error", {}).get("Code")
    if code == "InvalidArgument":
        return actual_code in ["InvalidArgument", "MalformedXML"]
    return actual_code == code


@pytest.fixture
def base_lifecycle_rule():
    """Return a base dictionary of valid lifecycle rule arguments."""
    return {
        "ID": "test-rule",
        "Status": "Enabled",
        "Prefix": "logs/",
        "Expiration": {"Days": 30}
    }


def apply_and_get_lifecycle(s3_client, bucket_name, rules):
    """Apply lifecycle configuration to the bucket and retrieve the applied rules."""
    if not isinstance(rules, list):
        rules = [rules]
    
    lifecycle_config = {"Rules": rules}
    try:
        resp = s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration=lifecycle_config
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
    except ClientError as e:
        return e
    
    result = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
    return result.get("Rules", [])


def get_future_date(days_ahead=30):
    """Return a future date as ISO string."""
    future_date = datetime.utcnow() + timedelta(days=days_ahead)
    return future_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def get_past_date(days_ago=30):
    """Return a past date as ISO string."""
    past_date = datetime.utcnow() - timedelta(days=days_ago)
    return past_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")


# ===== Basic Tests =====


def test_simple_valid_config(s3_client, existing_bucket_name, base_lifecycle_rule):
    """Tests a basic valid lifecycle configuration."""
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, base_lifecycle_rule)
    assert not isinstance(result, ClientError)
    assert len(result) == 1
    
    rule = result[0]
    assert rule.get("ID") == "test-rule"
    assert rule.get("Status") == "Enabled"
    assert rule.get("Prefix") == "logs/"
    assert rule.get("Expiration", {}).get("Days") == 30


def test_minimal_rule_no_id(s3_client, existing_bucket_name):
    """Tests minimal rule without ID."""
    rule = {
        "Status": "Enabled",
        "Prefix": "",
        "Expiration": {"Days": 1}
    }
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, rule)
    assert not isinstance(result, ClientError)
    assert len(result) == 1
    
    returned_rule = result[0]
    assert returned_rule.get("Status") == "Enabled"
    assert returned_rule.get("Prefix") == ""
    assert returned_rule.get("Expiration", {}).get("Days") == 1


def test_disabled_rule(s3_client, existing_bucket_name, base_lifecycle_rule):
    """Tests disabled lifecycle rule."""
    base_lifecycle_rule["Status"] = "Disabled"
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, base_lifecycle_rule)
    assert not isinstance(result, ClientError)
    assert result[0].get("Status") == "Disabled"


def test_empty_prefix_all_objects(s3_client, existing_bucket_name, base_lifecycle_rule):
    """Tests empty prefix to apply to all objects."""
    base_lifecycle_rule["Prefix"] = ""
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, base_lifecycle_rule)
    assert not isinstance(result, ClientError)
    assert result[0].get("Prefix") == ""


# ===== Expiration Tests =====


def test_expiration_by_days(s3_client, existing_bucket_name, base_lifecycle_rule):
    """Tests expiration by number of days."""
    test_cases = [1, 30, 365, 1000]
    
    for days in test_cases:
        base_lifecycle_rule["Expiration"] = {"Days": days}
        base_lifecycle_rule["ID"] = f"test-days-{days}"
        result = apply_and_get_lifecycle(s3_client, existing_bucket_name, base_lifecycle_rule)
        assert not isinstance(result, ClientError)
        assert result[0].get("Expiration", {}).get("Days") == days


def test_expiration_by_date(s3_client, existing_bucket_name, base_lifecycle_rule):
    """Tests expiration by specific date."""
    future_date = get_future_date(60)
    base_lifecycle_rule["Expiration"] = {"Date": future_date}

    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, base_lifecycle_rule)
    assert not isinstance(result, ClientError)

    returned_date = result[0].get("Expiration", {}).get("Date")
    assert returned_date is not None

    if isinstance(future_date, datetime):
        future_date_str = future_date.strftime("%Y-%m-%d")
    else:
        future_date_str = str(future_date)[:10]

    if isinstance(returned_date, datetime):
        returned_date_str = returned_date.strftime("%Y-%m-%d")
    else:
        returned_date_str = str(returned_date)[:10]

    assert future_date_str == returned_date_str


def test_expiration_date_boundaries(s3_client, existing_bucket_name, base_lifecycle_rule):
    """Tests boundary dates for expiration."""
    # Test dates: tomorrow, far future
    test_dates = [
        get_future_date(1),      # Tomorrow
        get_future_date(3650),   # ~10 years
        "2030-12-31T23:59:59.000Z"  # Specific future date
    ]
    
    for date_str in test_dates:
        base_lifecycle_rule["Expiration"] = {"Date": date_str}
        base_lifecycle_rule["ID"] = f"test-date-{date_str[:4]}"
        result = apply_and_get_lifecycle(s3_client, existing_bucket_name, base_lifecycle_rule)
        assert not isinstance(result, ClientError)
        assert result[0].get("Expiration", {}).get("Date") is not None


# ===== Multiple Rules Tests =====


def test_multiple_rules_different_prefixes(s3_client, existing_bucket_name):
    """Tests multiple rules with different prefixes."""
    rules = [
        {
            "ID": "logs-cleanup",
            "Status": "Enabled",
            "Prefix": "logs/",
            "Expiration": {"Days": 30}
        },
        {
            "ID": "temp-cleanup", 
            "Status": "Enabled",
            "Prefix": "temp/",
            "Expiration": {"Days": 7}
        },
        {
            "ID": "archive-old",
            "Status": "Enabled",
            "Prefix": "archive/",
            "Expiration": {"Days": 2555}  # ~7 years
        }
    ]
    
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, rules)
    assert not isinstance(result, ClientError)
    assert len(result) == 3
    
    # Verify each rule
    rule_by_id = {rule["ID"]: rule for rule in result}
    assert rule_by_id["logs-cleanup"]["Expiration"]["Days"] == 30
    assert rule_by_id["temp-cleanup"]["Expiration"]["Days"] == 7
    assert rule_by_id["archive-old"]["Expiration"]["Days"] == 2555


def test_mixed_expiration_types(s3_client, existing_bucket_name):
    """Tests rules with mixed expiration types (Days and Date)."""
    rules = [
        {
            "ID": "days-rule",
            "Status": "Enabled", 
            "Prefix": "daily/",
            "Expiration": {"Days": 90}
        },
        {
            "ID": "date-rule",
            "Status": "Enabled",
            "Prefix": "scheduled/",
            "Expiration": {"Date": get_future_date(180)}
        }
    ]
    
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, rules)
    assert not isinstance(result, ClientError)
    assert len(result) == 2
    
    rule_by_id = {rule["ID"]: rule for rule in result}
    assert rule_by_id["days-rule"]["Expiration"]["Days"] == 90
    assert rule_by_id["date-rule"]["Expiration"]["Date"] is not None


# ===== Validation / Error Tests =====


def test_invalid_status_value(s3_client, existing_bucket_name, base_lifecycle_rule):
    """Tests invalid status value."""
    base_lifecycle_rule["Status"] = "InvalidStatus"
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, base_lifecycle_rule)
    assert is_client_error_with_code(result, "InvalidArgument")


def test_missing_required_fields(s3_client, existing_bucket_name):
    """Tests rules missing required fields."""
    invalid_rules = [
        # Missing Status
        {
            "ID": "no-status",
            "Prefix": "test/",
            "Expiration": {"Days": 30}
        },
        # Missing Prefix
        {
            "ID": "no-prefix",
            "Status": "Enabled",
            "Expiration": {"Days": 30}
        },
        # Missing Expiration
        {
            "ID": "no-expiration",
            "Status": "Enabled",
            "Prefix": "test/"
        }
    ]
    
    for rule in invalid_rules:
        with pytest.raises(ParamValidationError) as exc_info:
            apply_and_get_lifecycle(s3_client, existing_bucket_name, base_lifecycle_rule)

        assert "Invalid type for parameter" in str(exc_info.value)


def test_negative_days(s3_client, existing_bucket_name, base_lifecycle_rule):
    """Tests negative days value."""
    base_lifecycle_rule["Expiration"] = {"Days": -1}
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, base_lifecycle_rule)
    assert is_client_error_with_code(result, "InvalidArgument")


def test_zero_days(s3_client, existing_bucket_name, base_lifecycle_rule):
    """Tests zero days value."""
    base_lifecycle_rule["Expiration"] = {"Days": 0}
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, base_lifecycle_rule)
    assert not isinstance(result, ClientError)
    assert result[0]["Expiration"]["Days"] == 0


def test_very_large_days(s3_client, existing_bucket_name, base_lifecycle_rule):
    """Tests very large days value."""
    base_lifecycle_rule["Expiration"] = {"Days": 2147483647}  # Max int32
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, base_lifecycle_rule)
    assert not isinstance(result, ClientError)
    assert result[0]["Expiration"]["Days"] == 2147483647


def test_past_date(s3_client, existing_bucket_name, base_lifecycle_rule):
    """Tests expiration date in the past."""
    past_date = get_past_date(30)
    base_lifecycle_rule["Expiration"] = {"Date": past_date}
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, base_lifecycle_rule)
    # Past dates should be accepted (objects expire immediately)
    assert not isinstance(result, ClientError)

def test_both_days_and_date(s3_client, existing_bucket_name, base_lifecycle_rule):
    """Tests rule with both Days and Date (should error)."""
    base_lifecycle_rule["Expiration"] = {
        "Days": 30,
        "Date": get_future_date(30)
    }
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, base_lifecycle_rule)
    assert is_client_error_with_code(result, "InvalidArgument")


def test_empty_expiration(s3_client, existing_bucket_name, base_lifecycle_rule):
    """Tests empty expiration object."""
    base_lifecycle_rule["Expiration"] = {}
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, base_lifecycle_rule)
    assert is_client_error_with_code(result, "InvalidArgument")


def test_empty_rules_list(s3_client, existing_bucket_name):
    """Tests empty rules list."""
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, [])
    assert is_client_error_with_code(result, "InvalidArgument")


# ===== Prefix Tests =====


def test_various_prefix_patterns(s3_client, existing_bucket_name):
    """Tests various prefix patterns."""
    prefixes = [
        "",                    # Empty (all objects)
        "logs/",              # Simple directory
        "data/2024/",         # Nested directory
        "temp-files-",        # Prefix without slash
        "archive/important/", # Deep nesting
        "123-numeric-prefix", # Starting with number
        "UPPERCASE/",         # Uppercase
        "mixed-Case/Files/",  # Mixed case
    ]
    
    rules = []
    for i, prefix in enumerate(prefixes):
        rules.append({
            "ID": f"rule-{i}",
            "Status": "Enabled",
            "Prefix": prefix,
            "Expiration": {"Days": 30 + i}
        })
    
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, rules)
    assert not isinstance(result, ClientError)
    assert len(result) == len(prefixes)
    
    # Verify prefixes are preserved
    returned_prefixes = [rule["Prefix"] for rule in result]
    for original_prefix in prefixes:
        assert original_prefix in returned_prefixes


def test_special_characters_in_prefix(s3_client, existing_bucket_name, base_lifecycle_rule):
    """Tests special characters in prefix."""
    special_prefixes = [
        "logs-with-dashes/",
        "logs_with_underscores/",
        "logs.with.dots/",
        "logs with spaces/",
        "logs@with#special$/",
        "logs(with)parentheses/",
        "logs[with]brackets/",
    ]
    
    for prefix in special_prefixes:
        base_lifecycle_rule["Prefix"] = prefix
        base_lifecycle_rule["ID"] = f"test-{hash(prefix) % 1000}"
        result = apply_and_get_lifecycle(s3_client, existing_bucket_name, base_lifecycle_rule)
        assert not isinstance(result, ClientError)
        assert result[0]["Prefix"] == prefix


def test_unicode_in_prefix(s3_client, existing_bucket_name, base_lifecycle_rule):
    """Tests unicode characters in prefix."""
    unicode_prefixes = [
        "日本語/",              # Japanese
        "português/",          # Portuguese
        "русский/",            # Russian
        "العربية/",            # Arabic
        "测试/",               # Chinese
        "émojis🚀/",          # Emojis
    ]
    
    for prefix in unicode_prefixes:
        base_lifecycle_rule["Prefix"] = prefix
        base_lifecycle_rule["ID"] = f"unicode-{hash(prefix) % 1000}"
        result = apply_and_get_lifecycle(s3_client, existing_bucket_name, base_lifecycle_rule)
        assert not isinstance(result, ClientError)
        assert result[0]["Prefix"] == prefix


def test_very_long_prefix(s3_client, existing_bucket_name, base_lifecycle_rule):
    """Tests very long prefix."""
    long_prefix = "very/long/prefix/" + "segment/" * 50 + "final/"
    base_lifecycle_rule["Prefix"] = long_prefix
    
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, base_lifecycle_rule)
    assert not isinstance(result, ClientError)
    assert result[0]["Prefix"] == long_prefix


# ===== ID Tests =====

def test_various_id_formats(s3_client, existing_bucket_name):
    """Tests various ID formats."""
    ids = [
        "simple-id",
        "ID_with_underscores",
        "ID.with.dots",
        "ID with spaces",
        "123-numeric-start",
        "ID-with-MIXED-case",
        "very-long-id-" + "x" * 100,
        "unicode-id-测试-🚀",
    ]
    
    rules = []
    for i, rule_id in enumerate(ids):
        rules.append({
            "ID": rule_id,
            "Status": "Enabled",
            "Prefix": f"prefix{i}/",
            "Expiration": {"Days": 30}
        })
    
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, rules)
    assert not isinstance(result, ClientError)
    assert len(result) == len(ids)
    
    returned_ids = [rule["ID"] for rule in result]
    for original_id in ids:
        assert original_id in returned_ids


def test_empty_id(s3_client, existing_bucket_name, base_lifecycle_rule):
    """Tests empty ID."""
    base_lifecycle_rule["ID"] = ""
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, base_lifecycle_rule)
    # Empty ID might be treated as missing ID, which should be valid
    assert not isinstance(result, ClientError)


# ===== Configuration Management Tests =====


def test_lifecycle_configuration_replacement(s3_client, existing_bucket_name, base_lifecycle_rule):
    """Tests that new lifecycle configuration completely replaces the previous one."""
    # First configuration
    result1 = apply_and_get_lifecycle(s3_client, existing_bucket_name, base_lifecycle_rule)
    assert not isinstance(result1, ClientError)
    assert len(result1) == 1
    
    # Second different configuration
    new_rule = {
        "ID": "replacement-rule",
        "Status": "Enabled",
        "Prefix": "new-prefix/",
        "Expiration": {"Days": 90}
    }
    result2 = apply_and_get_lifecycle(s3_client, existing_bucket_name, new_rule)
    assert not isinstance(result2, ClientError)
    assert len(result2) == 1
    
    # Verify the old configuration was completely replaced
    assert result2[0]["ID"] == "replacement-rule"
    assert result2[0]["Prefix"] == "new-prefix/"
    assert result2[0]["Expiration"]["Days"] == 90


def test_get_nonexistent_lifecycle(s3_client, existing_bucket_name):
    """Tests getting lifecycle configuration when none exists."""
    # First, make sure no lifecycle config exists by trying to delete it
    try:
        s3_client.delete_bucket_lifecycle(Bucket=existing_bucket_name)
    except ClientError:
        pass  # It's OK if there was no config to delete
    
    # Now try to get the configuration
    with pytest.raises(ClientError) as exc_info:
        s3_client.get_bucket_lifecycle_configuration(Bucket=existing_bucket_name)
    
    assert exc_info.value.response["Error"]["Code"] == "NoSuchLifecycleConfiguration"


def test_delete_lifecycle_configuration(s3_client, existing_bucket_name, base_lifecycle_rule):
    """Tests deleting lifecycle configuration."""
    # First apply a configuration
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, base_lifecycle_rule)
    assert not isinstance(result, ClientError)
    
    # Delete the configuration
    resp = s3_client.delete_bucket_lifecycle(Bucket=existing_bucket_name)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204
    
    # Verify it's gone
    with pytest.raises(ClientError) as exc_info:
        s3_client.get_bucket_lifecycle_configuration(Bucket=existing_bucket_name)
    
    assert exc_info.value.response["Error"]["Code"] == "NoSuchLifecycleConfiguration"


# ===== Edge Cases and Stress Tests =====


def test_maximum_rules(s3_client, existing_bucket_name):
    """Tests configuration with many rules (approaching limits)."""
    # AWS allows up to 1000 rules, but we'll test with a reasonable number
    num_rules = 50
    rules = []
    
    for i in range(num_rules):
        rules.append({
            "ID": f"rule-{i:03d}",
            "Status": "Enabled" if i % 2 == 0 else "Disabled",
            "Prefix": f"prefix-{i}/",
            "Expiration": {"Days": (i % 365) + 1}
        })
    
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, rules)
    assert not isinstance(result, ClientError)
    assert len(result) == num_rules
    
    # Verify a few random rules
    rule_by_id = {rule["ID"]: rule for rule in result}
    assert rule_by_id["rule-000"]["Status"] == "Enabled"
    assert rule_by_id["rule-001"]["Status"] == "Disabled"
    assert rule_by_id["rule-010"]["Prefix"] == "prefix-10/"


def test_overlapping_prefixes(s3_client, existing_bucket_name):
    """Tests rules with overlapping prefixes."""
    rules = [
        {
            "ID": "broad-rule",
            "Status": "Enabled",
            "Prefix": "logs/",
            "Expiration": {"Days": 365}
        },
        {
            "ID": "specific-rule",
            "Status": "Enabled",
            "Prefix": "logs/application/",
            "Expiration": {"Days": 30}
        },
        {
            "ID": "very-specific-rule",
            "Status": "Enabled",
            "Prefix": "logs/application/debug/",
            "Expiration": {"Days": 7}
        }
    ]
    
    result = apply_and_get_lifecycle(s3_client, existing_bucket_name, rules)
    assert not isinstance(result, ClientError)
    assert len(result) == 3
    
    # All rules should be accepted (S3 handles precedence)
    rule_by_id = {rule["ID"]: rule for rule in result}
    assert "broad-rule" in rule_by_id
    assert "specific-rule" in rule_by_id
    assert "very-specific-rule" in rule_by_id
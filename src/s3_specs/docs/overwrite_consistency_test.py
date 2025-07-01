import pytest
import logging
from s3_specs.docs.utils.consistency import (
    overwrite_and_validate_reads
)


@pytest.mark.slow
@pytest.mark.consistency
@pytest.mark.parametrize("quantity", [10])
def test_overwrite_read_consistency(profile_name, available_buckets, quantity):
    object_key = "overwrite-test/arquivo_unico.txt"
    for bucket_type, bucket_name in available_buckets:
        logging.info(f"Validando consistência após sobrescritas no bucket: {bucket_type}")
        overwrite_and_validate_reads(
            bucket_name=bucket_name,
            object_key=object_key,
            quantity=quantity,
            profile_name=profile_name,
            bucket_type=bucket_type,
            read_repeats=3,
            delay=1
        )

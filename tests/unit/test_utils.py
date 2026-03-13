import pytest

from utils import retry, validate_job_args


def test_validate_job_args_valid():
    validate_job_args("my-bucket-123", "dataset", "results")


def test_validate_job_args_empty_bucket():
    with pytest.raises(ValueError, match="bucket must be non-empty"):
        validate_job_args("", "dataset", "results")


def test_validate_job_args_invalid_bucket_format():
    with pytest.raises(ValueError, match="valid S3 bucket name"):
        validate_job_args("Invalid_Bucket", "dataset", "results")


def test_validate_job_args_empty_source_prefix():
    with pytest.raises(ValueError, match="source_prefix must be non-empty"):
        validate_job_args("my-bucket", "", "results")


def test_validate_job_args_empty_output_prefix():
    with pytest.raises(ValueError, match="output_prefix must be non-empty"):
        validate_job_args("my-bucket", "dataset", "")


def test_retry_succeeds_on_second_attempt():
    call_count = 0

    def flaky_function():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise ValueError("Temporary failure")
        return "success"

    decorated = retry(max_attempts=3, delay_seconds=0)(flaky_function)
    result = decorated()
    assert result == "success"
    assert call_count == 2


def test_retry_raises_after_max_attempts():
    @retry(max_attempts=2, delay_seconds=0, exceptions=(ValueError,))
    def always_fails():
        raise ValueError("Always fails")

    with pytest.raises(ValueError):
        always_fails()

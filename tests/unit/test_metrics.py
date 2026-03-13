from metrics import JobMetrics


def test_job_metrics_initial_state():
    m = JobMetrics("test-job", correlation_id="test-job-abc123")
    assert m.data["job_name"] == "test-job"
    assert m.data["status"] == "running"
    assert m.data["input_row_count"] == 0


def test_job_metrics_set_input_output():
    m = JobMetrics("test-job", correlation_id="test-job-abc123")
    m.set_input_count(100)
    m.set_output_count(80)
    assert m.data["input_row_count"] == 100
    assert m.data["output_row_count"] == 80
    assert m.data["dropped_rows"] == 20
    assert m.data["drop_percentage"] == 20.0


def test_job_metrics_mark_success():
    m = JobMetrics("test-job", correlation_id="test-job-abc123")
    m.mark_success()
    assert m.data["status"] == "success"
    assert m.data["duration_seconds"] >= 0


def test_job_metrics_mark_failure():
    m = JobMetrics("test-job", correlation_id="test-job-abc123")
    m.mark_failure("test error")
    assert m.data["status"] == "failed"
    assert m.data["error"] == "test error"

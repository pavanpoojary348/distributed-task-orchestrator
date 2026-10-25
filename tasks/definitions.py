"""Sample task definitions for the Distributed Task Orchestrator.

These functions represent background jobs that workers can execute, including
normal jobs, fast jobs, slow jobs, and flaky jobs that fail intentionally.
"""

import time
import random
import os


def resize_image(payload: dict) -> dict:
    """Simulates resizing an image to thumbnail dimensions."""
    raw_filename = str(payload.get("filename", "sample.png"))
    filename = os.path.basename(raw_filename) or "sample.png"
    width = int(payload.get("width", 300))
    height = int(payload.get("height", 300))
    # Simulate processing time with bounds check
    delay = max(0.0, min(float(payload.get("delay", 0.5)), 10.0))
    time.sleep(delay)
    return {
        "status": "success",
        "file": filename,
        "dimensions": f"{width}x{height}",
        "bytes_processed": width * height * 3
    }


def generate_pdf_report(payload: dict) -> dict:
    """Simulates compiling and rendering a multi-page PDF document."""
    report_title = payload.get("title", "Monthly Analytics Summary")
    pages = payload.get("pages", 12)
    # Simulate processing time with bounds check
    delay = max(0.0, min(float(payload.get("delay", 0.8)), 10.0))
    time.sleep(delay)
    return {
        "status": "success",
        "title": report_title,
        "pages": pages,
        "format": "PDF-v1.7"
    }


def flaky_task(payload: dict) -> dict:
    """Simulates an unreliable job (e.g., third-party API timeout) to test retries and DLQ."""
    failure_rate = max(0.0, min(float(payload.get("failure_rate", 0.6)), 1.0))
    time.sleep(0.3)
    if random.random() < failure_rate:
        raise RuntimeError("Transient upstream connection timeout: remote server unreachable")
    return {
        "status": "success",
        "message": "Flaky task succeeded after clearing network hurdle"
    }


def poison_pill_task(payload: dict) -> dict:
    """Always fails; guaranteed to exhaust retries and get routed to the Dead Letter Queue."""
    time.sleep(0.2)
    raise ValueError("Unrecoverable data format error: cannot parse corrupted payload")


# Registry mapping task names to callable Python functions
TASK_REGISTRY = {
    "resize_image": resize_image,
    "generate_pdf_report": generate_pdf_report,
    "flaky_task": flaky_task,
    "poison_pill": poison_pill_task,
}

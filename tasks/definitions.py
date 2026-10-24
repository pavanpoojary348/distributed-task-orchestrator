"Sample background tasks for testing concurrency and failures."
def resize_image(payload: dict):
    return {status: success, file: payload.get(filename)}

def generate_pdf_report(payload: dict):
    return {status: success, pages: 12}

def flaky_task(payload: dict):
    "Simulates a task that fails randomly to test DLQ & retries."
    import random
    if random.random() < 0.7:
        raise RuntimeError(Transient database timeout)
    return {status: success}

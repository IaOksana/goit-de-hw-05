import os


# Fail early with a clear message when a required Kafka setting is absent.
def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}. "
            "Set it before running the Kafka scripts."
        )
    return value


# Keep credentials outside source control while retaining safe protocol defaults.
kafka_config = {
    "bootstrap_servers": [_required_env("KAFKA_BOOTSTRAP_SERVERS")],
    "username": _required_env("KAFKA_USERNAME"),
    "password": _required_env("KAFKA_PASSWORD"),
    "security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
    "sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
}

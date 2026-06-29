from importlib.resources import files
from typing import Optional

import grpc


def proto_path() -> str:
    return str(files(__package__).joinpath("proto/anvil.proto"))


def insecure_channel(endpoint: str) -> grpc.Channel:
    return grpc.insecure_channel(endpoint)


def secure_channel(endpoint: str, root_certificates: Optional[bytes] = None) -> grpc.Channel:
    return grpc.secure_channel(endpoint, grpc.ssl_channel_credentials(root_certificates))


def bearer_metadata(token: str) -> tuple[tuple[str, str], ...]:
    return (("authorization", f"Bearer {token}"),)


__all__ = ["bearer_metadata", "insecure_channel", "proto_path", "secure_channel"]

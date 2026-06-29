import os
import tempfile
import unittest

import grpc

from anvil_storage_client import bearer_metadata, insecure_channel, proto_path, secure_channel


class ClientPackageTests(unittest.TestCase):
    def test_proto_path_points_to_packaged_proto(self):
        path = proto_path()
        self.assertTrue(path.endswith("proto/anvil.proto"))
        self.assertTrue(os.path.exists(path))

    def test_auth_metadata_uses_bearer_header(self):
        self.assertEqual(bearer_metadata("token-123"), (("authorization", "Bearer token-123"),))

    def test_channel_helpers_return_grpc_channels(self):
        insecure = insecure_channel("localhost:50051")
        self.assertIsInstance(insecure, grpc.Channel)

        with tempfile.NamedTemporaryFile() as _unused:
            secure = secure_channel("localhost:50051")
            self.assertIsInstance(secure, grpc.Channel)


if __name__ == "__main__":
    unittest.main()

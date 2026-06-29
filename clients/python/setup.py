from pathlib import Path
from setuptools import setup
from setuptools.command.build_py import build_py as _build_py


class build_py(_build_py):
    def run(self):
        from grpc_tools import protoc

        root = Path(__file__).parent.resolve()
        proto_dir = root / "src" / "anvil_storage_client" / "proto"
        out_dir = root / "src" / "anvil_storage_client"
        result = protoc.main([
            "grpc_tools.protoc",
            f"-I{proto_dir}",
            f"--python_out={out_dir}",
            f"--grpc_python_out={out_dir}",
            str(proto_dir / "anvil.proto"),
        ])
        if result != 0:
            raise RuntimeError("failed to generate Anvil Python gRPC bindings")
        grpc_file = out_dir / "anvil_pb2_grpc.py"
        if grpc_file.exists():
            grpc_file.write_text(
                grpc_file.read_text().replace(
                    "import anvil_pb2 as anvil__pb2",
                    "from . import anvil_pb2 as anvil__pb2",
                )
            )
        super().run()


setup(cmdclass={"build_py": build_py})

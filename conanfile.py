from pathlib import Path

from conan import ConanFile
from conan.tools.cmake import cmake_layout


def _read_version() -> str:
    version_file = Path(__file__).parent / "VERSION"
    if version_file.exists():
        return version_file.read_text().strip()
    return "0.1.0"


class EvmCppListenerConan(ConanFile):
    name = "evm_cpp_listener"
    version = _read_version()
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeDeps", "CMakeToolchain"

    def requirements(self):
        self.requires("boost/1.90.0")
        self.requires("openssl/3.5.9")
        self.requires("simdjson/4.6.3")
        self.requires("ethash/1.0.1")
        self.requires("redis-plus-plus/1.3.15")
        self.requires("tomlplusplus/3.4.0")
        self.requires("spdlog/1.17.0")
        self.requires("prometheus-cpp/1.3.0")
        self.requires("catch2/3.9.1")

    # Compiled Boost libs we need; Asio/Beast are header-only. Add more here only when code needs them.
    _BOOST_COMPILED = (
        "atomic", "charconv", "chrono", "cobalt", "container", "context", "contract", "coroutine",
        "date_time", "exception", "fiber", "filesystem", "graph", "graph_parallel", "iostreams",
        "json", "locale", "log", "math", "mpi", "nowide", "process", "program_options", "python",
        "random", "regex", "serialization", "stacktrace", "system", "test", "thread", "timer",
        "type_erasure", "url", "wave",
    )
    _BOOST_KEEP = ("json", "container", "system", "charconv")

    def configure(self):
        self.options["redis-plus-plus"].with_tls = True
        self.options["hiredis"].with_ssl = True  # required by redis-plus-plus with_tls
        self.options["prometheus-cpp"].with_pull = False
        for lib in self._BOOST_COMPILED:
            if lib not in self._BOOST_KEEP:
                setattr(self.options["boost"], f"without_{lib}", True)

    def layout(self):
        cmake_layout(self)

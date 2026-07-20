# --------------------------------------------------------------------
# TestInfra checks for the Tea Ubuntu 22.04 build image.
# --------------------------------------------------------------------

def test_installed_packages(host):
    packages = [
        "clang-15",
        "gcc-13",
        "g++-13",
        "git",
        "libclang-15-dev",
        "libcurl4-openssl-dev",
        "libipc-run-perl",
        "libprotobuf-dev",
        "libtool",
        "libxslt1-dev",
        "llvm-15",
        "llvm-15-dev",
        "pkg-config",
        "protobuf-compiler",
        "python3-pip",
        "redis-server",
    ]
    for package in packages:
        assert host.package(package).is_installed


def test_user_gpadmin_exists(host):
    user = host.user("gpadmin")
    assert user.exists
    assert "gpadmin" in user.groups or "root" in user.groups


def test_cmake_version(host):
    result = host.run("cmake --version")
    assert result.rc == 0
    assert "cmake version 3." in result.stdout
    # Tea requires CMake >= 3.25
    version = result.stdout.splitlines()[0].split()[-1]
    major, minor = (int(x) for x in version.split(".")[:2])
    assert (major, minor) >= (3, 25)


def test_compilers(host):
    assert host.run("gcc-13 --version").rc == 0
    assert host.run("g++-13 --version").rc == 0
    assert host.file("/usr/bin/clang-15").exists


def test_deps_prefix(host):
    deps = host.file("/home/gpadmin/local")
    assert deps.exists
    assert deps.is_directory
    assert host.file("/home/gpadmin/local/lib").exists
    assert host.file("/home/gpadmin/local/include").exists


def test_arrow_installed(host):
    assert host.file("/home/gpadmin/local/include/arrow/api.h").exists
    assert host.run(
        "test -n \"$(find /home/gpadmin/local -name '*arrow*' | head -n 1)\""
    ).rc == 0


def test_grpc_installed(host):
    assert host.file("/home/gpadmin/local/include/grpc/grpc.h").exists


def test_environment_variables(host):
    env = host.environment()
    assert env.get("DEPS_PREFIX") == "/home/gpadmin/local"
    assert env.get("CMAKE_PREFIX_PATH") == "/home/gpadmin/local"
    assert env.get("Greenplum_ROOT") == "/opt/greenplum-db-6"


def test_init_system_script(host):
    script = host.file("/tmp/init_system.sh")
    assert script.exists
    assert script.mode == 0o755 or script.mode == 0o777


def test_locale_generated(host):
    locale = host.run("locale -a | grep -i '^en_US\\.utf8$'")
    assert locale.rc == 0

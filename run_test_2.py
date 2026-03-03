import os
import sys
import pytest
import oracledb
import git
import hashlib
import warnings

warnings.filterwarnings("ignore")

try:
    import tomllib
except ImportError:
    import tomli as tomllib

CONFIG_FILE = "config.toml"


# ---------- CONFIG ----------

def load_config():
    if not os.path.exists(CONFIG_FILE):
        return {}
    with open(CONFIG_FILE, "rb") as f:
        return tomllib.load(f)


def get_db_credentials(config):
    db_user = os.environ.get('DB_USER')
    db_pass = os.environ.get('DB_PASS')
    dsn = os.environ.get('DB_DSN')

    if not db_user:
        db_user = config.get("database", {}).get("db_user")
    if not db_pass:
        db_pass = config.get("database", {}).get("db_password")
    if not dsn:
        dsn = config.get("database", {}).get("default_dsn")

    if not db_user or not db_pass or not dsn:
        pytest.exit("DB credentials not found", returncode=1)

    return db_user, db_pass, dsn


def checkout_and_pull_branch(repo, branch_name):
    git_user = os.environ.get('GIT_USER')
    git_pass = os.environ.get('GIT_PASS')

    if not git_user or not git_pass:
        pytest.exit("GIT credentials not found", returncode=1)

    auth_url = f"https://{git_user}:{git_pass}@git.moscow.alfaintra.net/scm/bialm_ft/bialm_ft_auto.git"
    repo.remotes.origin.config_writer.set("url", auth_url)

    repo.remotes.origin.fetch()
    repo.git.checkout(branch_name)
    repo.git.pull('origin', branch_name)


# ---------- PYTEST HOOK ----------

def pytest_addoption(parser):
    parser.addoption("--branch", action="store", required=True)


@pytest.fixture(scope="session")
def branch(request):
    return request.config.getoption("--branch")


@pytest.fixture(scope="session")
def config():
    return load_config()


@pytest.fixture(scope="session")
def db_credentials(config):
    return get_db_credentials(config)


@pytest.fixture(scope="session", autouse=True)
def prepare_repo(branch):
    repo = git.Repo(".")
    checkout_and_pull_branch(repo, branch)


# ---------- TEST GENERATION ----------

def pytest_generate_tests(metafunc):
    if "sql_file" in metafunc.fixturenames:
        config = load_config()
        tests_dir = config["tests"]["tests_directory"]

        sql_files = [
            os.path.join(tests_dir, f)
            for f in os.listdir(tests_dir)
            if f.endswith(".sql")
        ]

        metafunc.parametrize("sql_file", sql_files)


# ---------- TEST ----------

@pytest.mark.usefixtures("prepare_repo")
def test_sql_duplicates(sql_file, db_credentials, branch):
    import allure

    db_user, db_pass, dsn = db_credentials
    file_name = os.path.basename(sql_file)

    allure.dynamic.title(file_name)
    allure.dynamic.label("branch", branch)
    allure.dynamic.feature("SQL Duplicate Checks")
    allure.dynamic.story(file_name)

    test_case_id = hashlib.md5(file_name.encode()).hexdigest()
    allure.dynamic.id(test_case_id)

    try:
        conn = oracledb.connect(user=db_user, password=db_pass, dsn=dsn)
        cursor = conn.cursor()

        sql = open(sql_file, 'r', encoding='utf-8').read().rstrip(' \n;')

        with allure.step("Execute SQL"):
            cursor.execute(sql)
            rows = cursor.fetchall()

        if rows:
            details = "\n".join(str(row) for row in rows)

            allure.attach(
                details,
                name="Duplicates found",
                attachment_type=allure.attachment_type.TEXT
            )

            pytest.fail(f"Найдено {len(rows)} наборов дубликатов")

        conn.close()

    except oracledb.Error as e:
        allure.attach(
            str(e),
            name="DB Error",
            attachment_type=allure.attachment_type.TEXT
        )
        pytest.fail("Ошибка выполнения SQL")

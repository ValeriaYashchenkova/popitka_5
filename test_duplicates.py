import pytest
import oracledb
import os
from allure_commons.types import LabelType

# Параметры из Jenkins (передаются через env)
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')
DB_DSN = os.environ.get('DB_DSN', 'BIDBTST1:1521/DB12TST2')

# Путь к папке с тестами (можно сделать параметром)
TESTS_DIR = "tests"

# Фикстура для подключения к БД
@pytest.fixture(scope="session")
def db_connection():
    if not DB_USER or not DB_PASS:
        pytest.fail("DB_USER или DB_PASS не указаны в параметрах билда")

    try:
        conn = oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)
        yield conn
        conn.close()
    except oracledb.Error as e:
        pytest.fail(f"Ошибка подключения к БД: {e}")

# Динамически генерируем тесты для каждого .sql-файла
def pytest_generate_tests(metafunc):
    sql_files = [f for f in os.listdir(TESTS_DIR) if f.endswith('.sql')]
    metafunc.parametrize("sql_file", sql_files)

@pytest.mark.parametrize("sql_file", [])
def test_duplicate_check(sql_file, db_connection, request):
    sql_path = os.path.join(TESTS_DIR, sql_file)

    # Имя теста для Allure (красивое отображение)
    test_name = sql_file.replace("check_duplicates_", "").replace(".sql", "")
    allure.dynamic.title(f"Проверка дубликатов: {test_name}")
    allure.dynamic.description(f"SQL-файл: {sql_file}\nDSN: {DB_DSN}")

    # Добавляем лейблы для группировки в TestOps
    allure.dynamic.label(LabelType.SUITE, "Duplicate Checks")
    allure.dynamic.label(LabelType.FEATURE, test_name)
    allure.dynamic.label(LabelType.TAG, "sql-duplicates")
    allure.dynamic.label(LabelType.TAG, f"branch:{request.config.getoption('--branch')}")

    try:
        cursor = db_connection.cursor()
        sql = open(sql_path, 'r', encoding='utf-8').read().rstrip(' \n;')
        cursor.execute(sql)
        rows = cursor.fetchall()

        if rows:
            details = "\n".join(str(row) for row in rows)
            allure.attach(details, name="Найденные дубликаты", attachment_type="text/plain")
            pytest.fail(f"Найдено {len(rows)} наборов дубликатов")
        else:
            print(f"OK — дубликатов не найдено в {sql_file}")

    except oracledb.Error as e:
        allure.attach(str(e), name="Ошибка выполнения SQL", attachment_type="text/plain")
        pytest.fail(f"Ошибка выполнения: {e}")

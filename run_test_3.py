# run_test.py — теперь это pytest-модуль
import os
import pytest
import allure
import oracledb
import git
from glob import glob

# Загрузка конфига (можно оставить)
def load_config():
    import tomli
    with open("config.toml", "rb") as f:
        return tomli.load(f)

config = load_config()
DB_DSN = config["database"]["default_dsn"]

# Подключение к БД (логин/пароль из параметров Jenkins)
@pytest.fixture(scope="session")
def db_connection():
    db_user = os.environ.get('DB_USER')
    db_pass = os.environ.get('DB_PASS')
    
    if not db_user or not db_pass:
        pytest.fail("DB_USER или DB_PASS не установлены в параметрах билда")
    
    conn = oracledb.connect(user=db_user, password=db_pass, dsn=DB_DSN)
    yield conn
    conn.close()

# Фикстура для списка SQL-файлов
@pytest.fixture(scope="session")
def sql_files():
    tests_dir = config["tests"]["tests_directory"]
    return glob(os.path.join(tests_dir, "*.sql"))

# Один тест на каждый .sql-файл
@pytest.mark.parametrize("sql_file", sql_files(), ids=lambda x: os.path.basename(x))
def test_duplicate_check(sql_file, db_connection):
    file_name = os.path.basename(sql_file)
    
    @allure.title(f"Проверка дубликатов: {file_name}")
    @allure.description(f"Выполняем SQL-запрос из файла {file_name} и проверяем отсутствие дубликатов")
    @allure.severity(allure.severity_level.NORMAL)
    @allure.tag("duplicates", "sql", "database")
    @allure.label("owner", "Валерия Ященкова")
    def run_test():
        with allure.step(f"Выполнение SQL-запроса из {file_name}"):
            cursor = db_connection.cursor()
            sql = open(sql_file, 'r', encoding='utf-8').read().rstrip(' \n;')
            cursor.execute(sql)
            rows = cursor.fetchall()
            
            if rows:
                allure.attach(
                    "\n".join(str(row) for row in rows),
                    name="Найденные дубликаты",
                    attachment_type=allure.attachment_type.TEXT
                )
                pytest.fail(f"Найдено {len(rows)} наборов дубликатов")
            else:
                allure.attach("Дубликатов не найдено", name="Результат", attachment_type=allure.attachment_type.TEXT)
    
    run_test()

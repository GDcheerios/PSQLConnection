from unittest import TestCase
from PSQLConnector.connector import PSQLConnection as db


class TestPSQLConnection(TestCase):
    def test_connect(self):
        db.connect(
            "gdcheerios",
            "1234",
            "127.0.0.1",
            "local"
        )
        db.execute("CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, name TEXT);")
        db.execute("INSERT INTO test_table (name) VALUES ('Test Data');")

    def test_execute(self):
        db.execute("INSERT INTO test_table (name) VALUES ('Another Test');")
        result = db.fetch_all("SELECT COUNT(*) FROM test_table;")
        print(result)
        self.assertEqual(result[0][0], 2)

    def test_fetch_all(self):
        result = db.fetch_all("SELECT * FROM test_table;")
        print(result)
        self.assertEqual(type(result), list)

    def test_fetch_one(self):
        result = db.fetch_one("SELECT * FROM test_table;")
        print(result)
        self.assertEqual(type(result), tuple)

    def test_fetch_all_to_dict(self):
        result = db.fetch_all_to_dict("SELECT * FROM test_table;")
        print(result)
        self.assertEqual(type(result), list)
        self.assertEqual(len(result), 2)

    def test_fetch_to_dict(self):
        result = db.fetch_to_dict("SELECT * FROM test_table;")
        print(result)
        self.assertEqual(type(result), dict)
    
    def test_now(self):
        result = db.now()
        self.assertIsNotNone(result)

    @classmethod
    def tearDownClass(cls):
        print("Running cleanup...")
        db.execute("DROP TABLE IF EXISTS test_table;")
        db.end()

def test_path_maker():
    from lib.path.path_helper import create_csv_path
    create_csv_path(__file__)

if __name__ == "__main__":
    test_path_maker()
import pytest
import requests

def test_connection(url):
    response = requests.get(url)
    assert response.status_code == 200, f"Failed to connect to {url}. Status code: {response.status_code}"

def test_files_exits(url,files):
    response = requests.get(url)
    assert response.status_code == 200, f"Failed to connect to {url}. Status code: {response.status_code}"

    for file_name in files:
        if file_name not in response.text:
            pytest.fail(f"File {file_name} does not exist at {url}")

if __name__ == "__main__":
    pytest.main()
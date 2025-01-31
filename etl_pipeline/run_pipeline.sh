pytest -v integration_tests.py
python3 extractor.py
pytest -v transformer_tests.py
python3 pipeline.py
pytest -v master_tests.py
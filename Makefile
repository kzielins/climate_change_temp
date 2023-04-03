init:
    pip install -r requirements.txt

build:
    py -m build

test:
    python global_land_temperature_countrystate_tests.py
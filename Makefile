init:
    pip install -r requirements.txt

build:
    py -m build

test:
    py.test tests
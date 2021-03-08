#!/bin/bash

python3 -m pip install --upgrade build twine
python3 -m build
ls dist/
python3 -m twine upload 'dist/*'


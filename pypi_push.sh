# first bump up package.json manually, commit and tag
rm superset/assets/dist/*
cd ../..
python setup.py sdist upload


cd service-a/
python -m  venv env
source env/bin/activate
pip install -r requirements.txt
cd datagen
python ingestScript.py
deactivate
cd ../..
cd service-b/
python -m venv env
source env/bin/activate
pip install -r requirements.txt
deactivate
cd ..
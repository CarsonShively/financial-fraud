# financial-fraud
fraud detection

python3 -m venv .venv

source .venv/bin/activate

redis-cli -p 6380 -n 1 EXISTS fraud:features:CURRENT
redis-server --bind 127.0.0.1 --port 6380 --save "" --appendonly no --daemonize yes
redis-cli -h 127.0.0.1 -p 6380 ping
redis-cli -h 127.0.0.1 -p 6380 shutdown

python3 -m venv .venv-serving
source .venv-serving/bin/activate

python -m streamlit run app/demo.py
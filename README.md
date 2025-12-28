# financial-fraud
fraud detection

python3 -m venv .venv

source .venv/bin/activate

redis-server --bind 127.0.0.1 --port 6379 --save "" --appendonly no --daemonize yes
redis-cli -h 127.0.0.1 -p 6379 ping
redis-cli -h 127.0.0.1 -p 6379 shutdown

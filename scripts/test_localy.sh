docker build -t test_1 -f Dockerfile .
docker run -it test_1:latest bash scripts/integration_tests.sh
up:
	DHIS2_IMAGE=$${DHIS2_IMAGE:-dhis2/core:40.8.0} \
	DHIS2_DB_DUMP_URL=$${DHIS2_DB_DUMP_URL:-https://databases.dhis2.org/sierra-leone/2.40/dhis2-db-sierra-leone.sql.gz} \
	docker compose -f docker-compose.dhis2.yml up -d

build-tests:
	docker build -t ohx-tests -f docker/Dockerfile.tests .

test: build-tests
	docker run --rm --network host --env-file .env.example -v "$$PWD":/workspace ohx-tests

down:
	docker compose -f docker-compose.dhis2.yml down
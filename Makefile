SQLC_VERSION := v1.31.1

.PHONY: sqlc-generate sqlc-check

sqlc-generate:
	GOWORK=off go run github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION) generate

sqlc-check:
	GOWORK=off go run github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION) generate
	git diff --exit-code -- store/postgres/internal/db

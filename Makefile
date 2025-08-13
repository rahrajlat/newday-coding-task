DBT_PROJECT_DIR := dbt/dbt_newday_coding_assignment
PROFILES_DIR := $(DBT_PROJECT_DIR)

lint-dbt: 
	@echo "Linting dbt project in $(DBT_PROJECT_DIR)"
	@cd $(DBT_PROJECT_DIR) && DBT_PROFILES_DIR=$(PROFILES_DIR) sqlfluff lint --templater dbt models/*
	
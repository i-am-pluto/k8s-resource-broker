.PHONY: generate-models

generate-models:
	datamodel-codegen \
		--input openapi/models.yaml \
		--output src/generated/models.py \
		--output-model-type=pydantic_v2.BaseModel \
		--use-annotated \
		--use-double-quotes \
		--field-constraints
	sed -i '' 's/= "recommendation"/= Mode.recommendation/' src/generated/models.py

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.substrait as substrait

# 1. Define schema
schema = pa.schema([
    ("id", pa.int32()),
    ("name", pa.string()),
    ("salary", pa.int32())
])

# 2. Define a filter expression: salary > 50000
expr = pc.field("salary") > 50000

# 3. Serialize the expression (binds it to schema)
bound_expr = substrait.serialize_expressions(
    exprs=[expr],
    names=["filter_expr"],
    schema=schema
)

# 4. Serialize schema (to help with physical execution if needed)
substrait_schema = substrait.serialize_schema(schema)
print(bound_expr.tobytes())
# 5. Save to file for testing
with open("filter_plan.substrait", "wb") as f:
    f.write(bound_expr.tobytes())

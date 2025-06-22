import casbin
import sqlalchemy_adapter

# Step 1: Load existing policies from CSV
csv_enforcer = casbin.Enforcer("authorisation/model.conf", "authorisation/policy.csv")

# Step 2: Setup the database adapter (SQLite used here)
adapter = sqlalchemy_adapter.Adapter("sqlite:///idp.db")

# Step 3: Initialize DB enforcer
db_enforcer = casbin.Enforcer("authorisation/model.conf", adapter)

# Step 4: Copy policy rules
for rule in csv_enforcer.get_policy():
    db_enforcer.add_policy(*rule)

# Step 5: Copy group (role-user) assignments
for grouping in csv_enforcer.get_grouping_policy():
    db_enforcer.add_grouping_policy(*grouping)

# Step 6: Save policies to DB
db_enforcer.save_policy()

print("✅ Casbin policies migrated to database successfully.")

from sqlalchemy import create_engine
import pandas as pd

# PostgreSQL connection
DATABASE_URL = "postgresql://admin:admin@localhost:5432/nyc_taxi"

# Create engine
engine = create_engine(DATABASE_URL)

# Test query
query = "SELECT version();"

# Execute
df = pd.read_sql(query, engine)

print(df)
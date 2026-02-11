import duckdb as ddb
import ibis
from ibis.backends import duckdb as ibis_duckdb
import pandas as pd

url = "https://raw.githubusercontent.com/Opensourcefordatascience/Data-sets/master/blood_pressure.csv"
df = pd.read_csv(url)
con = ddb.connect()
con.register("blood_pressure", df)

be = ibis_duckdb.Backend(con)

t = ibis.table(
    name="blood_pressure",
    schema={
        "patient": "string",
        "sex": "string",
        "agegrp": "string",
        "bp_before": "int64",
        "bp_after": "int64",
    }
)

new_patient = "Patient" + t["patient"].cast("string")
avg_bp_before = t["bp_before"].mean()
avg_bp_after = t["bp_after"].mean()

difference_between_avg_bp = avg_bp_before - avg_bp_after

if __name__ == "__main__":
    print(df.head())

    print(avg_bp_after)
    # build ibis expression and compile to sql
    print(be.compile(avg_bp_after, pretty=True))

    print(
        be.compile(
            t.select(new_patient=new_patient, bp_before=t["bp_before"], bp_after=t["bp_after"]).group_by("new_patient").aggregate([avg_bp_before, avg_bp_after])
        )
    )

    print(
        be.compile(
            t.group_by("patient").agg(avg_bp_before=avg_bp_before, avg_bp_after=avg_bp_after)
        )
    )

    print(
        be.compile(
            t[["patient", "bp_before", "bp_after"]].group_by("patient").agg(avg_bp_before, avg_bp_after)
        )
    )

    query = be.compile(
        t[["patient", "bp_before", "bp_after"]].group_by("patient").agg(avg_bp_before, avg_bp_after)
    )
    print(con.execute(query).fetchall())

    query = be.compile(
        t[["patient", "bp_before", "bp_after"]].group_by("patient").agg(diff_in_avg_bp=difference_between_avg_bp).order_by("diff_in_avg_bp").limit(2)
    )

    print(query)

    print(con.execute(query).fetchall())


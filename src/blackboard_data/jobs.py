import dagster as dg


main_job = dg.define_asset_job(
    name="main_job", selection=dg.AssetSelection.groups("EXTRACT", "LOAD", "TRANSFORM")
)

<!-- We decided to Use Jersey City(JC) data along with the NYC dataset. by partitioned by the region while storing the silver bucket for quicker inference in paraquet(column storage form) -->

Bug Flag/Flaw in bronze to silver (Extraction): <!-- region is inferred from filename substring rather than passed explicitly — trades interface simplicity for coupling to naming convention; a truly unknown region silently falls through to 'nyc' rather than erroring, which is a known limitation." That kind of self-aware trade-off note is exactly the habit we talked about earlier. -->

citibike_dbt: Building the dim_station:
<!-- Station coordinates can drift slightly due to GPS jitter. We need to pick one definitive coordinate per station. DuckDB's arg_max function is perfect here: it lets us select the name and coordinates from the most recent time a station was used. -->
<!-- arg_max() is not "adding the latest station name." It's choosing the station name (and coordinates) from the most recent observation for that station ID. -->


#Imp logic mistake, we didn't how the silver need to be loaded, like the date wise

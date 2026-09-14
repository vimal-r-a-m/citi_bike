import duckdb
import pytest

from scripts.transform import clean_trips

@pytest.fixture
def con():
    return duckdb.connect(':memory:')

@pytest.fixture
def create_csv(tmp_path):
    def _create_csv(filename, header, rows):
        file_path = tmp_path / filename
        file_path.write_text(header + "\n" + "\n".join(rows))
        return str(file_path)
    return _create_csv

BASE_HEADER = "ride_id,rideable_type,started_at,ended_at,start_station_name,start_station_id,end_station_name,end_station_id,start_lat,start_lng,end_lat,end_lng,member_casual"

def test_drops_rows_with_null_started_at(con, create_csv):
    rows = [
        "R1,bike,,2026-05-01 10:15:00,A,1,B,2,0,0,0,0,member",          
        "R2,bike,2026-05-01 10:00:00,2026-05-01 10:15:00,A,1,B,2,0,0,0,0,member" 
    ]
    glob = create_csv("test_null_start.csv", BASE_HEADER, rows)
    clean_trips(con, glob)
    
    result = con.execute("SELECT ride_id FROM trips_silver").fetchall()
    assert result == [('R2',)]

def test_drops_rows_with_null_ended_at(con, create_csv):
    rows = [
        "R1,bike,2026-05-01 10:00:00,,A,1,B,2,0,0,0,0,member",          
        "R2,bike,2026-05-01 10:00:00,2026-05-01 10:15:00,A,1,B,2,0,0,0,0,member" 
    ]
    glob = create_csv("test_null_end.csv", BASE_HEADER, rows)
    clean_trips(con, glob)
    
    result = con.execute("SELECT ride_id FROM trips_silver").fetchall()
    assert result == [('R2',)]

def test_drops_rows_where_ended_before_started(con, create_csv):
    rows = [
        "R1,bike,2026-05-01 11:00:00,2026-05-01 10:00:00,A,1,B,2,0,0,0,0,member", 
        "R2,bike,2026-05-01 10:00:00,2026-05-01 10:15:00,A,1,B,2,0,0,0,0,member"  
    ]
    glob = create_csv("test_negative_duration.csv", BASE_HEADER, rows)
    clean_trips(con, glob)
    
    result = con.execute("SELECT ride_id FROM trips_silver").fetchall()
    assert result == [('R2',)]

def test_dedupes_on_ride_id_keeps_earliest(con, create_csv):
    rows = [
        "R1,bike,2026-05-01 10:15:00,2026-05-01 10:30:00,A,1,B,2,0,0,0,0,member", 
        "R1,bike,2026-05-01 10:00:00,2026-05-01 10:30:00,A,1,B,2,0,0,0,0,member"  
    ]
    glob = create_csv("test_dedupe.csv", BASE_HEADER, rows)
    clean_trips(con, glob)
    
    result = con.execute("SELECT ride_id, started_at FROM trips_silver").fetchall()
    assert len(result) == 1
    assert str(result[0][1]) == "2026-05-01 10:00:00" 

def test_station_id_cast_to_varchar_not_numeric(con, create_csv):
    rows = [
        "R1,bike,2026-05-01 10:00:00,2026-05-01 10:15:00,A,SYS038,B,5329.03,0,0,0,0,member"
    ]
    glob = create_csv("test_varchar_id.csv", BASE_HEADER, rows)
    clean_trips(con, glob)
    
    result = con.execute("SELECT start_station_id, end_station_id FROM trips_silver").fetchall()
    assert result == [('SYS038', '5329.03')]

def test_region_label_applied_correctly(con, create_csv):
    rows = [
        "R1,bike,2026-05-01 10:00:00,2026-05-01 10:15:00,A,1,B,2,0,0,0,0,member"
    ]
    
    # Filename contains JC so the CASE WHEN logic triggers
    glob_jc = create_csv("test_JC_region.csv", BASE_HEADER, rows)
    clean_trips(con, glob_jc)
    assert con.execute("SELECT region FROM trips_silver").fetchone()[0] == "jc"
    
    # Filename does not contain JC, defaults to nyc
    glob_nyc = create_csv("test_nyc_region.csv", BASE_HEADER, rows)
    clean_trips(con, glob_nyc)
    assert con.execute("SELECT region FROM trips_silver").fetchone()[0] == "nyc"

def test_union_by_name_handles_column_order_mismatch(con, tmp_path):
    rows_1 = "R1,bike,2026-05-01 10:00:00,2026-05-01 10:15:00,A,1,B,2,0,0,0,0,member"
    file1 = tmp_path / "file1.csv"
    file1.write_text(BASE_HEADER + "\n" + rows_1)
    
    alt_header = "member_casual,rideable_type,started_at,ended_at,start_station_name,start_station_id,end_station_name,end_station_id,start_lat,start_lng,end_lat,end_lng,ride_id"
    rows_2 = "casual,bike,2026-05-01 11:00:00,2026-05-01 11:15:00,A,1,B,2,0,0,0,0,R2"
    file2 = tmp_path / "file2.csv"
    file2.write_text(alt_header + "\n" + rows_2)
    
    glob = str(tmp_path / "*.csv")
    clean_trips(con, glob)
    
    result = con.execute("SELECT ride_id, member_casual FROM trips_silver ORDER BY ride_id").fetchall()
    assert result == [('R1', 'member'), ('R2', 'casual')]
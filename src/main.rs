extern crate postgres;
extern crate wkb_raster;

use postgres::{Client, NoTls};
use std::env;
use wkb_raster::{Raster, RasterBand, RasterDataSource, InMemoryRasterData, Endian};

const USER: &str = "postgres";
const HOST: &str = "localhost";
const PORT: &str = "5432";
const PASSWORD: &str = "postgres";
const DEFAULT_DB_NAME: &str = "postgres_bug_test";
const SRID: i32 = 4326;

fn main() {

    // Create the raster
        // 2x2 image bytes, u8 format
    let bytes = vec![
        vec![34, 40],
        vec![56, 0],
    ];

    let raster = Raster {
        endian: Endian::Big,    // note: currently Endian::Little is not supported in PostGIS
        version: 0,             // always set to 0
        scale_x: 500.0,         // pixel width in degrees
        scale_y: 1.0,           // pixel height in degrees
        ip_x: 49.89,            // upper left corner longitude in degrees
        ip_y: 8.56,             // upper left corner latitude in degrees
        skew_x: 0.0,            // rotation in degrees (0 to 360)
        skew_y: 0.0,            // rotation in degrees (0 to 360)
        srid: SRID,             // SRID EPSG identifier
        width: 2,               // pixel columns
        height: 2,              // rows
        bands: vec![RasterBand {
            is_nodata_value: false,                     // See documentation, usually false
            data: RasterDataSource::InMemory(
                InMemoryRasterData::UInt8 {
                    data: bytes,
                    nodata: None,
                }
            ),
        }],
    };

    // Connect to db, use "postgres-bug [database name]" to connect to a different database
    let cmd_args = env::args().into_iter().collect::<Vec<String>>();
    let db_name = cmd_args.get(1).map(|s| s.as_str()).unwrap_or(DEFAULT_DB_NAME);
    let mut db_connection = create_database(&db_name);

    db_connection.batch_execute(&format!(r#"
        CREATE EXTENSION IF NOT EXISTS postgis;
        ALTER DATABASE {database_name} SET postgis.enable_outdb_rasters = True;
        ALTER DATABASE {database_name} SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';
        SET postgis.enable_outdb_rasters = True;
        SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';
    "#, database_name = db_name)).unwrap();

    // Execute the rt_postgis.sql script to create the "raster" type
    db_connection.batch_execute(include_str!("./rtpostgis.sql")).unwrap();

    db_connection.batch_execute(&format!(r#"
        CREATE TABLE public.myraster ("id" BIGINT NOT NULL UNIQUE PRIMARY KEY, "raster_data" "raster" NOT NULL);
    "#)).unwrap();

    println!("inserting raster:\r\n\r\n{:#?}", raster);

    // Create and insert the WKB raster string
    let wkb_string = raster.to_wkb_string();
    println!("Inserting WKB string:\r\n{}", wkb_string);
    db_connection.batch_execute(&format!("INSERT INTO public.myraster(id, raster_data) VALUES (1, \'{}\');", wkb_string)).unwrap();

    // Get the WKB string out again
    let query = db_connection.prepare(&format!("SELECT encode(ST_AsBinary(raster_data), 'hex') FROM public.myraster WHERE id = 1;")).unwrap();
    let rows = db_connection.query(&query, &[]).unwrap();
    let first_row = rows.get(0).unwrap();
    let queried_wkb_string: &str = first_row.get(0);
    println!("Parsing WKB string:\r\n{}", queried_wkb_string);
    let parsed_raster = Raster::from_wkb_string(queried_wkb_string.as_bytes()).unwrap();
    println!("Got raster\r\n{:#?}", parsed_raster);
}

fn create_database(db_name: &str) -> Client {
    // connect to the "template1" database, establish a connection to the server
    let mut temp_connection = Client::connect(&format!("postgresql://{}:{}@{}:{}/{}", USER, PASSWORD, HOST, PORT, "template1"), NoTls).unwrap();

    // create a new database with the given name
    temp_connection.batch_execute(&format!("SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '{db_name}' AND pid <> pg_backend_pid();", db_name = db_name)).unwrap();
    temp_connection.batch_execute(&format!("DROP DATABASE IF EXISTS \"{db_name}\";", db_name = db_name)).unwrap();
    temp_connection.batch_execute(&format!("CREATE DATABASE \"{db_name}\" WITH OWNER = postgres ENCODING = \"utf8\";", db_name = db_name)).unwrap();

    // then return a connection to the new database
    Client::connect(&format!("postgresql://{}:{}@{}:{}/{}", USER, PASSWORD, HOST, PORT, db_name), NoTls).unwrap()
}